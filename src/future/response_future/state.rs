//! The request of a `ResponseFuture` and the last page received, changed only
//! through its transitions. Each state owns the last page received, so exactly
//! one copy of it exists.

use std::sync::Arc;

use pyo3::prelude::*;
use scylla::response::query_result::QueryResult;

use super::rows::{PageRows, Rows, clone_rows};
use crate::future::panicked_err;
use crate::legacy::pending_response_page::PendingResponsePage;

/// The request in flight or how it settled, and the last page received.
pub(super) struct RequestState(State);

enum State {
    /// A request is in flight. `last_page` is the page received before it, if any.
    Pending {
        last_page: Option<PendingResponsePage>,
    },
    Received {
        page: PendingResponsePage,
        /// `None` until the rows are built.
        rows: Option<Rows>,
    },
    Failed {
        err: PyErr,
        /// The last page received before the error, kept as the legacy driver did.
        last_page: Option<PendingResponsePage>,
    },
}

impl RequestState {
    pub(super) fn pending() -> Self {
        Self(State::Pending { last_page: None })
    }

    pub(super) fn is_pending(&self) -> bool {
        matches!(self.0, State::Pending { .. })
    }

    /// The last page received: the source of paging state, columns and the next page.
    pub(super) fn last_page(&self) -> Option<&PendingResponsePage> {
        match &self.0 {
            State::Pending { last_page } | State::Failed { last_page, .. } => last_page.as_ref(),
            State::Received { page, .. } => Some(page),
        }
    }

    /// `Pending` → `Received` or `Failed`. A failure keeps the last page received.
    pub(super) fn settle(&mut self, outcome: PyResult<PendingResponsePage>) {
        debug_assert!(self.is_pending(), "only a request in flight can settle");
        self.0 = match outcome {
            Ok(page) => State::Received { page, rows: None },
            Err(err) => State::Failed {
                err,
                last_page: self.take().into_last_page(),
            },
        };
    }

    /// The rows the request settled with; `None` while a request is in flight.
    pub(super) fn rows(&self, py: Python<'_>) -> Option<PageRows> {
        match &self.0 {
            State::Pending { .. } => None,
            State::Received { page, rows, .. } => Some(match rows {
                Some(rows) => PageRows::Built(clone_rows(py, rows)),
                None => PageRows::ToBuild(Arc::clone(page.query_result()), page.factory().clone()),
            }),
            State::Failed { err, .. } => Some(PageRows::Built(Err(err.clone_ref(py)))),
        }
    }

    /// Stores `built` as the rows of the page `query_result` belongs to, if it
    /// is still the current one and nobody stored rows first. Returns the
    /// stored rows, so every reader of a page gets the same list.
    pub(super) fn store_rows(
        &mut self,
        py: Python<'_>,
        query_result: &Arc<QueryResult>,
        built: Rows,
    ) -> Rows {
        match &mut self.0 {
            State::Received { page, rows, .. }
                if Arc::ptr_eq(page.query_result(), query_result) =>
            {
                clone_rows(py, rows.get_or_insert(built))
            }
            _ => built,
        }
    }

    /// The error the request failed with, or its row factory raised.
    pub(super) fn error(&self, py: Python<'_>) -> Option<PyErr> {
        match &self.0 {
            State::Failed { err, .. } => Some(err.clone_ref(py)),
            State::Received {
                rows: Some(Err(err)),
                ..
            } => Some(err.clone_ref(py)),
            _ => None,
        }
    }

    /// Takes the state out and leaves `Failed` with a panic error, so a
    /// transition that panics before it stores the next state is reported.
    fn take(&mut self) -> State {
        let panicked = State::Failed {
            err: panicked_err(),
            last_page: None,
        };
        std::mem::replace(&mut self.0, panicked)
    }
}

impl State {
    /// The last page received, consuming the state.
    fn into_last_page(self) -> Option<PendingResponsePage> {
        match self {
            State::Pending { last_page } | State::Failed { last_page, .. } => last_page,
            State::Received { page, .. } => Some(page),
        }
    }
}
