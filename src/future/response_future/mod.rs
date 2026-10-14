//! `ResponseFuture`: the callback-driven future of the legacy driver.
//!
//! Always runs on the tokio runtime, so there is no asyncio state to hand over
//! from. A request is `Pending` while in flight, then `Received` or `Failed`,
//! and `start_fetching_next_page()` moves it back to `Pending`.
//!
//! The worker completing a request never takes the GIL: it stores the page.
//! Rows are built by whichever GIL-holding thread asks first, for now a
//! `result()` caller.
//!
//! - `rows`: the rows of a page, built or still to build.
//! - `state`: the request in flight or how it settled, and the last page received.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::TryFutureExt;
use pyo3::prelude::*;

use crate::TaskExecutionMode;
use crate::core::results::PendingRequestResult;
use crate::errors::ScyllaError;
use crate::errors::execution::DriverExecuteError;
use crate::future::dropped_err;
use crate::future::panics::catch_unwind;
use crate::future::state_lock::StateLock;
use crate::future::task::{TaskDropped, spawn_guarded};
use crate::legacy::errors::QueryExhausted;
use crate::legacy::pending_response_page::PendingResponsePage;
use crate::legacy::session::LegacyQuery;
use crate::results::PyPagingState;
use crate::utils::WithOriginalPyObject;

mod rows;
mod state;
use rows::{PageRows, Rows, build_rows};
use state::RequestState;

/// A request for one page, boxed where it is built so the large future is copied once.
pub(crate) type LegacyRequestFuture<E> =
    Pin<Box<dyn Future<Output = Result<PendingRequestResult, E>> + Send>>;

/// Everything the one lock guards.
struct Shared {
    /// The request in flight or how it settled, and the last page received.
    request: RequestState,
}

impl Shared {
    /// Whether the last page received says more follow.
    fn has_more_pages(&self) -> bool {
        self.request
            .last_page()
            .is_some_and(|page| page.pager().has_more_pages())
    }
}

/// The part of a `ResponseFuture` that its requests share.
struct ResponseInner {
    /// Waiters are notified on every transition to `Ready`.
    shared: StateLock<Shared>,
}

impl ResponseInner {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            shared: StateLock::new(Shared {
                request: RequestState::pending(),
            }),
        })
    }

    /// Spawns a request whose page, once received, becomes the current one.
    fn spawn<E>(self: &Arc<Self>, request: LegacyRequestFuture<E>)
    where
        E: Into<PyErr> + 'static,
    {
        let this = Arc::clone(self);
        let request = request.map_ok(PendingResponsePage::from);

        spawn_guarded(catch_unwind(request), move |outcome| match outcome {
            Ok(outcome) => this.complete(outcome.and_then(|page| page.map_err(Into::into))),
            Err(TaskDropped) => this.complete_dropped(),
        });
    }

    /// The request finished on a runtime worker: settle without touching
    /// Python.
    fn complete(self: &Arc<Self>, outcome: PyResult<PendingResponsePage>) {
        let waiters = {
            // A tokio worker: not attached.
            let mut shared = self.shared.lock_detached();
            shared.request.settle(outcome);
            shared.waiters()
        };

        self.shared.notify(waiters);
    }

    /// The request was torn down before completing, likely because the
    /// runtime shut down.
    fn complete_dropped(&self) {
        let waiters = {
            // The task is dropped on a runtime thread, or during shutdown
            // inside py.detach: not attached.
            let mut shared = self.shared.lock_detached();
            if !shared.request.is_pending() {
                return;
            }

            shared.request.settle(Err(dropped_err()));
            shared.waiters()
        };

        self.shared.notify(waiters);
    }

    /// Blocks, with the GIL released, until the request in flight settles;
    /// returns the rows of its page (a list or `None`) or raises its error.
    ///
    /// Another thread may call `start_fetching_next_page()` between the wakeup
    /// and the read, so the state can be `Pending` again here. Like the legacy
    /// driver, which does not support paging concurrently with `result()`, no
    /// attempt is made to return the page that was skipped: the loop waits for
    /// whichever page settles next.
    fn wait_rows(&self, py: Python<'_>) -> Rows {
        loop {
            let rows = self.shared.lock_py_attached(py).request.rows(py);
            if let Some(rows) = rows {
                return self.build_rows(py, rows);
            }

            self.shared
                .wait_while(py, None, |shared| shared.request.is_pending());
        }
    }

    /// The rows of a page, built if needed. The row factory is user Python that
    /// may touch the future, so no lock is held while it runs: two threads
    /// asking at once both build, and the first to finish stores its rows.
    fn build_rows(&self, py: Python<'_>, rows: PageRows) -> Rows {
        let (query_result, factory) = match rows {
            PageRows::Built(rows) => return rows,
            PageRows::ToBuild(query_result, factory) => (query_result, factory),
        };

        let built = build_rows(py, Arc::clone(&query_result), &factory);
        self.shared
            .lock_py_attached(py)
            .request
            .store_rows(py, &query_result, built)
    }
}

/// A request in flight, delivering its result synchronously via `result()`.
/// Returned by `LegacySession.execute_async()`.
#[pyclass(module = "scylla.legacy", name = "ResponseFuture", frozen)]
pub(crate) struct PyResponseFuture {
    inner: Arc<ResponseInner>,
    /// The query as passed in, and what it was extracted to.
    query: WithOriginalPyObject<LegacyQuery>,
    timeout: Option<f64>,
    /// The row factory as the session passed it, `None` for the driver default.
    row_factory: Option<Py<PyAny>>,
}

impl PyResponseFuture {
    /// Spawns `request` on the runtime, returning the future that delivers its result.
    #[expect(dead_code, reason = "LegacySession calls it; it lands in a later PR")]
    pub(crate) fn spawn(
        py: Python<'_>,
        request: LegacyRequestFuture<DriverExecuteError>,
        query: WithOriginalPyObject<LegacyQuery>,
        timeout: Option<f64>,
        row_factory: Option<Py<PyAny>>,
    ) -> PyResult<Py<Self>> {
        let inner = ResponseInner::new();
        inner.spawn(request);

        Py::new(
            py,
            Self {
                inner,
                query,
                timeout,
                row_factory,
            },
        )
    }

    /// Blocks until the request in flight settles; returns the rows of its
    /// page (a list or `None`) or raises its error.
    pub(crate) fn wait_rows(&self, py: Python<'_>) -> Rows {
        self.inner.wait_rows(py)
    }
}

#[pymethods]
impl PyResponseFuture {
    /// Blocks until the request settles; returns the rows of its page or raises.
    fn result(&self, py: Python<'_>) -> Rows {
        self.wait_rows(py)
    }

    /// Whether the last page received says more follow.
    #[getter]
    fn has_more_pages(&self, py: Python<'_>) -> bool {
        self.inner.shared.lock_py_attached(py).has_more_pages()
    }

    /// Starts fetching the next page.
    /// Raises `QueryExhausted` if there is none.
    fn start_fetching_next_page(&self, py: Python<'_>) -> PyResult<()> {
        let (query_pager, row_factory) = {
            let mut shared = self.inner.shared.lock_py_attached(py);
            if shared.request.is_pending() {
                return Err(ScyllaError::new_err("a request is already in flight"));
            }
            let pager_and_factory = match shared.request.last_page() {
                Some(page) if page.pager().has_more_pages() => {
                    (page.pager().clone(), page.factory().clone())
                }
                _ => return Err(QueryExhausted::new_err("no more pages")),
            };

            shared.request.switch_to_pending();
            pager_and_factory
        };

        let next_page = async move {
            // Already on the runtime, as the future runs there.
            query_pager
                .fetch_next_pending_page(row_factory, TaskExecutionMode::Inline)
                .await?
                .ok_or_else(|| QueryExhausted::new_err("no more pages"))
        };

        self.inner.spawn(Box::pin(next_page));
        Ok(())
    }

    /// Paging state of the last page, `None` if the query is not paged or exhausted.
    #[getter]
    fn paging_state(&self, py: Python<'_>) -> Option<PyPagingState> {
        let shared = self.inner.shared.lock_py_attached(py);
        let page = shared.request.last_page()?;

        page.pager().paging_state().map(PyPagingState::from)
    }

    /// The statement being executed.
    #[getter]
    fn query(&self, py: Python<'_>) -> Py<PyAny> {
        self.query.original.clone_ref(py)
    }

    /// Client-side timeout of the request in seconds, `None` for no timeout.
    #[getter]
    fn timeout(&self) -> Option<f64> {
        self.timeout
    }

    /// Row factory of the request, `None` for the driver default.
    #[getter]
    fn row_factory(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.row_factory.as_ref().map(|f| f.clone_ref(py))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let outcome = {
            let shared = self.inner.shared.lock_py_attached(py);
            if shared.request.is_pending() {
                "(no result yet)".to_string()
            } else {
                match shared.request.error(py) {
                    Some(err) => format!("exception={err}"),
                    None => "result".to_string(),
                }
            }
        };

        Ok(format!(
            "<ResponseFuture: query={} {outcome}>",
            self.query.original.bind(py).repr()?
        ))
    }
}
