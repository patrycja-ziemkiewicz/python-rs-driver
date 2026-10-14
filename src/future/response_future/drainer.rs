//! The drainer of a `ResponseFuture`: a blocking thread that builds the rows
//! of every page whose callbacks have not run, and runs them in page order.

use std::collections::VecDeque;
use std::sync::Arc;

use pyo3::Python;

use super::rows::callback_value;
use super::state::{RequestState, Undelivered};
use super::{ResponseInner, Shared};
use crate::RUNTIME;
use crate::future::callbacks::Delivery;

/// Owned by the running drainer, else parked in `Drainer`. There is exactly
/// one per future, so two drainers can never run at once.
struct DrainToken(());

pub(super) struct Drainer {
    /// Pages paged past before the drainer ran their callbacks, oldest first.
    unfired_pages: VecDeque<Undelivered>,
    permit: Option<DrainToken>,
}

impl Drainer {
    pub(super) fn new() -> Self {
        Self {
            unfired_pages: VecDeque::new(),
            permit: Some(DrainToken(())),
        }
    }

    pub(super) fn queue(&mut self, page: Undelivered) {
        self.unfired_pages.push_back(page);
    }

    /// Starts a drainer on a blocking thread, unless one runs now.
    pub(super) fn wake(&mut self, inner: &Arc<ResponseInner>) {
        if let Some(token) = self.permit.take() {
            Self::run(Arc::clone(inner), token);
        }
    }

    /// Runs the callbacks of every undelivered page, oldest first, then parks
    /// the token again.
    fn run(inner: Arc<ResponseInner>, token: DrainToken) {
        RUNTIME.spawn_blocking(move || {
            Python::attach(|py| {
                loop {
                    let undelivered = {
                        let mut shared = inner.shared.lock_py_attached(py);
                        let Shared {
                            drainer, request, ..
                        } = &mut *shared;
                        match drainer.next(py, request) {
                            Some(undelivered) => undelivered,
                            None => {
                                drainer.permit = Some(token);
                                return;
                            }
                        }
                    };

                    let Undelivered { rows, unfired } = undelivered;
                    let rows = inner.build_rows(py, rows);
                    unfired.fire(py, Delivery::of(&rows), &callback_value(py, &rows));
                }
            });
        });
    }

    /// The oldest page paged past, else the current one.
    fn next(&mut self, py: Python<'_>, request: &mut RequestState) -> Option<Undelivered> {
        self.unfired_pages
            .pop_front()
            .or_else(|| request.take_unfired(py))
    }
}
