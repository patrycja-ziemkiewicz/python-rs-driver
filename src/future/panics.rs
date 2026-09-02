use std::any::Any;
use std::panic::{self, AssertUnwindSafe};

use std::pin::Pin;
use std::task::{Context, Poll};

use pyo3::exceptions::PyRuntimeError;
use pyo3::{Py, PyAny, PyErr, PyResult, Python};

use crate::future::boxed_future::{PyBoxedFuture, PyFuture, ResolvedResult};

/// Convert a caught panic payload into the error reported to Python.
fn panic_payload_to_err(payload: Box<dyn Any + Send>) -> PyErr {
    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Rust future panicked".to_string()
    };
    PyRuntimeError::new_err(msg)
}

/// Poll `future` once, turning a panic into `Err(PyRuntimeError)`.
///
/// A panic leaves the future half-way through its state machine, so it must not be
/// polled again — callers drop it once this returns [`Poll::Ready`].
pub(super) fn poll_catch_panics(
    future: Pin<&mut (dyn PyFuture + Send)>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), PyErr>> {
    // `Pin<&mut F>` is `!UnwindSafe`, being a mutable reference. Asserting is sound
    // because a future that panicked is only ever dropped afterwards.
    match panic::catch_unwind(AssertUnwindSafe(|| future.poll_stash(cx))) {
        Ok(poll) => poll.map(Ok),
        Err(payload) => Poll::Ready(Err(panic_payload_to_err(payload))),
    }
}

/// Run a deferred Python conversion, turning a panic into `Err(PyRuntimeError)`.
pub(super) fn resolve_catch_panics(
    resolved: ResolvedResult,
    py: Python<'_>,
) -> PyResult<Py<PyAny>> {
    // Asserting is sound `resolved` is consumed here
    // and nothing observes it again if the conversion unwinds.
    match panic::catch_unwind(AssertUnwindSafe(|| resolved.into_py_result(py))) {
        Ok(result) => result,
        Err(payload) => Err(panic_payload_to_err(payload)),
    }
}
