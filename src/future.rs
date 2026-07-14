use crate::coroutine::{Coroutine, PollResult};
use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::{Py, PyAny, PyResult};
/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Option<Py<PyTuple>>,
    kwargs: Option<Py<PyDict>>,
}
/// Internal state of a PyResponseFuture.
enum FutureState {
    /// Future is still running.
    Pending {
        coroutine: Coroutine,
        on_success: Vec<Callback>,
        on_error: Vec<Callback>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
}
