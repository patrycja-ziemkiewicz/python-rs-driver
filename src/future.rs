use std::sync::{Condvar, Mutex};
use crate::coroutine::{Coroutine, PollResult};
use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
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

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "ResponseFuture", frozen)]
pub struct PyResponseFuture {
    state: Mutex<FutureState>,
    /// Notified when state transitions to Ready, so concurrent result() callers can wake up.
    ready: Condvar,
}

impl PyResponseFuture {
    /// Create a PyResponseFuture from a Rust async future.
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self {
            state: Mutex::new(FutureState::Pending {
                coroutine: Coroutine::new(None, future),
                on_success: Vec::new(),
                on_error: Vec::new(),
            }),
            ready: Condvar::new(),
        }
    }
    /// Create an already-resolved PyResponseFuture with the given result.
    pub fn ready(result: PyResult<Py<PyAny>>) -> Self {
        Self {
            state: Mutex::new(FutureState::Ready { result }),
            ready: Condvar::new(),
        }
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
