use crate::future::asyncio::{Coroutine, PollResult};
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::{BoundObject, Py, PyAny, PyResult};
use std::sync::{Arc, Mutex};
use std::task::Wake;

mod asyncio;
mod boxed_future;
mod panics;

// # PyDriverFuture
//
// ## Two states
//
// `PendingAsyncio { coroutine }`
//     The future is driven by the asyncio event loop.
//     This is the default starting state.
//
// `Ready { result }`
//     Terminal state. Result stored permanently.
//
// `Panicked`
//     Terminal state. A panic unwound out of a state transition, taking the coroutine
//     with it; every entry point reports `panicked_err()`.
//
// ## Transitions
//
// - `PendingAsyncio` → `Ready`: when `poll` completes, or `close()` is called.
// - any state → `Panicked`: when a panic unwinds out of a transition (see below).
// - `Ready` / `Panicked` → (no transitions)

/// Internal state of a PyDriverFuture.
enum FutureState {
    /// Future is driven by the asyncio executor.
    PendingAsyncio { coroutine: Coroutine },

    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
    /// A transition that consumes the previous state is in progress, or panicked
    /// halfway through one.
    Panicked,
}

/// The error every entry point reports for a future left [`FutureState::Panicked`].
fn panicked_err() -> PyErr {
    PyRuntimeError::new_err(
        "internal driver error: a panic left this future unusable; \
         this is a bug in the scylla driver, please report it",
    )
}

struct FutureInner {
    state: Mutex<FutureState>,
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "DriverFuture", frozen)]
pub struct PyDriverFuture {
    inner: Arc<FutureInner>,
}

impl PyDriverFuture {
    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match std::mem::replace(&mut *state, FutureState::Panicked) {
            FutureState::Ready { result } => {
                let err = raise_stop_iteration(py, &result);
                *state = FutureState::Ready { result };
                Err(err)
            }

            // Drive the future via the coroutine.
            FutureState::PendingAsyncio { coroutine } => match coroutine.poll(py, None) {
                PollResult::Pending { coroutine, value } => {
                    *state = FutureState::PendingAsyncio { coroutine };
                    value
                }
                PollResult::Ready(result) => {
                    *state = FutureState::Ready {
                        result: clone_result(py, &result),
                    };
                    drop(state);
                    Err(raise_stop_iteration(py, &result))
                }
            },

            // Report the panic to the awaiter.
            FutureState::Panicked => Err(panicked_err()),
        }
    }

    /// Close the future. Transitions to Ready with `exc` as the error.
    fn close_future(&self, py: Python<'_>, exc: PyErr) {
        let err_result: PyResult<Py<PyAny>> = Err(exc);

        let waker = {
            let mut state = self.inner.state.lock_py_attached(py).unwrap();

            let closed = FutureState::Ready {
                result: clone_result(py, &err_result),
            };
            match std::mem::replace(&mut *state, closed) {
                terminal @ (FutureState::Panicked | FutureState::Ready { .. }) => {
                    *state = terminal;
                    return;
                }

                FutureState::PendingAsyncio { coroutine } => coroutine.into_waker(),
            }
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
    /// Throw an exception into the future.
    /// - Ready: re-raises the exception (coroutine is exhausted).
    /// - PendingAsyncio: delegates to `coroutine.poll(py, Some(exc))`.
    fn throw_into(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match std::mem::replace(&mut *state, FutureState::Panicked) {
            terminal @ (FutureState::Panicked | FutureState::Ready { .. }) => {
                *state = terminal;
                Err(PyErr::from_value(exc.into_bound(py)))
            }

            FutureState::PendingAsyncio { coroutine } => match coroutine.poll(py, Some(exc)) {
                PollResult::Pending { coroutine, value } => {
                    *state = FutureState::PendingAsyncio { coroutine };
                    value
                }
                PollResult::Ready(result) => {
                    *state = FutureState::Ready {
                        result: clone_result(py, &result),
                    };
                    drop(state);
                    Err(raise_stop_iteration(py, &result))
                }
            },
        }
    }
}

fn clone_result(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(err) => Err(err.clone_ref(py)),
    }
}

fn raise_stop_iteration(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyErr {
    match result {
        Ok(value) => PyStopIteration::new_err((value.clone_ref(py),)),
        Err(err) => err.clone_ref(py),
    }
}

#[pymethods]
impl PyDriverFuture {
    fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __iter__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.poll_coroutine(py)
    }

    fn send(&self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.__next__(py)
    }

    fn throw(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.throw_into(py, exc)
    }

    fn close(&self, py: Python<'_>) {
        self.close_future(py, PyRuntimeError::new_err("future was closed"));
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDriverFuture>()?;
    Ok(())
}
