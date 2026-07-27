use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use crate::RUNTIME;

use crate::coroutine::waker::AsyncioWaker;
use crate::coroutine::{BoxedFuture, Coroutine, PollResult};
use crate::utils::PrependedIterator;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{BoundObject, Py, PyAny, PyResult};

use tokio::task::AbortHandle;

// # PyResponseFuture — hybrid design
//
// ## Three states
//
// `PendingAsyncio { coroutine }`
//     The future is driven by the asyncio event loop.
//     This is the default starting state.
//
// `PendingTokio { on_success, on_error, abort_handle, waker }`
//     The future has been spawned on the tokio runtime. `__next__` just
//     yields the asyncio future from the waker. The spawned task transitions
//     to `Ready` on completion.
//
// `Ready { result }`
//     Terminal state. Result stored permanently.
//
// ## Transitions
//
// - `PendingAsyncio` → `PendingTokio`: when callbacks are registered or `result()` is called.
//   The inner future is taken from the coroutine, spawned on tokio.
// - `PendingAsyncio` → `Ready`: when `poll` completes or `close()` is called.
// - `PendingTokio` → `Ready`: when the spawned task completes or `close()` aborts it.
// - `Ready` → (no transitions)

/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
}

impl Callback {
    fn new(
        callable: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> Self {
        Self {
            callable,
            args: args.clone().unbind(),
            kwargs: kwargs.map(|k| k.clone().unbind()),
        }
    }

    /// Invoke this callback, passing `value` as the first argument
    /// followed by any extra args/kwargs. Errors are logged and swallowed.
    fn invoke(&self, py: Python<'_>, value: &Py<PyAny>) {
        let extra = self.args.bind(py);
        let first = value.clone_ref(py).into_any();
        let rest = extra.iter().map(|item| item.unbind());
        let exact_size_wrapper = PrependedIterator::new(first, rest);
        let args = PyTuple::new(py, exact_size_wrapper)
            .expect("failed to allocate PyTuple for callback args");

        let kwargs = self.kwargs.as_ref().map(|k| Bound::clone(k.bind(py)));
        if let Err(err) = self.callable.call(py, args, kwargs.as_ref()) {
            log::error!("ResponseFuture callback raised an exception: {}", err);
        }
    }
}

/// Discriminates whether a [`Callback`] fires on success or on error.
enum CallbackKind {
    /// Fired when the future resolves successfully. Passes the result value.
    OnSuccess(Callback),
    /// Fired when the future resolves with an error. Passes the exception instance.
    OnError(Callback),
}

impl CallbackKind {
    /// Invoke this callback if its variant matches the outcome of `result`.
    fn invoke(&self, py: Python<'_>, result: &PyResult<Py<PyAny>>) {
        match (self, result) {
            (CallbackKind::OnSuccess(cb), Ok(value)) => {
                cb.invoke(py, value);
            }
            (CallbackKind::OnError(cb), Err(err)) => {
                let exc_obj = err.value(py);
                cb.invoke(py, exc_obj.as_any().as_unbound());
            }
            _ => {}
        }
    }

    /// Fire every callback in `callbacks` that matches the outcome of `result`.
    fn fire_all(py: Python<'_>, callbacks: Vec<CallbackKind>, result: &PyResult<Py<PyAny>>) {
        for cb in &callbacks {
            cb.invoke(py, result);
        }
    }
}

/// Internal state of a PyResponseFuture.
enum FutureState {
    /// Future is driven by the asyncio executor.
    PendingAsyncio { coroutine: Coroutine },
    /// Future has been spawned on the tokio runtime.
    PendingTokio {
        callbacks: Vec<CallbackKind>,
        abort_handle: Option<AbortHandle>,
        waker: Arc<AsyncioWaker>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
}

struct FutureInner {
    state: Mutex<FutureState>,
    /// Notified when state transitions to Ready.
    ready: Condvar,
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "ResponseFuture", frozen)]
pub struct PyResponseFuture {
    inner: Arc<FutureInner>,
}

impl PyResponseFuture {
    /// Create a PyResponseFuture starting in PendingAsyncio (default).
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self {
            inner: Arc::new(FutureInner {
                state: Mutex::new(FutureState::PendingAsyncio {
                    coroutine: Coroutine::new(future),
                }),
                ready: Condvar::new(),
            }),
        }
    }
    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => Err(raise_stop_iteration(py, result)),

            FutureState::PendingTokio { waker, .. } => {
                // Future is running on tokio — just yield the asyncio future.
                let waker = Arc::clone(waker);
                drop(state);
                waker.yield_asyncio_future(py)
            }

            FutureState::PendingAsyncio { coroutine } => {
                // Drive the future via the coroutine.
                match coroutine.poll(py, None)? {
                    PollResult::Pending(maybe_future) => Ok(maybe_future),
                    PollResult::Ready(result) => {
                        *state = FutureState::Ready {
                            result: clone_result(py, &result),
                        };
                        drop(state);
                        self.inner.ready.notify_all();
                        Err(raise_stop_iteration(py, &result))
                    }
                }
            }
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
impl PyResponseFuture {
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
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
