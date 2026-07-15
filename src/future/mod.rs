pub(crate) mod waker;

use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use pyo3::BoundObject;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyDict, PyIterator, PyTuple};
use pyo3::{Py, PyAny, PyResult};

use tokio::task::AbortHandle;

use crate::RUNTIME;
use crate::future::waker::AsyncioWaker;
use crate::utils::PrependedIterator;

// # PyResponseFuture — design
//
// The future is spawned on the tokio runtime immediately. The spawned task
// holds an `Arc<Mutex<FutureState>>` and on completion transitions the state
// to `Ready`, fires all callbacks, wakes the `AsyncioWaker`, and notifies
// the condvar.
//
// `poll` (__next__) never drives the future — it only checks state.
// If Pending, it yields the asyncio future from the waker to park the coroutine.
// If Ready, it raises StopIteration with the result.
//
// `result()` waits on the condvar until Ready.
//
// `close()` aborts the task and transitions to Ready with CancelledError.

/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Option<Py<PyTuple>>,
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
            args: if args.is_empty() {
                None
            } else {
                Some(args.clone().unbind())
            },
            kwargs: kwargs.map(|k| k.clone().unbind()),
        }
    }
}

/// Internal state of a PyResponseFuture.
enum FutureState {
    /// Future is still running.
    Pending {
        on_success: Vec<Callback>,
        on_error: Vec<Callback>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
}

/// A Python awaitable wrapping a Rust future spawned on tokio.
#[pyclass(name = "ResponseFuture", frozen)]
pub struct PyResponseFuture {
    state: Arc<Mutex<FutureState>>,
    /// Notified when state transitions to Ready.
    ready: Arc<Condvar>,
    /// Waker for the asyncio event loop (never reset).
    waker: Arc<AsyncioWaker>,
    /// Handle to abort the spawned tokio task.
    abort_handle: AbortHandle,
}

impl PyResponseFuture {
    /// Create a `Py<PyResponseFuture>` from a future returning `Result<T, E>`.
    ///
    /// The future is spawned on the tokio runtime immediately.
    pub fn spawn<Fut, T, E>(py: Python<'_>, future: Fut) -> PyResult<Py<PyResponseFuture>>
    where
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: for<'py> IntoPyObject<'py> + Send + 'static,
        E: Into<PyErr> + Send + 'static,
    {
        let state = Arc::new(Mutex::new(FutureState::Pending {
            on_success: Vec::new(),
            on_error: Vec::new(),
        }));
        let ready = Arc::new(Condvar::new());
        let waker = Arc::new(AsyncioWaker::new());

        let state_clone = Arc::clone(&state);
        let ready_clone = Arc::clone(&ready);
        let waker_clone = Arc::clone(&waker);

        let handle = RUNTIME.spawn(async move {
            let result = future.await;

            Python::attach(|py| {
                let py_result: PyResult<Py<PyAny>> = result.map_err(Into::into).and_then(|v| {
                    v.into_pyobject(py)
                        .map(|b| b.into_any().unbind())
                        .map_err(Into::into)
                });

                let callbacks = {
                    let mut state = state_clone.lock_py_attached(py).unwrap();
                    match &mut *state {
                        FutureState::Pending {
                            on_success,
                            on_error,
                        } => {
                            let taken_success = std::mem::take(on_success);
                            let taken_error = std::mem::take(on_error);
                            *state = FutureState::Ready {
                                result: clone_result(py, &py_result),
                            };
                            Some((taken_success, taken_error))
                        }
                        FutureState::Ready { .. } => None,
                    }
                };

                // (callbacks, py_result)
                if let Some(cbs) = callbacks {
                    fire_callbacks(py, cbs, &py_result);
                    Wake::wake_by_ref(&waker_clone);
                    ready_clone.notify_all();
                }
            });

            // if let Some(cbs) = callbacks {
            //     let has_callbacks = !cbs.0.is_empty() || !cbs.1.is_empty();
            //     if has_callbacks {
            //         let _ = tokio::task::spawn_blocking(move || {
            //             Python::attach(|py| {
            //                 fire_callbacks(py, cbs, &py_result);
            //                 Wake::wake_by_ref(&waker_clone);
            //                 ready_clone.notify_all();
            //             });
            //         })
            //         .await;
            //     } else {
            //         Wake::wake_by_ref(&waker_clone);
            //         ready_clone.notify_all();
            //     }
            // }

            // // Use spawn_blocking to avoid blocking tokio worker threads on the GIL
            // let _ = tokio::task::spawn_blocking(move || {
            //     Python::attach(|py| {
            //         if let Some(cbs) = callbacks {
            //             fire_callbacks(py, cbs, &py_result);
            //             Wake::wake_by_ref(&waker_clone);
            //             ready_clone.notify_all();
            //         }
            //     });
            // })
            // .await;
        });

        let abort_handle = handle.abort_handle();

        Py::new(
            py,
            PyResponseFuture {
                state,
                ready,
                waker,
                abort_handle,
            },
        )
    }

    /// Create an already-resolved PyResponseFuture.
    pub fn ready(py: Python<'_>, result: PyResult<Py<PyAny>>) -> PyResult<Py<PyResponseFuture>> {
        let state = Arc::new(Mutex::new(FutureState::Ready { result }));
        let ready = Arc::new(Condvar::new());
        let waker = Arc::new(AsyncioWaker::new());

        // Dummy abort handle. Spawn a no-op task.
        let handle = RUNTIME.spawn(async {});
        let abort_handle = handle.abort_handle();

        Py::new(
            py,
            PyResponseFuture {
                state,
                ready,
                waker,
                abort_handle,
            },
        )
    }

    /// Yield the asyncio future from the waker to park the Python coroutine.
    fn initialize_and_return_asyncio_future(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(future) = self.waker.initialize_future(py)? {
            if let Some(future) = PyIterator::from_object(future).unwrap().next() {
                return Ok(future.unwrap().unbind());
            }
        }
        // Waker was already woken (task completed between state check and here)
        // yield None (sleep(0) equivalent), next __next__ will see Ready
        Ok(py.None())
    }

    /// Close the future with a given error. No-op if already Ready.
    fn close_with_error(&self, py: Python<'_>, err: PyErr) {
        let result: PyResult<Py<PyAny>> = Err(err);

        let callbacks = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            match &mut *state {
                FutureState::Pending {
                    on_success,
                    on_error,
                } => {
                    self.abort_handle.abort();
                    let taken_success = std::mem::take(on_success);
                    let taken_error = std::mem::take(on_error);
                    *state = FutureState::Ready {
                        result: clone_result(py, &result),
                    };
                    Some((taken_success, taken_error))
                }
                FutureState::Ready { .. } => None,
            }
        };

        if let Some(cbs) = callbacks {
            fire_callbacks(py, cbs, &result);
            Wake::wake_by_ref(&self.waker);
            self.ready.notify_all();
        }
    }
}

fn clone_result(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(err) => Err(err.clone_ref(py)),
    }
}

fn fire_callbacks(
    py: Python<'_>,
    callbacks: (Vec<Callback>, Vec<Callback>),
    result: &PyResult<Py<PyAny>>,
) {
    let (on_success, on_error) = callbacks;

    match result {
        Ok(value) => {
            for cb in &on_success {
                invoke_callback(py, cb, value);
            }
        }
        Err(err) => {
            let err_obj = err.value(py);
            for cb in &on_error {
                invoke_callback(py, cb, err_obj.as_any().as_unbound());
            }
        }
    }
}

fn invoke_callback(py: Python<'_>, cb: &Callback, value: &Py<PyAny>) {
    let args = if let Some(extra_args) = &cb.args {
        let extra = extra_args.bind(py);
        let first = value.clone_ref(py).into_any();
        let rest = extra.iter().map(|item| item.unbind());

        let exact_size_wrapper = PrependedIterator::new(first, rest);

        PyTuple::new(py, exact_size_wrapper)
            .expect("failed to allocate PyTuple for callback args")
            .unbind()
    } else {
        PyTuple::new(py, [value.clone_ref(py)])
            .expect("failed to allocate PyTuple for callback args")
            .unbind()
    };

    let kwargs = cb.kwargs.as_ref().map(|k| k.bind(py).clone());
    if let Err(err) = cb.callable.call(py, args.bind(py), kwargs.as_ref()) {
        log::error!("ResponseFuture callback raised an exception: {}", err);
    }
}

fn raise_stop_iteration(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Err(PyStopIteration::new_err((value.clone_ref(py),))),
        Err(err) => Err(err.clone_ref(py)),
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
        {
            let state = self.state.lock_py_attached(py).unwrap();
            if let FutureState::Ready { result } = &*state {
                return raise_stop_iteration(py, result);
            }
        }

        self.initialize_and_return_asyncio_future(py)
    }

    fn send(&self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.__next__(py)
    }

    fn throw(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let err = PyErr::from_value(exc.into_bound(py));

        // If Pending: abort task, set state to Ready with the error, raise it.
        // If already Ready: just raise the thrown exception (don't touch state).
        {
            let state = self.state.lock_py_attached(py).unwrap();
            if let FutureState::Ready { .. } = &*state {
                return Err(err);
            }
        }

        self.close_with_error(py, err.clone_ref(py));
        Err(err)
    }

    fn close(&self, py: Python<'_>) {
        self.close_with_error(py, PyRuntimeError::new_err("future was closed"));
    }

    /// Get the result of this future.
    ///
    /// If the future is still pending, this blocks the calling thread until
    /// it completes (releasing the GIL while waiting).
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        {
            let state = self.state.lock_py_attached(py).unwrap();
            if let FutureState::Ready { result } = &*state {
                return clone_result(py, result);
            }
        }

        // Wait on condvar, releasing GIL
        py.detach(|| {
            let state = self.state.lock().unwrap();
            let _state = self
                .ready
                .wait_while(state, |s| matches!(s, FutureState::Pending { .. }))
                .unwrap();
        });

        let state = self.state.lock_py_attached(py).unwrap();
        match &*state {
            FutureState::Ready { result } => clone_result(py, result),
            FutureState::Pending { .. } => unreachable!("condvar woke but state is still Pending"),
        }
    }

    /// Register a callback to be invoked when the future completes successfully.
    #[pyo3(signature = (callback, *args, **kwargs))]
    fn on_success(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) {
        let cb = Callback::new(callback, args, kwargs);

        let ready_value = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            match &mut *state {
                FutureState::Pending { on_success, .. } => {
                    on_success.push(cb);
                    return;
                }
                FutureState::Ready { result } => clone_result(py, result),
            }
        };

        if let Ok(value) = ready_value {
            invoke_callback(py, &cb, &value);
        }
    }

    /// Register a callback to be invoked when the future completes with an error.
    #[pyo3(signature = (callback, *args, **kwargs))]
    fn on_error(
        &self,
        py: Python<'_>,
        callback: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) {
        let cb = Callback::new(callback, args, kwargs);

        let ready_err = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            match &mut *state {
                FutureState::Pending { on_error, .. } => {
                    on_error.push(cb);
                    return;
                }
                FutureState::Ready { result } => clone_result(py, result),
            }
        };

        if let Err(err) = ready_err {
            let err_obj = err.value(py);
            invoke_callback(py, &cb, err_obj.as_any().as_unbound());
        }
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
