use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Condvar, Mutex};
use std::task::Wake;

use crate::RUNTIME;
use crate::coroutine::{Coroutine, PollResult};
use crate::utils::PrependedIterator;
use pyo3::BoundObject;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{Py, PyAny, PyResult};

// # PyResponseFuture — design and invariants
//
// ## State machine
//
// A `PyResponseFuture` is always in one of two states, protected by a `Mutex`:
//
//   `Pending { coroutine, on_success, on_error }`
//       The Rust future is still running. The coroutine drives it via the
//       Python async protocol (`__next__` / `send` / `throw`), or a blocking
//       thread is running it to completion via `.result()`.
//
//   `Ready { result }`
//       The future has completed. The result is stored permanently and all
//       registered callbacks have been (or are being) fired. Once in this
//       state the future never transitions back.
//
// ## Coroutine invariants
//
// Inside `Coroutine`, the inner future (`future: Option<Pin<Box<...>>>`) can
// be `None` for exactly two reasons:
//
//   1. `.result()` called `take_future_and_waker()` — the future was moved
//      into `RUNTIME.block_on()`. The waker is guaranteed to be `Some` in
//      this case because `take_future_and_waker` always initialises it before
//      returning. `poll_coroutine` seeing `future == None` with a live waker
//      knows it should suspend the Python coroutine via the asyncio future
//      rather than error.
//
//   2. `close()` or a completed `poll()` called `coroutine.close()` /
//      `close_and_get_waker()` — but both of those transitions atomically
//      set state to `Ready` under the same lock acquisition. Therefore, after
//      either path finishes, `FutureState` is `Ready` and `poll_coroutine`
//      will hit the `Ready` branch before it ever inspects the coroutine.
//      The `future == None` branch inside `poll` is therefore only reachable
//      via reason (1).
//
// ## Concurrency and wakeup
//
// When `.result()` finishes `block_on` it:
//   - re-acquires the mutex and writes `Ready` (or skips if already `Ready`
//     due to a concurrent `close()`),
//   - calls `waker.wake()` — signals the asyncio event loop via
//     `call_soon_threadsafe(future.set_result)` so any awaiting Python
//     coroutine is rescheduled,
//   - calls `ready.notify_all()` — wakes any other threads blocked on
//     `.result()` that are waiting on the `Condvar`.
//
// Threads waiting on the `Condvar` always release the GIL first (`py.detach`)
// so the blocking thread can re-acquire it after `block_on` completes.
// Without this, the blocking thread's `lock_py_attached` would deadlock
// against the waiting thread holding the GIL inside `wait_while`.

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

    fn fire_callbacks(
        &self,
        py: Python<'_>,
        callbacks: (Vec<Callback>, Vec<Callback>),
        result: &PyResult<Py<PyAny>>,
    ) {
        let (on_success, on_error) = callbacks;

        match result {
            Ok(value) => {
                for cb in &on_success {
                    Self::invoke_callback(py, cb, value);
                }
            }
            Err(err) => {
                let err_obj = err.value(py);
                for cb in &on_error {
                    Self::invoke_callback(py, cb, err_obj.as_any().as_unbound());
                }
            }
        }
    }

    /// Invoke a single callback, passing the result/error as the first argument
    /// followed by any extra args/kwargs. Errors are logged and swallowed —
    /// a failing callback must not abort sibling callbacks or the future itself.
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

    /// Takes the inner future and runs it to completion on the Tokio runtime, blocking
    /// the calling thread (GIL released via `py.detach`). If another thread is already
    /// blocking on the same future, waits on the `Condvar` (GIL released) until that
    /// thread writes `Ready` and calls `notify_all`. On completion transitions state to
    /// `Ready`, fires `waker.wake()` to reschedule any awaiting Python coroutine, and
    /// notifies all condvar waiters.
    fn block_until_ready(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Take the future and waker under the lock. If the future is already taken by a
        // concurrent result() call, fall through to the condvar wait path instead.
        let (future, waker) = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            match &mut *state {
                FutureState::Pending { coroutine, .. } => {
                    match coroutine.take_future_and_waker() {
                        Some((future, waker)) => (future, waker),
                        None => {
                            // Another result() already took the future and is blocking.
                            // Release the GIL while waiting so the blocking thread can
                            // re-acquire it when it calls lock_py_attached after block_on.
                            drop(state);
                            py.detach(|| {
                                let state = self.state.lock().unwrap();
                                let _state = self
                                    .ready
                                    .wait_while(state, |s| matches!(s, FutureState::Pending { .. }))
                                    .unwrap();
                            });

                            let state = self.state.lock_py_attached(py).unwrap();
                            return match &*state {
                                FutureState::Ready { result } => clone_result(py, result),
                                FutureState::Pending { .. } => {
                                    unreachable!("condvar woke but state is still Pending")
                                }
                            };
                        }
                    }
                }
                FutureState::Ready { result } => return clone_result(py, result),
            }
        };

        let result = run_future(py, future)?;

        let (on_success, on_error) = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            match &mut *state {
                FutureState::Pending {
                    on_success,
                    on_error,
                    ..
                } => {
                    let taken_success = std::mem::take(on_success);
                    let taken_error = std::mem::take(on_error);
                    *state = FutureState::Ready {
                        result: clone_result(py, &result),
                    };
                    (taken_success, taken_error)
                }
                FutureState::Ready { result } => {
                    waker.wake();
                    self.ready.notify_all();
                    return clone_result(py, result);
                }
            }
        };

        self.fire_callbacks(py, (on_success, on_error), &result);

        waker.wake();
        self.ready.notify_all();

        result
    }

    /// Poll the coroutine with an optional exception, completing the future if ready.
    /// Returns the yielded value if still pending, or raises StopIteration/exception if done.
    fn poll_coroutine(&self, py: Python<'_>, exc: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let (on_success, on_error, result) = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            match &mut *state {
                FutureState::Pending {
                    coroutine,
                    on_success,
                    on_error,
                } => match coroutine.poll(py, exc)? {
                    PollResult::Pending(value) => return Ok(value),
                    PollResult::Ready(result) => {
                        let taken_success = std::mem::take(on_success);
                        let taken_error = std::mem::take(on_error);

                        *state = FutureState::Ready {
                            result: clone_result(py, &result),
                        };

                        (taken_success, taken_error, result)
                    }
                },
                FutureState::Ready { result } => return raise_stop_iteration(py, result),
            }
        };

        self.ready.notify_all();
        self.fire_callbacks(py, (on_success, on_error), &result);
        raise_stop_iteration(py, &result)
    }

    /// Transitions the future from `Pending` to `Ready` with a "future was closed" error.
    /// Drops the inner Rust future, fires the waker to reschedule any awaiting Python
    /// coroutine, notifies condvar waiters, and invokes all registered error callbacks.
    /// No-op if the future is already `Ready`.
    fn close_coroutine(&self, py: Python<'_>) {
        let result = Err(PyRuntimeError::new_err("future was closed"));
        let (on_success, on_error, waker) = {
            let mut state = self.state.lock_py_attached(py).unwrap();
            let FutureState::Pending {
                coroutine,
                on_success,
                on_error,
            } = &mut *state
            else {
                return;
            };

            let waker = coroutine.close_and_get_waker();
            let taken_success = std::mem::take(on_success);
            let taken_error = std::mem::take(on_error);

            *state = FutureState::Ready {
                result: clone_result(py, &result),
            };
            (taken_success, taken_error, waker)
        };

        if let Some(waker) = waker {
            waker.wake()
        }

        self.ready.notify_all();

        self.fire_callbacks(py, (on_success, on_error), &result);
    }
}

fn clone_result(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(err) => Err(err.clone_ref(py)),
    }
}

/// Run a pinned future to completion on the Tokio runtime, releasing the GIL while blocking.
/// Catches panics and converts them to `PyRuntimeError`.
fn run_future(
    py: Python<'_>,
    future: Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>,
) -> PyResult<PyResult<Py<PyAny>>> {
    match catch_unwind(AssertUnwindSafe(|| py.detach(|| RUNTIME.block_on(future)))) {
        Ok(result) => Ok(result),
        Err(err) => {
            let msg = if let Some(s) = err.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = err.downcast_ref::<String>() {
                s.clone()
            } else {
                "blocking on future panicked".to_string()
            };
            Err(PyRuntimeError::new_err(msg))
        }
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
        self.poll_coroutine(py, None)
    }

    fn send(&self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.__next__(py)
    }

    fn throw(&self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.poll_coroutine(py, Some(exc))
    }

    fn close(&self, py: Python<'_>) {
        self.close_coroutine(py);
    }

    /// Get the result of this future.
    ///
    /// If the future is still pending, this blocks the calling thread until
    /// it completes (releasing the GIL while waiting).
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.block_until_ready(py)
    }

    /// Register a callback to be invoked when the future completes successfully.
    ///
    /// The callback is called as `callback(result, *args, **kwargs)`.
    /// If the future is already done with a success, the callback is invoked immediately.
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
            Self::invoke_callback(py, &cb, &value);
        }
    }

    /// Register a callback to be invoked when the future completes with an error.
    ///
    /// The callback is called as `callback(exception, *args, **kwargs)`.
    /// If the future is already done with an error, the callback is invoked immediately.
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
            Self::invoke_callback(py, &cb, err_obj.as_any().as_unbound());
        }
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
