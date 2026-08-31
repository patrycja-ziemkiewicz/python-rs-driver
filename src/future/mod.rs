use crate::RUNTIME;
use crate::future::asyncio::waker::AsyncioWaker;
use crate::future::asyncio::{Coroutine, PollResult};
use crate::future::boxed_future::PyBoxedFuture;
pub(crate) use crate::future::boxed_future::{BoxedFuture, boxed_py_future};
use crate::future::callbacks::CallbackKind;
pub(crate) use crate::future::driver_future::DriverFuture;
use crate::future::panics::{catch_panics, resolve_catch_panics};
use crate::utils::PyDuration;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::{Py, PyAny, PyResult};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;
use std::time::Duration;

use tokio::task::AbortHandle;

mod asyncio;
mod boxed_future;
mod callbacks;
mod panics;

// # PyDriverFuture — hybrid design
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
// `Panicked`
//     Terminal state. A panic unwound out of a state transition, taking the coroutine
//     with it; every entry point reports `panicked_err()`.
//
// ## Transitions
//
// - `PendingAsyncio` → `PendingTokio`: when callbacks are registered, `result()` is
//   called, or `start()` is called explicitly. The inner future is taken from the
//   coroutine, spawned on tokio.
// - `PendingAsyncio` → `Ready`: when `poll` completes, or `close()` is called.
// - `PendingTokio` → `Ready`: when the spawned task completes, or `close()` aborts it.
// - any state → `Panicked`: when a panic unwinds out of a transition (see below).
// - `Ready` / `Panicked` → (no transitions)

/// Internal state of a PyDriverFuture.
enum FutureState {
    /// Future is driven by the asyncio executor.
    PendingAsyncio { coroutine: Coroutine },
    /// Future has been spawned on the tokio runtime.
    PendingTokio {
        callbacks: Vec<CallbackKind>,
        abort_handle: AbortHandle,
        waker: Arc<AsyncioWaker>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
    /// A transition that consumes the previous state is in progress, or panicked
    /// halfway through one.
    Panicked,
}

impl FutureState {
    /// Whether the future can still make progress. Both terminal states —
    /// [`FutureState::Ready`] and [`FutureState::Panicked`]
    fn is_terminal(&self) -> bool {
        matches!(self, FutureState::Ready { .. } | FutureState::Panicked)
    }
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
    /// Notified when the state transitions to a terminal state.
    ready: Condvar,
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "DriverFuture", frozen)]
pub struct PyDriverFuture {
    inner: Arc<FutureInner>,
}

impl PyDriverFuture {
    /// Spawn a future on tokio, returning the abort handle.
    /// On completion the spawned task transitions `state` to `Ready`,
    /// fires any registered callbacks, wakes the asyncio waker, and notifies
    /// the condvar.
    fn spawn_future_on_tokio(
        future: PyBoxedFuture,
        inner: &Arc<FutureInner>,
        waker: &Arc<AsyncioWaker>,
    ) -> AbortHandle {
        let inner_clone = Arc::clone(inner);
        let waker_clone = Arc::clone(waker);

        let handle = RUNTIME.spawn(async move {
            let resolved = catch_panics(future).await;

            Python::attach(|py| {
                let result = resolve_catch_panics(resolved, py);

                let callbacks = {
                    let mut state = inner_clone.state.lock_py_attached(py).unwrap();
                    match &mut *state {
                        FutureState::PendingTokio { callbacks, .. } => {
                            let taken = std::mem::take(callbacks);
                            *state = FutureState::Ready {
                                result: clone_result(py, &result),
                            };
                            Some(taken)
                        }
                        _ => None,
                    }
                };

                // `None` means the future was already closed/cancelled/thrown-into
                // by the time this task completed. There is nothing left to notify.
                let Some(callbacks) = callbacks else {
                    return;
                };

                if callbacks.is_empty() {
                    waker_clone.wake();
                    inner_clone.ready.notify_all();
                    return;
                }

                let result_for_cbs = clone_result(py, &result);
                RUNTIME.spawn_blocking(move || {
                    Python::attach(|py| {
                        CallbackKind::fire_all(py, callbacks, &result_for_cbs);

                        waker_clone.wake();
                        inner_clone.ready.notify_all();
                    });
                });
            });
        });

        handle.abort_handle()
    }

    /// Transition from PendingAsyncio to PendingTokio by spawning the given
    /// future on the tokio runtime.
    /// Must be called while holding the state lock.
    fn transition_to_tokio(
        future: PyBoxedFuture,
        waker: Arc<AsyncioWaker>,
        inner: &Arc<FutureInner>,
        state_guard: &mut std::sync::MutexGuard<'_, FutureState>,
    ) {
        let abort_handle = Self::spawn_future_on_tokio(future, inner, &waker);

        **state_guard = FutureState::PendingTokio {
            callbacks: Vec::new(),
            abort_handle,
            waker,
        };
    }

    /// If `state_guard` is `PendingAsyncio`, take its future/waker and
    /// transition to `PendingTokio`. No-op otherwise.
    /// Must be called while holding the state lock.
    fn ensure_started(
        inner: &Arc<FutureInner>,
        state_guard: &mut std::sync::MutexGuard<'_, FutureState>,
    ) {
        let coroutine = match std::mem::replace(&mut **state_guard, FutureState::Panicked) {
            FutureState::PendingAsyncio { coroutine } => coroutine,
            // Already started, or finished — put the state back untouched.
            other => {
                **state_guard = other;
                return;
            }
        };

        let (future, waker) = coroutine.into_future_and_waker();
        Self::transition_to_tokio(future, waker, inner, state_guard);
    }

    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match std::mem::replace(&mut *state, FutureState::Panicked) {
            FutureState::Ready { result } => {
                let err = raise_stop_iteration(py, &result);
                *state = FutureState::Ready { result };
                Err(err)
            }

            // Future is running on tokio — just yield the asyncio future.
            FutureState::PendingTokio {
                callbacks,
                abort_handle,
                waker,
            } => {
                let asyncio_waker = Arc::clone(&waker);
                *state = FutureState::PendingTokio {
                    callbacks,
                    abort_handle,
                    waker,
                };
                drop(state);
                asyncio_waker.yield_asyncio_future(py)
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
                    self.inner.ready.notify_all();
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

        let (callbacks, waker) = {
            let mut state = self.inner.state.lock_py_attached(py).unwrap();

            let closed = FutureState::Ready {
                result: clone_result(py, &err_result),
            };
            match std::mem::replace(&mut *state, closed) {
                terminal @ (FutureState::Panicked | FutureState::Ready { .. }) => {
                    *state = terminal;
                    return;
                }

                FutureState::PendingTokio {
                    callbacks,
                    abort_handle,
                    waker,
                } => {
                    abort_handle.abort();
                    (Some(callbacks), Some(waker))
                }

                FutureState::PendingAsyncio { coroutine } => (None, coroutine.into_waker()),
            }
        };

        self.inner.ready.notify_all();

        if let Some(waker) = waker {
            waker.wake();
        }

        if let Some(callbacks) = callbacks {
            CallbackKind::fire_all(py, callbacks, &err_result);
        }
    }

    /// Release the GIL, wait on the condvar until state is Ready or `timeout`
    /// elapses, then return the result. Raises `TimeoutError` on timeout.
    fn wait_for_ready(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Py<PyAny>> {
        let timed_out = py.detach(|| {
            let state = self.inner.state.lock().unwrap();
            match timeout {
                None => {
                    let _guard = self
                        .inner
                        .ready
                        .wait_while(state, |s| !s.is_terminal())
                        .unwrap();
                    false
                }
                Some(timeout) => {
                    let (guard, result) = self
                        .inner
                        .ready
                        .wait_timeout_while(state, timeout, |s| !s.is_terminal())
                        .unwrap();

                    result.timed_out() && !guard.is_terminal()
                }
            }
        });

        if timed_out {
            return Err(PyTimeoutError::new_err("DriverFuture.result() timed out"));
        }

        let state = self.inner.state.lock_py_attached(py).unwrap();
        match &*state {
            FutureState::Ready { result } => clone_result(py, result),
            // The condvar only releases on a terminal state, and the only other
            // terminal state is `Panicked`.
            _ => Err(panicked_err()),
        }
    }

    /// Block until the future is ready, returning the result.
    /// If `timeout` elapses first, raises `TimeoutError`.
    fn block_until_ready(&self, py: Python<'_>, timeout: Option<Duration>) -> PyResult<Py<PyAny>> {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => clone_result(py, result),

            FutureState::PendingTokio { .. } => {
                drop(state);
                self.wait_for_ready(py, timeout)
            }

            FutureState::PendingAsyncio { .. } => {
                Self::ensure_started(&self.inner, &mut state);
                drop(state);
                self.wait_for_ready(py, timeout)
            }

            FutureState::Panicked => Err(panicked_err()),
        }
    }

    /// Register a [`CallbackKind`] on this future.
    ///
    /// - If already `Ready`, invokes the callback immediately.
    /// - If `PendingTokio`, queues it.
    /// - If `PendingAsyncio`, transitions to `PendingTokio` first, then queues it.
    fn register_callback(&self, py: Python<'_>, cb: CallbackKind) {
        let mut state = self.inner.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => {
                let result = clone_result(py, result);
                drop(state);
                cb.invoke(py, &result);
            }

            FutureState::PendingTokio { callbacks, .. } => {
                callbacks.push(cb);
            }

            FutureState::PendingAsyncio { .. } => {
                Self::ensure_started(&self.inner, &mut state);
                if let FutureState::PendingTokio { callbacks, .. } = &mut *state {
                    callbacks.push(cb);
                }
            }

            // The future will never complete, so a queued callback would never fire:
            // report the panic to the callback right away instead.
            FutureState::Panicked => {
                drop(state);
                cb.invoke(py, &Err(panicked_err()));
            }
        }
    }

    /// Throw an exception into the future.
    /// - Ready: re-raises the exception (coroutine is exhausted).
    /// - PendingAsyncio: delegates to `coroutine.poll(py, Some(exc))`.
    /// - PendingTokio: aborts the tokio task, fires the error callbacks,
    ///   transitions to Ready, and re-raises the exception.
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
                    self.inner.ready.notify_all();
                    Err(raise_stop_iteration(py, &result))
                }
            },

            FutureState::PendingTokio {
                callbacks,
                abort_handle,
                waker,
            } => {
                abort_handle.abort();

                let err_result: PyResult<Py<PyAny>> = Err(PyErr::from_value(exc.into_bound(py)));
                *state = FutureState::Ready {
                    result: clone_result(py, &err_result),
                };
                drop(state);

                waker.wake();
                self.inner.ready.notify_all();
                CallbackKind::fire_all(py, callbacks, &err_result);

                // Re-raise the thrown exception.
                err_result
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

    /// Force the transition from `PendingAsyncio` to `PendingTokio`.
    ///
    /// Spawns the inner future onto the tokio runtime immediately, without
    /// waiting for a callback registration or a `result()` call. No-op if
    /// the future is already `PendingTokio` or `Ready`. Returns `self` so
    /// calls can be chained, e.g. `future = session.execute(...).start()`.
    fn start(self_: Py<Self>, py: Python<'_>) -> Py<Self> {
        {
            let this = self_.borrow(py);
            let mut state = this.inner.state.lock_py_attached(py).unwrap();
            Self::ensure_started(&this.inner, &mut state);
        }
        self_
    }

    /// Register a callback to be invoked when the future completes successfully.
    ///
    /// The callback is called as `callback(result)`.
    /// If the future is already done with a success, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    fn on_result(&self, py: Python<'_>, callback: Py<PyAny>) {
        let cb = CallbackKind::on_success(callback);
        self.register_callback(py, cb);
    }

    /// Register a callback to be invoked when the future completes with an error.
    ///
    /// The callback is called as `callback(exception)`.
    /// If the future is already done with an error, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    fn on_error(&self, py: Python<'_>, callback: Py<PyAny>) {
        let cb = CallbackKind::on_error(callback);
        self.register_callback(py, cb);
    }

    /// Register a callback to be invoked when the future completes, whichever way
    /// it goes.
    ///
    /// The callback is called as `callback(future)` with the very future it was
    /// registered on.
    ///
    /// If the future is already done, the callback is invoked immediately.
    /// If the future is pending on asyncio, it is moved to tokio to support callbacks.
    fn on_done(self_: Py<Self>, py: Python<'_>, callback: Py<PyAny>) {
        let cb = CallbackKind::on_done(callback, self_.clone_ref(py));
        self_.borrow(py).register_callback(py, cb);
    }

    /// Get the result of this future.
    ///
    /// If the future is still pending, this blocks the calling thread until
    /// it completes (releasing the GIL while waiting). If `timeout` is
    /// given and elapses before the future completes, raises `TimeoutError`.
    #[pyo3(signature = (timeout=None))]
    fn result(&self, py: Python<'_>, timeout: Option<PyDuration>) -> PyResult<Py<PyAny>> {
        self.block_until_ready(py, timeout.map(|d| d.0))
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDriverFuture>()?;
    Ok(())
}
