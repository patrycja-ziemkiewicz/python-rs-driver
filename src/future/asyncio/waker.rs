// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.
//
// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3
//
// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.

//! Changes from the original pyo3 source:
//!
//! - The `PyOnceLock<Option<LoopAndFuture>>` became a `Mutex<WakerSlot>`. Upstream's
//!   once-cell has two costs on the hot path: its initialisation detaches from the
//!   interpreter and reattaches (a GIL release per parked coroutine, and the moment a
//!   tokio worker steals the GIL), and it can only be reset through `&mut self`, which
//!   forced a fresh `Arc` whenever the event loop still held the old one. The mutex is
//!   uncontended in practice and resets in place.
//!
//! - Added `yield_asyncio_future` to encapsulate parking: it creates the asyncio future
//!   and yields it, or returns `py.None()` if the waker was already woken (the
//!   `sleep(0)` equivalent).

use std::sync::{Arc, Mutex};
use std::task::Wake;

use pyo3::prelude::*;
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyCFunction, PyIterator};
use pyo3::{intern, wrap_pyfunction};

/// Where the coroutine using this waker currently is.
enum WakerSlot {
    /// Not parked, no wake pending.
    Idle,
    /// Woken before it could park: the next `yield_asyncio_future` must yield None.
    Woken,
    /// Parked on an `asyncio.Future`.
    Parked(Parked),
}

/// A parked coroutine's `asyncio.Future` and the loop that has to wake it.
struct Parked {
    future: Py<PyAny>,
    event_loop: Py<PyAny>,
}

/// Lazy `asyncio.Future` wrapper, implementing [`Wake`] by arranging for
/// `Future.set_result` to run on the event loop thread.
///
/// The asyncio future is left uninitialized until [`yield_asyncio_future`] is called.
/// If [`wake`] is called before that (during Rust future polling),
/// [`yield_asyncio_future`] yields `None` instead (roughly `asyncio.sleep(0)`).
///
/// [`yield_asyncio_future`]: AsyncioWaker::yield_asyncio_future
/// [`wake`]: Wake::wake
pub(crate) struct AsyncioWaker {
    slot: Mutex<WakerSlot>,
}

impl AsyncioWaker {
    pub(crate) fn new() -> Self {
        Self {
            slot: Mutex::new(WakerSlot::Idle),
        }
    }

    /// Forget any pending wake or parked future. Called right before a poll, so a
    /// wake that arrives during the poll is the only one that counts.
    pub(crate) fn reset(&self, py: Python<'_>) {
        // A `Parked` dropped here releases its `Py`s, which needs the GIL we hold.
        *self.slot.lock_py_attached(py).unwrap() = WakerSlot::Idle;
    }

    /// Park the coroutine: create the asyncio future and yield it.
    /// Returns `py.None()` if the waker was already woken (sleep(0) equivalent).
    pub(crate) fn yield_asyncio_future(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut slot = self.slot.lock_py_attached(py).unwrap();

        match &*slot {
            WakerSlot::Woken => {
                *slot = WakerSlot::Idle;
                return Ok(py.None());
            }
            // Polled again while still parked (nothing woke us): keep waiting on
            // the same future, unless it is done, in which case park afresh below.
            WakerSlot::Parked(parked) => {
                if let Some(yielded) = yield_future(parked.future.bind(py))? {
                    return Ok(yielded);
                }
            }
            WakerSlot::Idle => {}
        }

        let event_loop = running_loop(py)?;
        let future = event_loop.call_method0(intern!(py, "create_future"))?;
        let yielded = yield_future(&future)?.expect("a fresh asyncio.Future is not done");

        *slot = WakerSlot::Parked(Parked {
            future: future.unbind(),
            event_loop: event_loop.unbind(),
        });
        Ok(yielded)
    }
}

/// `asyncio.get_running_loop`, resolved once.
fn running_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let get_running_loop = GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<_> {
        let asyncio = py.import("asyncio")?;
        Ok(asyncio.getattr("get_running_loop")?.unbind())
    })?;
    get_running_loop.bind(py).call0()
}

/// What to yield to the event loop to park on `future`: the future itself, or
/// `None` if it is already done.
///
/// `asyncio.Future` must be awaited; fortunately, it implements `__iter__ = __await__`
/// and yields itself, flagged as blocking, if its result has not been set.
fn yield_future<'py>(future: &Bound<'py, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    PyIterator::from_object(future)
        .expect("asyncio.Future implements __iter__ = __await__")
        .next()
        .map(|yielded| yielded.map(Bound::unbind))
        .transpose()
}

impl Wake for AsyncioWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let parked = {
            let mut slot = self.slot.lock().unwrap();
            match std::mem::replace(&mut *slot, WakerSlot::Woken) {
                // The wake is delivered below; nothing stays pending.
                WakerSlot::Parked(parked) => {
                    *slot = WakerSlot::Idle;
                    parked
                }
                // Idle becomes Woken; Woken stays Woken.
                WakerSlot::Idle | WakerSlot::Woken => return,
            }
        };

        wake_directly(parked.future, parked.event_loop);
    }
}

/// Upstream's per-wake path: `call_soon_threadsafe(release_waiter, future)`.
fn wake_directly(future: Py<PyAny>, event_loop: Py<PyAny>) {
    Python::attach(|py| {
        static RELEASE_WAITER: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
        let release_waiter = RELEASE_WAITER.get_or_init(py, || {
            wrap_pyfunction!(release_waiter_fn, py)
                .expect("wrapping a pyfunction cannot fail")
                .unbind()
        });
        // `Future.set_result` must be called in the event loop thread,
        // so it requires `call_soon_threadsafe`
        let scheduled = event_loop.call_method1(
            py,
            intern!(py, "call_soon_threadsafe"),
            (release_waiter, &future),
        );
        if let Err(err) = scheduled {
            // `call_soon_threadsafe` raises if the event loop is closed; instead of
            // catching an unspecific `RuntimeError`, check directly if it's closed.
            let closed = event_loop
                .call_method0(py, intern!(py, "is_closed"))
                .and_then(|c| c.extract::<bool>(py))
                .unwrap_or(true);
            if !closed {
                log::error!("unexpected error in coroutine waker: {err}");
            }
        }
    });
}

/// Call `future.set_result(None)` if the future is not done.
///
/// The future can be cancelled by the event loop before being woken.
/// See <https://github.com/python/cpython/blob/main/Lib/asyncio/tasks.py#L452C5-L452C5>
fn release_waiter(future: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = future.py();
    let done = future.call_method0(intern!(py, "done"))?;
    if !done.extract::<bool>()? {
        future.call_method1(intern!(py, "set_result"), (py.None(),))?;
    }
    Ok(())
}

#[pyo3::pyfunction]
#[pyo3(name = "release_waiter")]
fn release_waiter_fn(future: &Bound<'_, PyAny>) -> PyResult<()> {
    release_waiter(future)
}
