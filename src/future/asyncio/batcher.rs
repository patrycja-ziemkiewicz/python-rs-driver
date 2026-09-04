//! Batches the wakeups of parked coroutines, one event-loop trip per batch.
//!
//! Waking a coroutine parked on an `asyncio.Future` means calling `set_result` on
//! the event loop's thread. Doing that from a tokio worker costs one GIL
//! acquisition per completion (`Python::attach`, then `call_soon_threadsafe`,
//! which also writes to the loop's self-pipe) and stalls the loop thread for the
//! whole GIL section.
//!
//! A [`Batcher`] cuts that down to one GIL acquisition per loop iteration.
//! Completions push their parked future onto a queue without touching Python.
//! Only the first push after a drain schedules the drain callback; every later
//! push before the drain runs simply rides along. The drain runs on the loop
//! thread and calls `set_result` on everything queued, no `call_soon_threadsafe`
//! needed per future.
//!
//! One batcher exists per event loop. It is stored on the loop object itself so
//! its lifetime matches the loop's exactly, and it refers back to the loop only
//! weakly, so it forms no cycle the garbage collector cannot see. Loops that
//! cannot carry attributes fall back to the unbatched, per-completion wake.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyAttributeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyCFunction, PyWeakrefMethods, PyWeakrefReference};
use pyo3::{intern, wrap_pyfunction};

/// Attribute under which the loop carries its [`PyBatcherHandle`].
const LOOP_ATTR: &str = "_scylla_completion_batcher";

/// `asyncio.get_running_loop`, resolved once.
pub(super) fn running_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let get_running_loop = GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<_> {
        let asyncio = py.import("asyncio")?;
        Ok(asyncio.getattr("get_running_loop")?.unbind())
    })?;
    get_running_loop.bind(py).call0()
}

/// Call `future.set_result(None)` if the future is not done.
///
/// The future can be cancelled by the event loop before being woken.
/// See <https://github.com/python/cpython/blob/main/Lib/asyncio/tasks.py#L452C5-L452C5>
pub(super) fn release_waiter(future: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = future.py();
    let done = future.call_method0(intern!(py, "done"))?;
    if !done.extract::<bool>()? {
        future.call_method1(intern!(py, "set_result"), (py.None(),))?;
    }
    Ok(())
}

/// The completion queue of one event loop.
pub(crate) struct Batcher {
    /// Parked `asyncio.Future`s waiting for `set_result`.
    queue: Mutex<Vec<Py<PyAny>>>,
    /// Set from the first push after a drain until that drain runs.
    armed: AtomicBool,
    /// `weakref.ref(loop)`. Strong would be a cycle through the loop's attribute
    /// that the garbage collector cannot trace.
    event_loop: Py<PyWeakrefReference>,
}

impl Batcher {
    fn new(event_loop: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            queue: Mutex::new(Vec::new()),
            armed: AtomicBool::new(false),
            event_loop: PyWeakrefReference::new(event_loop)?.unbind(),
        })
    }

    /// Queue `future` for `set_result`, scheduling a drain if none is pending.
    ///
    /// Callable from any thread; takes the GIL only when it has to schedule.
    pub(crate) fn push(&self, future: Py<PyAny>) {
        self.queue.lock().unwrap().push(future);
        if !self.armed.swap(true, Ordering::SeqCst) {
            self.schedule_drain();
        }
    }

    /// Hand the drain to the event loop. Runs once per batch.
    fn schedule_drain(&self) {
        Python::attach(|py| {
            let Some(event_loop) = self.event_loop.bind(py).upgrade() else {
                self.discard(py);
                return;
            };
            let scheduled = event_loop.call_method1(
                intern!(py, "call_soon_threadsafe"),
                (drain_fn(py), &event_loop),
            );
            if let Err(err) = scheduled {
                // `call_soon_threadsafe` raises once the loop is closed; anything
                // else is unexpected and worth hearing about.
                let closed = event_loop
                    .call_method0(intern!(py, "is_closed"))
                    .and_then(|c| c.extract::<bool>())
                    .unwrap_or(true);
                if !closed {
                    log::error!("failed to schedule the completion drain: {err}");
                }
                self.discard(py);
            }
        });
    }

    /// The loop is gone or unusable: nothing queued can ever be woken.
    fn discard(&self, _py: Python<'_>) {
        // Dropping the futures needs the GIL, which the caller holds.
        let dead = std::mem::take(&mut *self.queue.lock().unwrap());
        drop(dead);
        // Let the next push try again, so a dead loop never accumulates a queue.
        self.armed.store(false, Ordering::SeqCst);
    }

    /// Wake everything queued. Runs on the loop thread.
    fn drain(&self, py: Python<'_>) -> PyResult<()> {
        // Clear the flag BEFORE taking the queue: a push that lands during the
        // drain must arm the next one, and no push can be lost between the two.
        self.armed.store(false, Ordering::SeqCst);
        let ready = std::mem::take(&mut *self.queue.lock_py_attached(py).unwrap());

        let mut first_err = None;
        for future in ready {
            if let Err(err) = release_waiter(future.bind(py)) {
                first_err.get_or_insert(err);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

/// Python-visible owner of a [`Batcher`], stored on the event loop.
#[pyclass(name = "_CompletionBatcher", frozen, module = "scylla._rust.future")]
pub(crate) struct PyBatcherHandle {
    batcher: Arc<Batcher>,
}

/// Wake every future queued on `event_loop`'s batcher.
#[pyfunction]
fn drain(event_loop: &Bound<'_, PyAny>) -> PyResult<()> {
    let handle = event_loop.getattr(intern!(event_loop.py(), LOOP_ATTR))?;
    let handle = handle.cast::<PyBatcherHandle>()?;
    handle.get().batcher.drain(event_loop.py())
}

fn drain_fn(py: Python<'_>) -> &Bound<'_, PyCFunction> {
    static DRAIN: PyOnceLock<Py<PyCFunction>> = PyOnceLock::new();
    DRAIN
        .get_or_init(py, || {
            wrap_pyfunction!(drain, py)
                .expect("wrapping a pyfunction cannot fail")
                .unbind()
        })
        .bind(py)
}

/// The batcher of `event_loop`, created on first use.
///
/// `None` if the loop cannot carry attributes, in which case the caller must
/// wake its future the unbatched way.
pub(crate) fn batcher_for(event_loop: &Bound<'_, PyAny>) -> PyResult<Option<Arc<Batcher>>> {
    let py = event_loop.py();
    match event_loop.getattr(intern!(py, LOOP_ATTR)) {
        Ok(handle) => {
            let handle = handle.cast::<PyBatcherHandle>()?;
            return Ok(Some(Arc::clone(&handle.get().batcher)));
        }
        Err(err) if err.is_instance_of::<PyAttributeError>(py) => {}
        Err(err) => return Err(err),
    }

    let batcher = Arc::new(Batcher::new(event_loop)?);
    let handle = Py::new(
        py,
        PyBatcherHandle {
            batcher: Arc::clone(&batcher),
        },
    )?;
    match event_loop.setattr(intern!(py, LOOP_ATTR), handle) {
        Ok(()) => Ok(Some(batcher)),
        // A loop without `__dict__` (a C-implemented one, say) rejects the
        // attribute. Such a loop gets no batching.
        Err(err)
            if err.is_instance_of::<PyAttributeError>(py)
                || err.is_instance_of::<PyTypeError>(py) =>
        {
            Ok(None)
        }
        Err(err) => Err(err),
    }
}
