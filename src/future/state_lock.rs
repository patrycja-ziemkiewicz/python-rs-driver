//! A state that threads can block on until it changes, shared by the futures.

use std::ops::{Deref, DerefMut};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

use pyo3::Python;
use pyo3::sync::MutexExt;

use crate::utils::thread_is_attached;

/// A state behind a mutex, with a condvar for the threads that block until it
/// changes.
pub(super) struct StateLock<S> {
    inner: Mutex<Inner<S>>,
    condvar: Condvar,
}

struct Inner<S> {
    state: S,
    /// Threads blocked in `wait_while`. Lets `notify` skip the condvar's
    /// syscall when nobody waits.
    waiters: usize,
}

/// A locked [`StateLock`]. Dereferences to its state.
pub(super) struct StateGuard<'a, S>(MutexGuard<'a, Inner<S>>);

impl<S> Deref for StateGuard<'_, S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.0.state
    }
}

impl<S> DerefMut for StateGuard<'_, S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.0.state
    }
}

impl<S> StateGuard<'_, S> {
    /// Threads blocked in `wait_while` now. Pass it to [`StateLock::notify`]
    /// after the guard is dropped.
    pub(super) fn waiters(&self) -> usize {
        self.0.waiters
    }
}

impl<S: Send> StateLock<S> {
    pub(super) fn new(state: S) -> Self {
        Self {
            inner: Mutex::new(Inner { state, waiters: 0 }),
            condvar: Condvar::new(),
        }
    }

    pub(super) fn lock_py_attached(&self, py: Python<'_>) -> StateGuard<'_, S> {
        StateGuard(self.inner.lock_py_attached(py).unwrap())
    }

    /// Locks from a thread that is not attached, such as a runtime worker.
    pub(super) fn lock_detached(&self) -> StateGuard<'_, S> {
        debug_assert!(!thread_is_attached(), "lock_detached on an attached thread");
        #[expect(clippy::disallowed_methods, reason = "the caller is not attached")]
        StateGuard(self.inner.lock().unwrap())
    }

    /// Wakes the threads blocked in `wait_while`, if `waiters` says there are any.
    pub(super) fn notify(&self, waiters: usize) {
        if waiters > 0 {
            self.condvar.notify_all();
        }
    }

    /// Releases the GIL and blocks while `pending(&state)` is true, or until
    /// `timeout` elapses. Returns whether it timed out.
    pub(super) fn wait_while(
        &self,
        py: Python<'_>,
        timeout: Option<Duration>,
        mut pending: impl FnMut(&S) -> bool + Send,
    ) -> bool {
        #[expect(
            clippy::disallowed_methods,
            reason = "every wait below is inside py.detach"
        )]
        let timed_out = py.detach(|| {
            let StateGuard(mut inner) = self.lock_detached();
            if !pending(&inner.state) {
                return false;
            }

            inner.waiters += 1;
            let (mut inner, timed_out) = match timeout {
                None => {
                    let inner = self
                        .condvar
                        .wait_while(inner, |inner| pending(&inner.state))
                        .unwrap();
                    (inner, false)
                }
                Some(timeout) => {
                    let (inner, result) = self
                        .condvar
                        .wait_timeout_while(inner, timeout, |inner| pending(&inner.state))
                        .unwrap();
                    let timed_out = result.timed_out() && pending(&inner.state);
                    (inner, timed_out)
                }
            };
            inner.waiters -= 1;
            timed_out
        });
        timed_out
    }
}
