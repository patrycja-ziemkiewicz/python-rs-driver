//! Spawning futures on the runtime with a guaranteed completion signal.

use std::future::Future;
use std::marker::PhantomData;

use tokio::task::AbortHandle;

use crate::RUNTIME;

/// The task was torn down before its future completed: aborted, or the runtime
/// shutting down.
pub(in crate::future) struct TaskDropped;

/// Spawns `future` on the runtime and hands its outcome to `on_done`.
///
/// If the task is torn down first, `on_done` gets `Err(TaskDropped)`, so whoever
/// waits on the outcome is always told.
pub(in crate::future) fn spawn_guarded<F, C>(future: F, on_done: C) -> AbortHandle
where
    F: Future + Send + 'static,
    C: FnOnce(Result<F::Output, TaskDropped>) + Send + 'static,
{
    RUNTIME
        .spawn(async move {
            let mut guard = DropGuard {
                on_done: Some(on_done),
                _output: PhantomData,
            };
            let output = future.await;
            if let Some(on_done) = guard.on_done.take() {
                on_done(Ok(output));
            }
        })
        .abort_handle()
}

/// Calls `on_done` with `Err(TaskDropped)` when dropped, unless it was taken first.
struct DropGuard<T, C: FnOnce(Result<T, TaskDropped>)> {
    on_done: Option<C>,
    _output: PhantomData<fn(T)>,
}

impl<T, C: FnOnce(Result<T, TaskDropped>)> Drop for DropGuard<T, C> {
    fn drop(&mut self) {
        if let Some(on_done) = self.on_done.take() {
            on_done(Err(TaskDropped));
        }
    }
}
