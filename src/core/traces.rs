use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

/// A fetched trace that becomes a [`QueryTrace`] when handed to Python.
pub(crate) struct PendingQueryTrace {
    pub(crate) trace_id: Uuid,
    pub(crate) info: TracingInfo,
}
