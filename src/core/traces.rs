use std::sync::Arc;

use scylla::client::session::Session;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

use crate::errors::execution::DriverTraceError;

/// A fetched trace that becomes a [`QueryTrace`] when handed to Python.
pub(crate) struct PendingQueryTrace {
    pub(crate) trace_id: Uuid,
    pub(crate) info: TracingInfo,
}

/// Fetches the trace of the request with `trace_id`.
pub(crate) async fn fetch_trace(
    session: Arc<Session>,
    trace_id: Uuid,
) -> Result<PendingQueryTrace, DriverTraceError> {
    let info = session
        .get_tracing_info(&trace_id)
        .await
        .map_err(DriverTraceError::rust_driver_tracing_error)?;

    Ok(PendingQueryTrace { trace_id, info })
}
