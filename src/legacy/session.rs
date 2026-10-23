//! `LegacySession`: the blocking, callback-driven session of the legacy driver.

use pyo3::prelude::*;

use crate::batch::{BatchState, PyBatch};
use crate::core::session::ExecutableStatement;
use crate::errors::execution::DriverStatementConversionError;

/// What `execute_async` accepts: a batch, or anything executable as one statement.
#[derive(Clone)]
#[expect(dead_code, reason = "LegacySession reads it; it lands in a later PR")]
pub(crate) enum LegacyQuery {
    Batch(BatchState),
    Statement(ExecutableStatement),
}

impl LegacyQuery {
    pub(crate) fn is_batch(&self) -> bool {
        matches!(self, Self::Batch(_))
    }
}

impl<'py> FromPyObject<'_, 'py> for LegacyQuery {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(batch) = obj.cast::<PyBatch>() {
            return Ok(Self::Batch(batch.get().snapshot(obj.py())));
        }

        Ok(Self::Statement(ExecutableStatement::extract(obj)?))
    }
}
