//! `LegacySession`: the blocking, callback-driven session of the legacy driver.

use pyo3::prelude::*;

use crate::batch::PyBatch;
use crate::core::session::ExecutableStatement;
use crate::errors::DriverStatementConversionError;

/// What `execute_async` accepts: a batch, or anything executable as one statement.
#[derive(Clone)]
pub(crate) enum LegacyQuery {
    Batch(PyBatch),
    Statement(ExecutableStatement),
}

impl<'py> FromPyObject<'_, 'py> for LegacyQuery {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(batch) = obj.cast::<PyBatch>() {
            return Ok(Self::Batch(batch.borrow().clone()));
        }

        Ok(Self::Statement(ExecutableStatement::extract(obj)?))
    }
}
