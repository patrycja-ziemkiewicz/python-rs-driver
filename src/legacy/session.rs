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

impl LegacyQuery {
    /// Whether this is a batch: a `Batch`, or a statement whose text begins one.
    pub(crate) fn is_batch(&self) -> bool {
        match self {
            Self::Batch(_) => true,
            Self::Statement(statement) => begins_batch(statement.contents()),
        }
    }
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

/// `^\s*BEGIN\s+[a-zA-Z]*\s*BATCH`, as the legacy driver matched it.
fn begins_batch(text: &str) -> bool {
    let Some(rest) = text.trim_start().strip_prefix("BEGIN") else {
        return false;
    };
    let trimmed = rest.trim_start();
    if trimmed.len() == rest.len() {
        return false;
    }
    trimmed.starts_with("BATCH")
        || trimmed
            .trim_start_matches(|c: char| c.is_ascii_alphabetic())
            .trim_start()
            .starts_with("BATCH")
}
