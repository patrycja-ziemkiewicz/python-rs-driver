//! The legacy (`cassandra-driver` compatible) API.

use pyo3::prelude::*;

pub(crate) mod result_set;
pub(crate) mod session;

pub(crate) use result_set::PyResultSet;

#[pymodule]
pub(crate) fn legacy(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResultSet>()?;
    Ok(())
}
