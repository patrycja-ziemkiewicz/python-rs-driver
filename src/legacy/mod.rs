//! The legacy (`cassandra-driver` compatible) API.

use pyo3::prelude::*;

use crate::future::PyResponseFuture;

pub(crate) mod errors;
pub(crate) mod pending_response_page;
pub(crate) mod session;

use errors::QueryExhausted;

#[pymodule]
pub(crate) fn legacy(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    module.add("QueryExhausted", py.get_type::<QueryExhausted>())?;
    Ok(())
}
