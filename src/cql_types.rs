use pyo3::prelude::*;

#[pyclass(module = "scylla.cql_types", frozen)]
pub struct CqlEmpty;

#[pymethods]
impl CqlEmpty {
    fn __repr__(&self) -> &'static str {
        "CqlEmpty"
    }
}

#[pymodule]
pub(crate) fn cql_types(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CqlEmpty>()?;
    Ok(())
}
