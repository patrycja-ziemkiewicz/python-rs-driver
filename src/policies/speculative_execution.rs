use crate::errors::DriverSpeculativeExecutionPolicyError;
use crate::session_builder::PyDuration;
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Borrowed, Bound, FromPyObject, PyAny, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::policies::speculative_execution::{
    SimpleSpeculativeExecutionPolicy, SpeculativeExecutionPolicy,
};
use std::sync::Arc;
use std::time::Duration;

/// Built-in speculative execution policy that starts a new execution of the request
/// every `delay` seconds, at most `max_attempts` times.
#[pyclass(name = "SimpleSpeculativeExecutionPolicy", frozen)]
#[derive(Debug)]
pub(crate) struct PySimpleSpeculativeExecutionPolicy {
    pub(crate) inner: Arc<SimpleSpeculativeExecutionPolicy>,
}

#[pymethods]
impl PySimpleSpeculativeExecutionPolicy {
    #[new]
    fn new(delay: PyDuration, max_attempts: usize) -> Self {
        Self {
            inner: Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: max_attempts,
                retry_interval: delay.0,
            }),
        }
    }

    #[getter]
    fn get_delay(&self) -> Duration {
        self.inner.retry_interval
    }

    #[getter]
    fn get_max_attempts(&self) -> usize {
        self.inner.max_retry_count
    }
}

#[pymodule]
pub(crate) fn speculative_execution(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySimpleSpeculativeExecutionPolicy>()?;
    Ok(())
}
