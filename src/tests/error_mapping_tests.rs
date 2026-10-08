use std::time::Duration;

use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use scylla::errors::ExecutionError;

use crate::errors::OperationTimedOut;
use crate::errors::request::execution_error_to_pyerr;

fn attr<'py, T: FromPyObjectOwned<'py>>(py: Python<'py>, err: &PyErr, name: &str) -> T {
    let value = err.value(py).getattr(name).unwrap();
    value.extract().map_err(Into::<PyErr>::into).unwrap()
}

#[test]
fn request_timeout_is_in_seconds() {
    Python::initialize();
    let err = ExecutionError::RequestTimeout(Duration::from_millis(1500));
    let err = execution_error_to_pyerr(&err, err.to_string());
    Python::attach(|py| {
        assert!(err.is_instance_of::<OperationTimedOut>(py));
        assert!(err.is_instance_of::<PyTimeoutError>(py));
        assert_eq!(attr::<f64>(py, &err, "timeout"), 1.5);
    });
}
