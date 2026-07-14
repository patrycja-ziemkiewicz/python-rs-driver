use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::{Py, PyAny, PyResult};
/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Option<Py<PyTuple>>,
    kwargs: Option<Py<PyDict>>,
}
