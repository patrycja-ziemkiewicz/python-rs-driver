use crate::enums::PyConsistency;
use crate::enums::PySerialConsistency;
use crate::routing::PyToken;
use pyo3::prelude::*;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::types::PyString;
use pyo3::{Bound, BoundObject, Py, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::cluster::ClusterState;
use scylla::frame::response::result::TableSpec;
use scylla::policies::load_balancing::RoutingInfo;
use scylla::routing::NodeLocationPreference;
use scylla::routing::Shard;
use scylla::routing::Token;
use scylla::statement::{Consistency, SerialConsistency};

/// Describes the preferred location of nodes to contact when executing requests.
#[pyclass(name = "NodeLocationPreference", frozen)]
struct PyNodeLocationPreference {
    #[pyo3(get)]
    preferred_datacenter: Option<Py<PyString>>,
    #[pyo3(get)]
    preferred_rack: Option<Py<PyString>>,
}

#[pymethods]
impl PyNodeLocationPreference {
    #[classattr]
    #[allow(non_snake_case)]
    fn ANY(py: Python<'_>) -> Py<PyNodeLocationPreference> {
        static ANY_INSTANCE: PyOnceLock<Py<PyNodeLocationPreference>> = PyOnceLock::new();
        ANY_INSTANCE
            .get_or_init(py, || {
                Py::new(
                    py,
                    PyNodeLocationPreference {
                        preferred_datacenter: None,
                        preferred_rack: None,
                    },
                )
                .expect("Failed to create NodeLocationPreference.ANY instance")
            })
            .clone_ref(py)
    }

    #[staticmethod]
    fn datacenter(py: Python<'_>, name: Py<PyString>) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                preferred_datacenter: Some(name),
                preferred_rack: None,
            },
        )
    }

    #[staticmethod]
    fn datacenter_and_rack(
        py: Python<'_>,
        datacenter_name: Py<PyString>,
        rack_name: Py<PyString>,
    ) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                preferred_datacenter: Some(datacenter_name),
                preferred_rack: Some(rack_name),
            },
        )
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let repr_str = match (&self.preferred_datacenter, &self.preferred_rack) {
            (None, None) => PyString::new(py, "NodeLocationPreference.ANY"),
            (Some(dc), None) => {
                PyString::from_fmt(py, format_args!("NodeLocationPreference.datacenter({dc})"))?
            }
            (Some(dc), Some(rack)) => PyString::from_fmt(
                py,
                format_args!("NodeLocationPreference.datacenter_and_rack({dc}, {rack})"),
            )?,
            (None, Some(_)) => unreachable!("rack without datacenter is not allowed"),
        };

        Ok(repr_str.into())
    }
}

#[pymodule]
pub(crate) fn load_balancing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNodeLocationPreference>()?;
    Ok(())
}
