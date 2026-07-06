use crate::enums::PyConsistency;
use crate::enums::PySerialConsistency;
use crate::routing::PyToken;
use pyo3::PyAny;
use pyo3::prelude::*;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::PyString;
use pyo3::{Bound, BoundObject, Py, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::cluster::ClusterState;
use scylla::frame::response::result::TableSpec;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::load_balancing::RoutingInfo;
use scylla::routing::NodeLocationPreference;
use scylla::routing::Shard;
use scylla::routing::Token;
use scylla::statement::{Consistency, SerialConsistency};
use std::fmt::Debug;
use std::sync::Arc;

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

/// Python representation of routing information for a request.
/// Exposed to Python as `RoutingInfo`.
#[pyclass(frozen, name = "RoutingInfo")]
pub struct PyRoutingInfo {
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    token: Option<Token>,
    ks_name: Option<String>,
    table_name: Option<String>,
    #[pyo3(get)]
    is_confirmed_lwt: bool,
    node_location_preference: NodeLocationPreference,

    // Cached Python-side representations used by the getters.
    py_consistency: PyOnceLock<Py<PyConsistency>>,
    py_serial_consistency: PyOnceLock<Py<PySerialConsistency>>,
    py_token: PyOnceLock<Py<PyToken>>,
    py_ks: PyOnceLock<Py<PyString>>,
    py_table: PyOnceLock<Py<PyString>>,
    py_preferred_datacenter: PyOnceLock<Py<PyString>>,
    py_preferred_rack: PyOnceLock<Py<PyString>>,
    py_node_location_preference: PyOnceLock<Py<PyNodeLocationPreference>>,
}

impl<'a> From<&RoutingInfo<'a>> for PyRoutingInfo {
    fn from(info: &RoutingInfo<'a>) -> Self {
        let ks_name = info.table.map(|t| t.ks_name().to_string());
        let table_name = info.table.map(|t| t.table_name().to_string());

        Self {
            consistency: info.consistency,
            serial_consistency: info.serial_consistency,
            token: info.token,
            ks_name,
            table_name,
            is_confirmed_lwt: info.is_confirmed_lwt,
            node_location_preference: info.node_location_preference.clone(),

            py_consistency: PyOnceLock::new(),
            py_serial_consistency: PyOnceLock::new(),
            py_token: PyOnceLock::new(),
            py_ks: PyOnceLock::new(),
            py_table: PyOnceLock::new(),
            py_preferred_datacenter: PyOnceLock::new(),
            py_preferred_rack: PyOnceLock::new(),
            py_node_location_preference: PyOnceLock::new(),
        }
    }
}

impl PyRoutingInfo {
    fn to_routing_info<'a>(&'a self, table_spec: Option<&'a TableSpec<'a>>) -> RoutingInfo<'a> {
        RoutingInfo::new(
            self.consistency,
            self.serial_consistency,
            self.token,
            table_spec,
            self.is_confirmed_lwt,
            &self.node_location_preference,
        )
    }
}

#[pymethods]
impl PyRoutingInfo {
    #[getter]
    fn consistency(&self, py: Python<'_>) -> PyResult<Py<PyConsistency>> {
        let bound_enum = self.py_consistency.get_or_try_init(py, || {
            let py_enum: PyConsistency = self.consistency.into();
            Py::new(py, py_enum)
        })?;
        Ok(bound_enum.clone_ref(py))
    }

    #[getter]
    fn serial_consistency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.serial_consistency {
            None => Ok(py.None()),
            Some(sc) => {
                let bound_enum = self.py_serial_consistency.get_or_try_init(py, || {
                    let py_enum: PySerialConsistency = sc.into();
                    Py::new(py, py_enum)
                })?;
                Ok(bound_enum.clone_ref(py).into_any())
            }
        }
    }

    #[getter]
    fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.token {
            None => Ok(py.None()),
            Some(token) => {
                let bound_token = self
                    .py_token
                    .get_or_try_init(py, || Py::new(py, PyToken::from(token)))?;
                Ok(bound_token.clone_ref(py).into_any())
            }
        }
    }

    #[getter]
    fn keyspace(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.ks_name {
            None => py.None(),
            Some(ks) => {
                let bound_str = self
                    .py_ks
                    .get_or_init(py, || PyString::new(py, ks).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn table(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.table_name {
            None => py.None(),
            Some(table) => {
                let bound_str = self
                    .py_table
                    .get_or_init(py, || PyString::new(py, table).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn preferred_datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        match self.node_location_preference.datacenter() {
            None => py.None(),
            Some(dc) => {
                let bound_str = self
                    .py_preferred_datacenter
                    .get_or_init(py, || PyString::new(py, dc).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn preferred_rack(&self, py: Python<'_>) -> Py<PyAny> {
        match self.node_location_preference.rack() {
            None => py.None(),
            Some(rack) => {
                let bound_str = self
                    .py_preferred_rack
                    .get_or_init(py, || PyString::new(py, rack).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn node_location_preference(&self, py: Python<'_>) -> PyResult<Py<PyNodeLocationPreference>> {
        let bound_pref = self.py_node_location_preference.get_or_try_init(py, || {
            match &self.node_location_preference {
                NodeLocationPreference::Any => Ok(PyNodeLocationPreference::ANY(py)),
                NodeLocationPreference::Datacenter(dc) => {
                    let dc_py = PyString::new(py, dc).unbind();
                    PyNodeLocationPreference::datacenter(py, dc_py)
                }
                NodeLocationPreference::DatacenterAndRack(dc, rack) => {
                    let dc_py = PyString::new(py, dc).unbind();
                    let rack_py = PyString::new(py, rack).unbind();
                    PyNodeLocationPreference::datacenter_and_rack(py, dc_py, rack_py)
                }
                _ => unreachable!("We need consider this branch as it is non exhaustive"),
            }
        })?;
        Ok(bound_pref.clone_ref(py))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "RoutingInfo(consistency={:?}, serial_consistency={:?}, token={}, keyspace={:?}, table={:?}, is_confirmed_lwt={})",
                self.consistency,
                self.serial_consistency,
                self.token(py)?.bind(py).repr()?,
                self.ks_name,
                self.table_name,
                self.is_confirmed_lwt
            ),
        )?;

        Ok(repr_str.into())
    }
}

/// Built-in load balancing policy, equivalent to the Rust driver's DefaultPolicy.
#[derive(Debug)]
#[pyclass(name = "DefaultPolicy", frozen)]
struct PyDefaultPolicy {
    inner: Arc<dyn LoadBalancingPolicy>,
    #[pyo3(get)]
    token_aware: bool,
    #[pyo3(get)]
    permit_dc_failover: bool,
    #[pyo3(get)]
    enable_shuffling_replicas: bool,
    #[pyo3(get)]
    node_location_preference: Option<Py<PyNodeLocationPreference>>,
}

#[pymethods]
impl PyDefaultPolicy {
    #[new]
    #[pyo3(signature = (*,
        node_location_preference = None,
        token_aware = true,
        permit_dc_failover = false,
        enable_shuffling_replicas = true,
    ))]
    fn new(
        py: Python<'_>,
        node_location_preference: Option<Py<PyNodeLocationPreference>>,
        token_aware: bool,
        permit_dc_failover: bool,
        enable_shuffling_replicas: bool,
    ) -> Result<Self, DriverLoadBalancingPolicyError> {
        let mut builder = DefaultPolicy::builder();

        builder = builder
            .enable_shuffling_replicas(enable_shuffling_replicas)
            .permit_dc_failover(permit_dc_failover)
            .token_aware(token_aware);

        if let Some(ref pref) = node_location_preference {
            let pref = pref.get();
            match (&pref.preferred_datacenter, &pref.preferred_rack) {
                (Some(dc), Some(rack)) => {
                    let dc_str = dc
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();
                    let rack_str = rack
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();
                    builder = builder.prefer_datacenter_and_rack(dc_str, rack_str);
                }
                (Some(dc), None) => {
                    let dc_str = dc
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();
                    builder = builder.prefer_datacenter(dc_str);
                }
                (None, None) => {
                    builder = builder.prefer_no_datacenter();
                }
                (None, Some(_)) => unreachable!("rack without datacenter is not allowed"),
            }
        }

        Ok(Self {
            inner: builder.build(),
            node_location_preference,
            token_aware,
            permit_dc_failover,
            enable_shuffling_replicas,
        })
    }

    /// The preferred datacenter, or None. Convenience getter.
    #[getter]
    fn preferred_datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_location_preference
            .as_ref()
            .and_then(|pref| pref.get().preferred_datacenter.as_ref())
            .map_or_else(|| py.None(), |dc| dc.clone_ref(py).into_any())
    }

    /// The preferred rack, or None. Convenience getter.
    #[getter]
    fn preferred_rack(&self, py: Python<'_>) -> Py<PyAny> {
        self.node_location_preference
            .as_ref()
            .and_then(|pref| pref.get().preferred_rack.as_ref())
            .map_or_else(|| py.None(), |rack| rack.clone_ref(py).into_any())
    }

    fn pick_targets(
        &self,
        py: Python,
        py_routing_info: Py<PyRoutingInfo>,
        py_cluster_state: Py<PyClusterState>,
    ) -> PyResult<Py<PyList>> {
        let py_routing_info = py_routing_info.get();
        let py_cluster_state = py_cluster_state.get();

        let local_spec = match (&py_routing_info.ks_name, &py_routing_info.table_name) {
            (Some(ks), Some(table)) => Some(TableSpec::borrowed(ks.as_str(), table.as_str())),
            _ => None,
        };

        let routing_info = py_routing_info.to_routing_info(local_spec.as_ref());
        let cluster_state = py_cluster_state._inner.as_ref();

        let fallback = self.inner.fallback(&routing_info, cluster_state);

        let list = PyList::empty(py);
        let known_nodes = py_cluster_state.known_nodes.bind(py);

        for (node, shard) in fallback {
            let py_node = known_nodes
                .get_item(node.host_id)?
                .expect("node can't be known by Rust Driver and simultaneously None");
            list.append((py_node, shard))?;
        }

        Ok(list.unbind())
    }
}

#[pymodule]
pub(crate) fn load_balancing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNodeLocationPreference>()?;
    module.add_class::<PyDefaultPolicy>()?;
    module.add_class::<PyRoutingInfo>()?;
    Ok(())
}
