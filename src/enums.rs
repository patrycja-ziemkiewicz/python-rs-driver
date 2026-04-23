use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::client::{PoolSize, SelfIdentity, WriteCoalescingDelay};
use scylla::statement;
use scylla_cql::frame::Compression;
use std::num::{NonZeroU64, NonZeroUsize};

#[pyclass(eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Consistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    LocalOne,
    Serial,
    LocalSerial,
}

impl Consistency {
    pub(crate) fn to_rust(self) -> statement::Consistency {
        match self {
            Consistency::Any => statement::Consistency::Any,
            Consistency::One => statement::Consistency::One,
            Consistency::Two => statement::Consistency::Two,
            Consistency::Three => statement::Consistency::Three,
            Consistency::Quorum => statement::Consistency::Quorum,
            Consistency::All => statement::Consistency::All,
            Consistency::LocalQuorum => statement::Consistency::LocalQuorum,
            Consistency::EachQuorum => statement::Consistency::EachQuorum,
            Consistency::LocalOne => statement::Consistency::LocalOne,
            Consistency::Serial => statement::Consistency::Serial,
            Consistency::LocalSerial => statement::Consistency::LocalSerial,
        }
    }

    pub(crate) fn to_python(consistency: statement::Consistency) -> Self {
        match consistency {
            statement::Consistency::Any => Consistency::Any,
            statement::Consistency::One => Consistency::One,
            statement::Consistency::Two => Consistency::Two,
            statement::Consistency::Three => Consistency::Three,
            statement::Consistency::Quorum => Consistency::Quorum,
            statement::Consistency::All => Consistency::All,
            statement::Consistency::LocalQuorum => Consistency::LocalQuorum,
            statement::Consistency::EachQuorum => Consistency::EachQuorum,
            statement::Consistency::LocalOne => Consistency::LocalOne,
            statement::Consistency::Serial => Consistency::Serial,
            statement::Consistency::LocalSerial => Consistency::LocalSerial,
        }
    }
}

#[pyclass(eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum SerialConsistency {
    Serial,
    LocalSerial,
}

impl SerialConsistency {
    pub(crate) fn to_rust(self) -> statement::SerialConsistency {
        match self {
            SerialConsistency::Serial => statement::SerialConsistency::Serial,
            SerialConsistency::LocalSerial => statement::SerialConsistency::LocalSerial,
        }
    }

    pub(crate) fn to_python(consistency: statement::SerialConsistency) -> Self {
        match consistency {
            statement::SerialConsistency::Serial => SerialConsistency::Serial,
            statement::SerialConsistency::LocalSerial => SerialConsistency::LocalSerial,
        }
    }
}

#[pyclass(eq, eq_int, frozen, from_py_object, name = "Compression")]
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum PyCompression {
    Lz4,
    Snappy,
}

impl From<PyCompression> for Compression {
    fn from(value: PyCompression) -> Self {
        match value {
            PyCompression::Lz4 => Self::Lz4,
            PyCompression::Snappy => Self::Snappy,
        }
    }
}

#[pyclass(name = "PoolSize", from_py_object, frozen)]
#[derive(Clone, Copy, Debug)]
pub struct PyPoolSize {
    pub(crate) inner: PoolSize,
}

#[pymethods]
impl PyPoolSize {
    #[staticmethod]
    fn per_host(connections: NonZeroUsize) -> PyResult<Self> {
        Ok(Self {
            inner: PoolSize::PerHost(connections),
        })
    }

    #[staticmethod]
    fn per_shard(connections: NonZeroUsize) -> Self {
        Self {
            inner: PoolSize::PerShard(connections),
        }
    }
}

#[pyclass(name = "WriteCoalescingDelay", from_py_object, frozen)]
#[derive(Clone, Debug)]
pub struct PyWriteCoalescingDelay {
    pub(crate) inner: WriteCoalescingDelay,
}

#[pymethods]
impl PyWriteCoalescingDelay {
    #[staticmethod]
    fn small_nondeterministic() -> Self {
        Self {
            inner: WriteCoalescingDelay::SmallNondeterministic,
        }
    }

    #[staticmethod]
    fn milliseconds(delay: NonZeroU64) -> PyResult<Self> {
        Ok(Self {
            inner: WriteCoalescingDelay::Milliseconds(delay),
        })
    }
}

#[pyclass(name = "SelfIdentity", from_py_object)]
#[derive(Clone, Debug, Default)]
pub struct PySelfIdentity {
    pub(crate) inner: SelfIdentity<'static>,
}

#[pymethods]
impl PySelfIdentity {
    #[new]
    #[pyo3(signature = (
        *,
        custom_driver_name = None,
        custom_driver_version = None,
        application_name = None,
        application_version = None,
        client_id = None,
    ))]
    fn new(
        custom_driver_name: Option<String>,
        custom_driver_version: Option<String>,
        application_name: Option<String>,
        application_version: Option<String>,
        client_id: Option<String>,
    ) -> Self {
        let mut inner = SelfIdentity::new();

        if let Some(v) = custom_driver_name {
            inner.set_custom_driver_name(v);
        }
        if let Some(v) = custom_driver_version {
            inner.set_custom_driver_version(v);
        }
        if let Some(v) = application_name {
            inner.set_application_name(v);
        }
        if let Some(v) = application_version {
            inner.set_application_version(v);
        }
        if let Some(v) = client_id {
            inner.set_client_id(v);
        }

        Self { inner }
    }

    #[getter]
    fn custom_driver_name(&self) -> Option<&str> {
        self.inner.get_custom_driver_name()
    }

    #[setter]
    fn set_custom_driver_name(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.inner.set_custom_driver_name(v);
        }
    }

    #[getter]
    fn custom_driver_version(&self) -> Option<&str> {
        self.inner.get_custom_driver_version()
    }

    #[setter]
    fn set_custom_driver_version(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.inner.set_custom_driver_version(v);
        }
    }

    #[getter]
    fn application_name(&self) -> Option<&str> {
        self.inner.get_application_name()
    }

    #[setter]
    fn set_application_name(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.inner.set_application_name(v);
        }
    }

    #[getter]
    fn application_version(&self) -> Option<&str> {
        self.inner.get_application_version()
    }

    #[setter]
    fn set_application_version(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.inner.set_application_version(v);
        }
    }

    #[getter]
    fn client_id(&self) -> Option<&str> {
        self.inner.get_client_id()
    }

    #[setter]
    fn set_client_id(&mut self, value: Option<String>) {
        if let Some(v) = value {
            self.inner.set_client_id(v);
        }
    }

    fn __repr__(&self, py: Python) -> PyResult<Py<PyString>> {
        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "SelfIdentity(custom_driver_name={:?}, custom_driver_version={:?}, application_name={:?}, application_version={:?}, client_id={:?})",
                self.custom_driver_name(),
                self.custom_driver_version(),
                self.application_name(),
                self.application_version(),
                self.client_id(),
            ),
        )?;

        Ok(repr_str.into())
    }
}

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Consistency>()?;
    module.add_class::<SerialConsistency>()?;
    module.add_class::<PyCompression>()?;
    module.add_class::<PyPoolSize>()?;
    module.add_class::<PyWriteCoalescingDelay>()?;
    module.add_class::<PySelfIdentity>()?;
    Ok(())
}
