use pyo3::prelude::*;
use scylla::client::PoolSize;
use scylla::statement;
use scylla_cql::frame::Compression;
use std::num::NonZeroUsize;

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

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Consistency>()?;
    module.add_class::<SerialConsistency>()?;
    module.add_class::<PyCompression>()?;
    module.add_class::<PyPoolSize>()?;
    Ok(())
}
