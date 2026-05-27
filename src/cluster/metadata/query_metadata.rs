use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyString, PyTuple};
use scylla::frame::response::result::ColumnSpec;

use crate::cluster::metadata::column_type::{PyCqlColumnType, extract_column_type};
use crate::errors::DriverQueryMetadataError;

/// Specification of a column in a result set, used for both prepared statement metadata and query result metadata.
#[pyclass(name = "ColumnSpec", skip_from_py_object, frozen)]
pub(crate) struct PyColumnSpec {
    inner: ColumnSpec<'static>,

    // Cached Python-side representations used by the getters.
    py_name: PyOnceLock<Py<PyString>>,
    py_table_name: PyOnceLock<Py<PyString>>,
    py_keyspace_name: PyOnceLock<Py<PyString>>,
    py_cql_type: PyOnceLock<Py<PyCqlColumnType>>,
}

impl From<&ColumnSpec<'_>> for PyColumnSpec {
    fn from(spec: &ColumnSpec<'_>) -> Self {
        Self {
            inner: spec.clone().into_owned(),

            py_name: PyOnceLock::new(),
            py_table_name: PyOnceLock::new(),
            py_keyspace_name: PyOnceLock::new(),
            py_cql_type: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl PyColumnSpec {
    /// The name of the column.
    #[getter]
    fn name(&self, py: Python<'_>) -> Py<PyString> {
        let name = self
            .py_name
            .get_or_init(py, || PyString::new(py, self.inner.name()).unbind());
        name.clone_ref(py)
    }

    /// The name of the table containing the column.
    #[getter]
    fn table_name(&self, py: Python<'_>) -> Py<PyString> {
        let table_name = self.py_table_name.get_or_init(py, || {
            PyString::new(py, self.inner.table_spec().table_name()).unbind()
        });
        table_name.clone_ref(py)
    }

    /// The name of the keyspace containing the column.
    #[getter]
    fn keyspace_name(&self, py: Python<'_>) -> Py<PyString> {
        let keyspace_name = self.py_keyspace_name.get_or_init(py, || {
            PyString::new(py, self.inner.table_spec().ks_name()).unbind()
        });
        keyspace_name.clone_ref(py)
    }

    /// The CQL type of the column.
    #[getter]
    fn cql_type(&self, py: Python<'_>) -> PyResult<Py<PyCqlColumnType>> {
        let cql_type = self.py_cql_type.get_or_try_init(py, || {
            extract_column_type(py, self.inner.typ())
                .map_err(DriverQueryMetadataError::column_type_extraction_failed)
        })?;
        Ok(cql_type.clone_ref(py))
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let cql_type = self.cql_type(py)?;
        let cql_type_name = cql_type.bind(py).get_type().name()?;

        PyString::from_fmt(
            py,
            format_args!(
                "ColumnSpec(keyspace_name='{}', table_name='{}', name='{}', cql_type={})",
                self.inner.table_spec().ks_name(),
                self.inner.table_spec().table_name(),
                self.inner.name(),
                cql_type_name,
            ),
        )
    }
}

/// Builds a Python tuple of lazily-initialized `ColumnSpec`s from column specifications.
pub(crate) fn column_spec_tuple(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> PyResult<Py<PyTuple>> {
    PyTuple::new(py, specs.iter().map(PyColumnSpec::from)).map(Bound::unbind)
}
