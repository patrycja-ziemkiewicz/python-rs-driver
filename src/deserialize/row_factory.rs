use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Borrowed, PyClass, intern};
use scylla::frame::response::result::ColumnSpec;

use crate::cluster::metadata::query_metadata::{PyColumnSpec, column_spec_tuple};
use crate::deserialize::error::{DriverRowFactoryError, DriverRowIterationError};
use crate::utils::PyValueOrError;

/// Base class of all row factories. A subclass overrides `prepare`, which
/// gets the columns of a page and returns the callable that builds each row.
#[pyclass(module = "scylla.results", name = "RowFactory", subclass, frozen)]
pub(crate) struct PyRowFactoryBase;

#[pymethods]
impl PyRowFactoryBase {
    // Takes any arguments, so a subclass `__init__` can have its own.
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        Self
    }

    fn prepare(&self, _columns: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        Err(PyNotImplementedError::new_err(
            "a RowFactory subclass must override 'prepare'",
        ))
    }
}

fn builtin<T: PyClass<BaseType = PyRowFactoryBase>>(factory: T) -> PyClassInitializer<T> {
    PyClassInitializer::from(PyRowFactoryBase).add_subclass(factory)
}

/// Builds every row as a `dict` mapping column names to values, in column
/// order. This is the default.
#[pyclass(module = "scylla.results", name = "DictRowFactory", extends = PyRowFactoryBase, frozen)]
pub(crate) struct PyDictRowFactory;

#[pymethods]
impl PyDictRowFactory {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        builtin(Self)
    }

    fn prepare(
        &self,
        py: Python<'_>,
        columns: &Bound<'_, PyTuple>,
    ) -> PyResult<PyBuiltinRowBuilder> {
        let names = py_column_names(py, columns)?;
        let column_count = names.len();

        Ok(PyBuiltinRowBuilder::new(
            RowBuilder::Dict(names),
            column_count,
        ))
    }
}

/// Builds every row as a plain `tuple` of values, in column order.
#[pyclass(module = "scylla.results", name = "TupleRowFactory", extends = PyRowFactoryBase, frozen)]
pub(crate) struct PyTupleRowFactory;

#[pymethods]
impl PyTupleRowFactory {
    #[new]
    fn new() -> PyClassInitializer<Self> {
        builtin(Self)
    }

    fn prepare(&self, columns: &Bound<'_, PyTuple>) -> PyBuiltinRowBuilder {
        PyBuiltinRowBuilder::new(RowBuilder::Tuple, columns.len())
    }
}

/// The builder a built-in factory's `prepare` returns, called with a tuple of
/// column values.
#[pyclass(name = "BuiltinRowBuilder", frozen)]
pub(crate) struct PyBuiltinRowBuilder {
    builder: RowBuilder,
    column_count: usize,
}

impl PyBuiltinRowBuilder {
    fn new(builder: RowBuilder, column_count: usize) -> Self {
        Self {
            builder,
            column_count,
        }
    }
}

#[pymethods]
impl PyBuiltinRowBuilder {
    fn __call__(&self, py: Python<'_>, values: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
        if values.len() != self.column_count {
            return Err(PyValueError::new_err(format!(
                "expected {} column values, got {}",
                self.column_count,
                values.len()
            )));
        }

        // The values are already Python objects, so only Python errors can occur.
        self.builder
            .build(py, values.iter().map(Ok))
            .map_err(|err| match err {
                DriverRowIterationError::PythonError(err) => err,
                err => err.into(),
            })
    }
}

/// Reuses the name strings each `ColumnSpec` caches.
fn py_column_names(py: Python<'_>, columns: &Bound<'_, PyTuple>) -> PyResult<Vec<Py<PyString>>> {
    columns
        .iter_borrowed()
        .map(|column| Ok(column.cast::<PyColumnSpec>()?.get().name(py)))
        .collect()
}

/// A row factory as handed over from Python, classified but not yet resolved:
/// resolving needs the column metadata, which only arrives with the response.
#[derive(Clone)]
pub(crate) enum PyRowFactory {
    Dict,
    Tuple,
    /// A user `RowFactory` subclass, whose `prepare` is called once the
    /// metadata is known.
    Deferred(Py<PyAny>),
    /// A callable used directly as the row builder.
    Builder(Py<PyAny>),
}

impl<'py> FromPyObject<'_, 'py> for PyRowFactory {
    type Error = DriverRowFactoryError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.cast::<PyDictRowFactory>().is_ok() {
            return Ok(Self::Dict);
        }

        if obj.cast::<PyTupleRowFactory>().is_ok() {
            return Ok(Self::Tuple);
        }

        if obj.cast::<PyRowFactoryBase>().is_ok() {
            return Ok(Self::Deferred(obj.to_owned().unbind()));
        }

        if obj.is_callable() {
            return Ok(Self::Builder(obj.to_owned().unbind()));
        }

        Err(DriverRowFactoryError::invalid_factory(obj))
    }
}

/// A row factory resolved against the column metadata of one page.
///
/// Pages of one request can differ in columns, for example after a schema
/// change, so each page gets its own builder.
#[derive(Clone)]
pub(crate) enum RowBuilder {
    Dict(Vec<Py<PyString>>),
    Tuple,
    Custom(Py<PyAny>),
}

impl RowBuilder {
    pub(crate) fn resolve(
        py: Python<'_>,
        factory: &PyRowFactory,
        specs: &[ColumnSpec<'_>],
    ) -> PyResult<Self> {
        Ok(match factory {
            PyRowFactory::Dict => Self::Dict(column_names(py, specs)),
            PyRowFactory::Tuple => Self::Tuple,
            PyRowFactory::Deferred(deferred) => {
                let columns = column_spec_tuple(py, specs)?;
                let builder = deferred
                    .bind(py)
                    .call_method1(intern!(py, "prepare"), (columns,))?;

                if !builder.is_callable() {
                    return Err(
                        DriverRowFactoryError::uncallable_builder(builder.as_borrowed()).into(),
                    );
                }

                Self::Custom(builder.unbind())
            }
            PyRowFactory::Builder(build) => Self::Custom(build.clone_ref(py)),
        })
    }

    /// Builds one Python row out of the values of its columns, in column order.
    pub(crate) fn build<'py, V: IntoPyObject<'py>>(
        &self,
        py: Python<'py>,
        values: impl ExactSizeIterator<Item = Result<V, DriverRowIterationError>>,
    ) -> Result<Py<PyAny>, DriverRowIterationError> {
        let row = match self {
            // {name: v, ...}
            Self::Dict(names) => named_values(py, names, values)?.into_any(),
            // (v, ...)
            Self::Tuple => row_values(py, values)?.into_any(),
            // build((v, ...))
            Self::Custom(build) => {
                let args = row_values(py, values)?;
                build.bind(py).call1((args,))?
            }
        };

        Ok(row.unbind())
    }
}

fn named_values<'py, V: IntoPyObject<'py>>(
    py: Python<'py>,
    names: &[Py<PyString>],
    values: impl Iterator<Item = Result<V, DriverRowIterationError>>,
) -> Result<Bound<'py, PyDict>, DriverRowIterationError> {
    let row = PyDict::new(py);

    for (name, value) in names.iter().zip(values) {
        row.set_item(name, value?)?;
    }

    Ok(row)
}

fn row_values<'py, V: IntoPyObject<'py>>(
    py: Python<'py>,
    values: impl ExactSizeIterator<Item = Result<V, DriverRowIterationError>>,
) -> Result<Bound<'py, PyTuple>, DriverRowIterationError> {
    Ok(PyTuple::new(py, values.map(PyValueOrError::new))?)
}

fn column_names(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> Vec<Py<PyString>> {
    specs
        .iter()
        .map(|spec| PyString::new(py, spec.name()).unbind())
        .collect()
}
