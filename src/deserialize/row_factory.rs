use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Borrowed, intern};
use scylla::frame::response::result::ColumnSpec;

use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::deserialize::error::{DriverRowFactoryError, DriverRowIterationError};
use crate::deserialize::results::ColumnDeserializer;
use crate::utils::PyValueOrError;

/// Returns every row as a `dict` mapping column names to values. This is
/// the default.
#[pyclass(name = "DictRowFactory", frozen)]
pub(crate) struct PyDictRowFactory {}

#[pymethods]
impl PyDictRowFactory {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// A row factory as handed over from Python, classified but not yet resolved:
/// resolving needs the column metadata, which only arrives with the response.
#[derive(Clone)]
pub(crate) enum PyRowFactory {
    Dict,
    /// An object with a `prepare` method, called once the metadata is known.
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

        match obj
            .getattr_opt(intern!(obj.py(), "prepare"))
            .map_err(|err| DriverRowFactoryError::prepare_lookup_failed(obj, err))?
        {
            Some(prepare) if prepare.is_callable() => {
                return Ok(Self::Deferred(obj.to_owned().unbind()));
            }
            Some(_) => return Err(DriverRowFactoryError::invalid_factory(obj)),
            None => {}
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

    /// Builds one Python row out of the deserialized columns.
    pub(crate) fn build(
        &self,
        values: ColumnDeserializer<'_, '_>,
    ) -> Result<Py<PyAny>, DriverRowIterationError> {
        let py = values.py();

        let row = match self {
            // {name: v, ...}
            Self::Dict(names) => named_values(names, values)?.into_any(),
            // build((v, ...))
            Self::Custom(build) => {
                let args = row_values(values)?;
                build.bind(py).call1((args,))?
            }
        };

        Ok(row.unbind())
    }
}

fn named_values<'py>(
    names: &[Py<PyString>],
    values: ColumnDeserializer<'_, 'py>,
) -> Result<Bound<'py, PyDict>, DriverRowIterationError> {
    let row = PyDict::new(values.py());

    for (name, value) in names.iter().zip(values) {
        row.set_item(name, value?)?;
    }

    Ok(row)
}

fn row_values<'py>(
    values: ColumnDeserializer<'_, 'py>,
) -> Result<Bound<'py, PyTuple>, DriverRowIterationError> {
    let py = values.py();

    Ok(PyTuple::new(py, values.map(PyValueOrError::new))?)
}

fn column_names(py: Python<'_>, specs: &[ColumnSpec<'_>]) -> Vec<Py<PyString>> {
    specs
        .iter()
        .map(|spec| PyString::new(py, spec.name()).unbind())
        .collect()
}
