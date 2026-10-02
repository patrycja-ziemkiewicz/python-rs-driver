use pyo3::Borrowed;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::deserialize::error::{DriverRowFactoryError, DriverRowIterationError};
use crate::deserialize::results::RowColumnCursor;

/// Factory responsible for constructing Python row objects.
///
/// `RowFactory` defines how a row is materialized from a column iterator.
/// The default implementation consumes all columns of the current row and
/// returns a Python dictionary mapping column names to values.
///
/// Users may subclass this type to implement custom row mappings.
#[pyclass(module = "scylla.results", subclass, frozen)]
pub struct RowFactory {}

#[pymethods]
impl RowFactory {
    /// Create a new `RowFactory`.
    ///
    /// The default row factory builds each row as a Python `dict`
    /// mapping column names to deserialized Python values.
    ///
    /// The constructor accepts arbitrary positional and keyword arguments.
    /// This allows Python subclasses to define their own `__init__`
    /// signatures and store custom configuration or state.
    #[expect(unused_variables)]
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    pub fn new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        RowFactory {}
    }

    /// Build a Python object representing a single row.
    ///
    /// This method consumes all columns from the provided column iterator
    /// and returns a Python `dict` mapping column names to values.
    ///
    /// Parameters
    /// ----------
    /// column_iterator : RowColumnCursor
    ///     Iterator over columns of the current row.
    ///
    /// Returns
    /// -------
    /// dict
    ///     A dictionary mapping column names (`str`) to deserialized
    ///     Python values.
    ///
    /// Raises
    /// ------
    /// DeserializationError
    ///     If any column cannot be deserialized into a Python object.
    /// RowIterationError
    ///     If building the Python row object fails (with original error attached).
    pub fn build<'py>(
        &self,
        py: Python<'py>,
        column_iterator: &Bound<'py, RowColumnCursor>,
    ) -> Result<Py<PyDict>, DriverRowIterationError> {
        let mut columns = column_iterator.borrow_mut();

        let dict = PyDict::new(py);
        while let Some(next) = columns.next_column(py) {
            let column = next.map_err(DriverRowIterationError::Deserialization)?;
            dict.set_item(column.column_name, column.value)
                .map_err(DriverRowIterationError::PythonError)?;
        }

        Ok(dict.into())
    }
}

impl RowFactory {
    pub(crate) fn default_instance() -> &'static Self {
        static DEFAULT_FACTORY: RowFactory = RowFactory {};
        &DEFAULT_FACTORY
    }
}

/// A row factory as handed over from Python, classified but not yet resolved:
/// resolving needs the column metadata, which only arrives with the response.
#[derive(Clone)]
pub(crate) enum PyRowFactory {
    /// A `RowFactory` instance, whose `prepare` is called once the metadata
    /// is known.
    Deferred(Py<PyAny>),
    /// A callable used directly as the row builder.
    Builder(Py<PyAny>),
}

impl<'py> FromPyObject<'_, 'py> for PyRowFactory {
    type Error = DriverRowFactoryError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.cast::<RowFactory>().is_ok() {
            return Ok(Self::Deferred(obj.to_owned().unbind()));
        }

        if obj.is_callable() {
            return Ok(Self::Builder(obj.to_owned().unbind()));
        }

        Err(DriverRowFactoryError::invalid_factory(obj))
    }
}
