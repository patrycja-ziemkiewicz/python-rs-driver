use std::fmt::Write;
use std::sync::Arc;

use crate::RUNTIME;
use crate::row::CqlRow;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::response::query_result::QueryRowsResult;
use scylla::value::Row;
use scylla_cql::deserialize::DeserializationError;
use scylla_cql::deserialize::result::TypedRowIterator;
use scylla_cql::frame::response::result::DeserializedMetadataAndRawRows;
use stable_deref_trait::StableDeref;
use std::ops::Deref;
use yoke::{Yoke, Yokeable};

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    async fn execute(&self, request: Py<PyString>) -> PyResult<RequestResult> {
        let request_string = Python::with_gil(|py| request.to_str(py))?.to_string();
        let session_clone = Arc::clone(&self._inner);

        let result = RUNTIME
            .spawn(async move {
                session_clone
                    .query_unpaged(request_string, &[])
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
                    })
            })
            .await
            .expect("Driver should not panic")?;
        Ok(RequestResult { inner: result })
    }
}

#[pyclass]
pub(crate) struct RequestResult {
    pub(crate) inner: scylla::response::query_result::QueryResult,
}

#[pymethods]
impl RequestResult {
    fn __str__<'gil>(&mut self, py: Python<'gil>) -> PyResult<Bound<'gil, PyString>> {
        let mut result = String::new();
        let rows_result = match self.inner.clone().into_rows_result() {
            Ok(r) => r,
            Err(e) => return Ok(PyString::new(py, &format!("non-rows result: {}", e))),
        };
        for r in rows_result.rows::<Row>().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
        })? {
            let row = match r {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Failed to deserialize row: {}",
                        e
                    )));
                }
            };
            write!(result, "|").unwrap();
            for col in row.columns {
                match col {
                    Some(c) => write!(result, "{}", c).unwrap(),
                    None => write!(result, "null").unwrap(),
                };
                write!(result, "|").unwrap();
            }
            writeln!(result).unwrap();
        }
        Ok(PyString::new(py, &result))
    }

    #[pyo3(signature = (factory=None))]
    fn create_rows_result(&self, factory: Option<PyObject>) -> PyResult<RowsResult> {
        if let Some(ref f) = factory {
            Python::with_gil(|py| {
                let row_factory_type = py.get_type::<RowFactory>();

                let f_ref = f.bind(py);

                if !f_ref.is_instance(&row_factory_type)? {
                    return Err(PyTypeError::new_err(
                        "row_factory must be a subclass of RowFactory",
                    ));
                }

                Ok(())
            })?;
        }

        let row_iterator = RowsIterator::new(
            self.inner
                .clone()
                .into_rows_result()
                .map_err(|e| PyTypeError::new_err(e.to_string()))?,
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(RowsResult {
            row_iterator,
            factory,
        })
    }
}

#[derive(Yokeable)]
struct TypedRowIteratorWrapper<'a> {
    iterator: TypedRowIterator<'a, 'a, CqlRow>,
}
struct RawMetadataCart(Box<DeserializedMetadataAndRawRows>);

impl Deref for RawMetadataCart {
    type Target = DeserializedMetadataAndRawRows;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl StableDeref for RawMetadataCart {}

#[pyclass]
pub struct RowsIterator {
    yoked: Yoke<TypedRowIteratorWrapper<'static>, RawMetadataCart>,
}

impl RowsIterator {
    pub fn new(result: QueryRowsResult) -> Result<Self, DeserializationError> {
        let (data, _, _, _) = result.into_inner();

        let cart = RawMetadataCart(Box::new(data));

        let yoked = Yoke::try_attach_to_cart(cart, |cart| -> Result<_, DeserializationError> {
            let iterator = cart.rows_iter().map_err(DeserializationError::new)?;

            Ok(TypedRowIteratorWrapper { iterator })
        })?;

        Ok(Self { yoked })
    }
}
#[pyclass]
pub struct RowsResult {
    row_iterator: RowsIterator,
    factory: Option<PyObject>,
}

#[pymethods]
impl RowsResult {
    pub fn __next__(&mut self) -> PyResult<PyObject> {
        let row = self
            .row_iterator
            .yoked
            .with_mut_return(|view| view.iterator.next())
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyStopIteration, _>(""))?
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Python::with_gil(|py| match &self.factory {
            Some(f) => f.call_method1(py, "build", (row,)),
            None => Ok(row.columns.into()),
        })
    }

    pub fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }
}

#[pyclass(subclass)]
pub struct RowFactory {}
#[pymethods]
impl RowFactory {
    #[new]
    fn new() -> Self {
        RowFactory {}
    }
    pub fn build<'py>(&self, py: Python<'py>, column: PyRef<CqlRow>) -> PyResult<Py<PyAny>> {
        Ok(column.columns.clone_ref(py).into())
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<RowsResult>()?;
    module.add_class::<RowFactory>()?;

    Ok(())
}
