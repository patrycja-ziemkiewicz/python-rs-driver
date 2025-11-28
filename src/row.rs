use crate::value::{PyDeserializeValue, PyDeserializedValue};
use pyo3::prelude::{PyDictMethods, PyModule, PyModuleMethods};
use pyo3::types::PyDict;
use pyo3::{Bound, Py, PyResult, Python, pyclass, pymodule};
use scylla::_macro_internal::{
    ColumnIterator, ColumnSpec, DeserializationError, DeserializeRow, TypeCheckError,
};

#[pyclass]
pub struct CqlRow {
    #[pyo3(get)]
    pub columns: Py<PyDict>,
}

impl DeserializeRow<'_, '_> for CqlRow {
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }

    fn deserialize(row: ColumnIterator) -> Result<Self, DeserializationError> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            for col in row {
                let raw_col = col?;

                let val =
                    PyDeserializedValue::deserialize_py(raw_col.spec.typ(), raw_col.slice, py)?;

                dict.set_item(raw_col.spec.name(), val)
                    .map_err(DeserializationError::new)?;
            }

            Ok(CqlRow {
                columns: dict.unbind(),
            })
        })
    }
}

#[pymodule]
pub(crate) fn row(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CqlRow>()?;
    Ok(())
}
