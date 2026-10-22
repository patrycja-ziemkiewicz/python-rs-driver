use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use scylla_cql::value::CqlDuration;

#[pyclass(module = "scylla.cql_types", frozen)]
pub struct CqlEmpty;

#[pymethods]
impl CqlEmpty {
    fn __repr__(&self) -> &'static str {
        "CqlEmpty"
    }
}

#[pyclass(
    module = "scylla.cql_types",
    name = "CqlDuration",
    frozen,
    eq,
    hash,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyCqlDuration {
    #[pyo3(get)]
    months: i32,
    #[pyo3(get)]
    days: i32,
    #[pyo3(get)]
    nanoseconds: i64,
}

/// CQL rejects a duration whose fields have different signs.
pub(crate) fn has_uniform_sign(months: i32, days: i32, nanoseconds: i64) -> bool {
    let non_negative = months >= 0 && days >= 0 && nanoseconds >= 0;
    let non_positive = months <= 0 && days <= 0 && nanoseconds <= 0;
    non_negative || non_positive
}

#[pymethods]
impl PyCqlDuration {
    #[new]
    #[pyo3(signature = (months = 0, days = 0, nanoseconds = 0))]
    fn new(months: i32, days: i32, nanoseconds: i64) -> PyResult<Self> {
        if !has_uniform_sign(months, days, nanoseconds) {
            return Err(PyValueError::new_err(
                "All CqlDuration fields must have the same sign.",
            ));
        }

        Ok(Self {
            months,
            days,
            nanoseconds,
        })
    }

    fn __getnewargs__(&self) -> (i32, i32, i64) {
        (self.months, self.days, self.nanoseconds)
    }

    fn __repr__(&self) -> String {
        format!(
            "CqlDuration(months={}, days={}, nanoseconds={})",
            self.months, self.days, self.nanoseconds
        )
    }
}

impl From<CqlDuration> for PyCqlDuration {
    fn from(d: CqlDuration) -> Self {
        Self {
            months: d.months,
            days: d.days,
            nanoseconds: d.nanoseconds,
        }
    }
}

impl From<PyCqlDuration> for CqlDuration {
    fn from(d: PyCqlDuration) -> Self {
        Self {
            months: d.months,
            days: d.days,
            nanoseconds: d.nanoseconds,
        }
    }
}

#[pymodule]
pub(crate) fn cql_types(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CqlEmpty>()?;
    module.add_class::<PyCqlDuration>()?;
    Ok(())
}
