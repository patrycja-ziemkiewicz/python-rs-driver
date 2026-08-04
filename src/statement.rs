use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::execution_profile::PyExecutionProfile;
use crate::policies::retry::policies::PyRetryPolicy;
use crate::types::UnsetType;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyString};
use scylla::statement::SerialConsistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use std::time::Duration;

use crate::policies::load_balancing::PyLoadBalancingPolicy;
use crate::utils::WithOriginalPyObject;

#[pyclass(name = "PreparedStatement", frozen)]
pub(crate) struct PyPreparedStatement {
    pub(crate) inner: PreparedStatement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) execution_profile: Option<Py<PyExecutionProfile>>,
    pub(crate) load_balancing_policy: Option<Py<PyAny>>,
    pub(crate) retry_policy: Option<Py<PyAny>>,
}

impl PyPreparedStatement {
    pub(crate) fn new(
        inner: PreparedStatement,
        is_serial_consistency_set: bool,
        execution_profile: Option<Py<PyExecutionProfile>>,
        load_balancing_policy: Option<Py<PyAny>>,
        retry_policy: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            inner,
            is_serial_consistency_set,
            execution_profile,
            load_balancing_policy,
            retry_policy,
        }
    }
}

#[pymethods]
impl PyPreparedStatement {
    fn with_execution_profile(&self, profile: Py<PyExecutionProfile>) -> Self {
        let mut p = self.inner.clone();
        p.set_execution_profile_handle(Some(profile.get().inner.clone().into_handle()));
        Self::new(
            p,
            self.is_serial_consistency_set,
            Some(profile),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_execution_profile_handle(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            None,
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<Py<PyExecutionProfile>> {
        self.execution_profile.clone()
    }

    fn with_load_balancing_policy(
        &self,
        py_policy: WithOriginalPyObject<PyLoadBalancingPolicy>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut p = self.inner.clone();
        p.set_load_balancing_policy(Some(py_policy.extracted.into_inner()));
        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            Some(py_policy.original),
            self.retry_policy.clone(),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_load_balancing_policy(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            None,
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self.load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut p = self.inner.clone();
        p.set_consistency(c.into());
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    fn without_consistency(&self) -> Self {
        let mut p = self.inner.clone();
        p.unset_consistency();
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self.inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut p = self.inner.clone();
        p.set_serial_consistency(sc.map(SerialConsistency::from));

        Self::new(
            p,
            true,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    fn without_serial_consistency(&self) -> Self {
        let mut p = self.inner.clone();
        p.unset_serial_consistency();

        Self::new(
            p,
            false,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverStatementConfigError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed);
        }
        match self.inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Self, DriverStatementConfigError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?,
        };

        let mut p = self.inner.clone();

        p.set_request_timeout(Some(timeout));

        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_request_timeout(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self.inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Self {
        let mut p = self.inner.clone();
        p.set_page_size(page_size);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_page_size(&self) -> i32 {
        self.inner.get_page_size()
    }

    fn with_retry_policy(
        &self,
        py_policy: WithOriginalPyObject<PyRetryPolicy>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut p = self.inner.clone();
        p.set_retry_policy(Some(py_policy.extracted.into_inner()));

        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            Some(py_policy.original),
        ))
    }

    fn without_retry_policy(&self) -> Self {
        let mut p = self.inner.clone();
        p.set_retry_policy(None);

        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            None,
        )
    }

    #[getter]
    fn get_retry_policy(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.retry_policy.as_ref().map(|rp| rp.clone_ref(py))
    }

    fn set_is_idempotent(&self, is_idempotent: bool) -> Self {
        let mut p = self.inner.clone();
        p.set_is_idempotent(is_idempotent);

        Self::new(
            p,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_is_idempotent(&self) -> bool {
        self.inner.get_is_idempotent()
    }
}

#[derive(Clone)]
#[pyclass(name = "Statement", frozen, skip_from_py_object)]
pub(crate) struct PyStatement {
    pub(crate) inner: Statement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) execution_profile: Option<Py<PyExecutionProfile>>,
    pub(crate) load_balancing_policy: Option<Py<PyAny>>,
    pub(crate) retry_policy: Option<Py<PyAny>>,
}

impl PyStatement {
    pub(crate) fn new(
        inner: Statement,
        is_serial_consistency_set: bool,

        execution_profile: Option<Py<PyExecutionProfile>>,
        load_balancing_policy: Option<Py<PyAny>>,
        retry_policy: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            inner,
            is_serial_consistency_set,
            execution_profile,
            load_balancing_policy,
            retry_policy,
        }
    }
}

#[pymethods]
impl PyStatement {
    #[new]
    fn py_new(query_str: String) -> Self {
        let s = Statement::from(query_str);
        Self::new(s, false, None, None, None)
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self.inner.contents)
    }

    fn with_execution_profile(&self, profile: Py<PyExecutionProfile>) -> Self {
        let mut s = self.inner.clone();
        s.set_execution_profile_handle(Some(profile.get().inner.clone().into_handle()));
        Self::new(
            s,
            self.is_serial_consistency_set,
            Some(profile),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_execution_profile_handle(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            None,
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<Py<PyExecutionProfile>> {
        self.execution_profile.clone()
    }

    fn with_load_balancing_policy(
        &self,
        py_policy: WithOriginalPyObject<PyLoadBalancingPolicy>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut s = self.inner.clone();
        s.set_load_balancing_policy(Some(py_policy.extracted.into_inner()));
        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            Some(py_policy.original),
            self.retry_policy.clone(),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_load_balancing_policy(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            None,
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self.load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut s = self.inner.clone();
        s.set_consistency(c.into());
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    fn without_consistency(&self) -> Self {
        let mut s = self.inner.clone();
        s.unset_consistency();
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self.inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut s = self.inner.clone();
        s.set_serial_consistency(sc.map(SerialConsistency::from));
        Self::new(
            s,
            true,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    fn without_serial_consistency(&self) -> Self {
        let mut s = self.inner.clone();
        s.unset_serial_consistency();
        Self::new(
            s,
            false,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverStatementConfigError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed);
        }
        match self.inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Self, DriverStatementConfigError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?,
        };

        let mut s = self.inner.clone();
        s.set_request_timeout(Some(timeout));
        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_request_timeout(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self.inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Self {
        let mut s = self.inner.clone();
        s.set_page_size(page_size);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_page_size(&self) -> i32 {
        self.inner.get_page_size()
    }

    fn with_retry_policy(
        &self,
        py_policy: WithOriginalPyObject<PyRetryPolicy>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut s = self.inner.clone();
        s.set_retry_policy(Some(py_policy.extracted.into_inner()));

        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            Some(py_policy.original),
        ))
    }

    fn without_retry_policy(&self) -> Self {
        let mut s = self.inner.clone();
        s.set_retry_policy(None);

        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            None,
        )
    }

    #[getter]
    fn get_retry_policy(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.retry_policy.as_ref().map(|rp| rp.clone_ref(py))
    }

    fn set_is_idempotent(&self, is_idempotent: bool) -> Self {
        let mut s = self.inner.clone();
        s.set_is_idempotent(is_idempotent);

        Self::new(
            s,
            self.is_serial_consistency_set,
            self.execution_profile.clone(),
            self.load_balancing_policy.clone(),
            self.retry_policy.clone(),
        )
    }

    #[getter]
    fn get_is_idempotent(&self) -> bool {
        self.inner.get_is_idempotent()
    }
}

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreparedStatement>()?;
    module.add_class::<PyStatement>()?;
    Ok(())
}
