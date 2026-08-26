use std::sync::Arc;

use pyo3::prelude::*;
use scylla::client::session::Session;
use scylla_cql::frame::request::query::PagingState;

use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::session::{ExecutableStatement, SessionCore};
use crate::deserialize::results::{PyPagingState, RequestResult, RowFactory};
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError, DriverUseKeyspaceError,
};
use crate::serialize::value_list::PyValueList;
use crate::statement::PyPreparedStatement;

/// Python-facing asynchronous session.
///
/// A thin facade over [`SessionCore`]: every method here converts its Python
/// arguments, hands the work to the core, and awaits the resulting future.
#[pyclass(name = "Session", frozen)]
pub(crate) struct PySession {
    pub(crate) core: SessionCore,
}

impl TryFrom<Arc<Session>> for PySession {
    type Error = PyErr;

    fn try_from(inner: Arc<Session>) -> Result<Self, Self::Error> {
        Ok(Self {
            core: SessionCore::try_from(inner)?,
        })
    }
}

#[pymethods]
impl PySession {
    #[pyo3(signature = (keyspace, case_sensitive=false))]
    async fn use_keyspace(
        &self,
        keyspace: String,
        case_sensitive: bool,
    ) -> Result<(), DriverUseKeyspaceError> {
        self.core
            .clone()
            .use_keyspace(keyspace, case_sensitive)
            .await
    }

    #[pyo3(signature = (statement, values=None, /, *, factory=None, paging_state=None, paged=true))]
    async fn execute(
        &self,
        statement: ExecutableStatement,
        values: Option<PyValueList>,
        factory: Option<Py<RowFactory>>,
        paging_state: Option<Py<PyPagingState>>,
        paged: bool,
    ) -> Result<RequestResult, DriverExecuteError> {
        // Why not accept PyValueList instead of Option<PyValueList>?
        // It would require us to use `Default::default` as default value in
        // `pyo3(signature = ...)`, and thus use `text_signature` as well
        // to keep signature usable for Python users. I think it is cleaner
        // to `unwrap_or_default()` here.
        let values = values.unwrap_or_default();
        let paging_state: Option<PagingState> =
            paging_state.map(|state| Python::attach(|py| state.borrow(py).inner.clone()));

        self.core
            .clone()
            .execute(statement, values, factory, paging_state, paged)
            .await
            .map(RequestResult::from)
    }

    async fn prepare(
        &self,
        statement: ExecutableStatement,
    ) -> Result<PyPreparedStatement, DriverPrepareError> {
        self.core.clone().prepare(statement).await
    }

    #[pyo3(signature = (batch, /, *,  factory=None))]
    async fn batch(
        &self,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResult, DriverExecuteError> {
        self.core
            .clone()
            .batch(batch, factory)
            .await
            .map(RequestResult::from)
    }

    async fn await_schema_agreement(&self) -> Result<uuid::Uuid, DriverSchemaAgreementError> {
        self.core.clone().await_schema_agreement().await
    }

    async fn check_schema_agreement(
        &self,
    ) -> Result<Option<uuid::Uuid>, DriverSchemaAgreementError> {
        self.core.clone().check_schema_agreement().await
    }

    #[getter]
    fn get_cluster_state(&self, py: Python<'_>) -> PyResult<Py<PyClusterState>> {
        self.core.cluster_state(py)
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySession>()?;

    Ok(())
}
