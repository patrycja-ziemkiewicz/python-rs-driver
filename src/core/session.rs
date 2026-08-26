use std::future::Future;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyString;
use scylla::client::session::Session;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::BatchStatement;
use scylla::statement::prepared::PreparedStatement;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::cluster::state::PyClusterState;
use crate::core::results::{Pager, RequestResultCore};
use crate::deserialize::results::RowFactory;
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError,
    DriverStatementConversionError, DriverUseKeyspaceError,
};
use crate::serialize::value_list::PyValueList;
use crate::statement::{PyPreparedStatement, PyStatement};

/// Helper performing the core logic of executing queries.
#[derive(Clone)]
pub(crate) struct SessionCore {
    pub(crate) inner: Arc<Session>,
    /// Cached Python snapshot of the cluster state. Shared by every facade
    /// wrapping this core, so one underlying session has exactly one cache.
    cluster_state: Arc<Mutex<Py<PyClusterState>>>,
}

impl TryFrom<Arc<Session>> for SessionCore {
    type Error = PyErr;

    fn try_from(inner: Arc<Session>) -> Result<Self, Self::Error> {
        let cluster_state =
            Python::attach(|py| Py::new(py, PyClusterState::try_from(inner.get_cluster_state())?))?;
        Ok(Self {
            cluster_state: Arc::new(Mutex::new(cluster_state)),
            inner,
        })
    }
}

impl SessionCore {
    pub(crate) async fn use_keyspace(
        self,
        keyspace: String,
        case_sensitive: bool,
    ) -> Result<(), DriverUseKeyspaceError> {
        self.spawn_on_runtime(async move |s| {
            s.use_keyspace(keyspace, case_sensitive)
                .await
                .map_err(DriverUseKeyspaceError::from)
        })
        .await
    }

    pub(crate) async fn execute(
        self,
        statement: ExecutableStatement,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
        paging_state: Option<PagingState>,
        paged: bool,
    ) -> Result<RequestResultCore, DriverExecuteError> {
        if paged {
            self.execute_paged(statement, paging_state, values, factory)
                .await
        } else {
            if paging_state.is_some() {
                return Err(DriverExecuteError::paging_state_must_be_none_for_unpaged_execution());
            }

            self.execute_unpaged(statement, values, factory).await
        }
    }

    pub(crate) async fn prepare(
        self,
        statement: ExecutableStatement,
    ) -> Result<PyPreparedStatement, DriverPrepareError> {
        match statement {
            ExecutableStatement::Unprepared(py_statement) => {
                match self.inner.prepare(py_statement.inner).await {
                    Ok(prepared) => {
                        let is_serial_consistency_set = prepared.get_serial_consistency().is_some();
                        Ok(PyPreparedStatement::new(
                            prepared,
                            is_serial_consistency_set,
                            py_statement.execution_profile,
                            py_statement.load_balancing_policy,
                            py_statement.retry_policy,
                        ))
                    }
                    Err(err) => Err(DriverPrepareError::rust_driver_prepare_error(err)),
                }
            }
            ExecutableStatement::Prepared(_) => {
                Err(DriverPrepareError::cannot_prepare_prepared_statement())
            }
        }
    }

    pub(crate) async fn batch(
        self,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResultCore, DriverExecuteError> {
        let result = self
            .spawn_on_runtime(async move |s| {
                s.batch(&batch.inner, batch.values)
                    .await
                    .map_err(DriverExecuteError::rust_driver_execution_error)
            })
            .await?;

        Ok(RequestResultCore::new(result, Pager::unpaged(), factory))
    }

    pub(crate) async fn await_schema_agreement(
        self,
    ) -> Result<uuid::Uuid, DriverSchemaAgreementError> {
        self.spawn_on_runtime(async move |s| {
            s.await_schema_agreement()
                .await
                .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
        })
        .await
    }

    pub(crate) async fn check_schema_agreement(
        self,
    ) -> Result<Option<uuid::Uuid>, DriverSchemaAgreementError> {
        self.spawn_on_runtime(async move |s| {
            s.check_schema_agreement()
                .await
                .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
        })
        .await
    }

    /// Returns the cached Python cluster state snapshot, refreshing it first if
    /// the Rust driver has since replaced its own.
    pub(crate) fn cluster_state(&self, py: Python<'_>) -> PyResult<Py<PyClusterState>> {
        // PyClusterState holds `Arc<ClusterState>` preventing Rust driver from replacing
        // inner Rust `Session`'s `ClusterState` with a new object in the same memory.
        //
        // This means by comparing current Rust `Session` `ClusterState` pointer
        // and `PyClusterState`'s internal `ClusterState` pointer
        // we can determine if the `PyClusterState`'s snapshot is stale
        // and needs to be replaced with a fresh snapshot.
        let mut py_cluster_state = self.cluster_state.lock_py_attached(py).unwrap();
        let rust_current_cluster_state = self.inner.get_cluster_state();
        let python_snapshot_cluster_state = &py_cluster_state.get().inner;
        if !Arc::ptr_eq(&rust_current_cluster_state, python_snapshot_cluster_state) {
            *py_cluster_state = Py::new(
                py,
                PyClusterState::try_from(self.inner.get_cluster_state())?,
            )?;
        }

        Ok(py_cluster_state.clone_ref(py))
    }

    async fn execute_unpaged(
        self,
        statement: ExecutableStatement,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResultCore, DriverExecuteError> {
        let result = match statement {
            ExecutableStatement::Prepared(p) => {
                let serialized_values = p
                    .serialize_values_unstable(&values)
                    .map_err(DriverExecuteError::serialization_failed)?;
                self.spawn_on_runtime(async move |s| {
                    s.execute_unstable(&p, &serialized_values, false, PagingState::start())
                        .await
                        .map(|(result, _paging_response)| result)
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await?
            }
            ExecutableStatement::Unprepared(q) => {
                self.spawn_on_runtime(async move |s| {
                    s.query_unpaged(q.inner, values)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await?
            }
        };

        Ok(RequestResultCore::new(result, Pager::unpaged(), factory))
    }

    async fn execute_paged(
        self,
        statement: ExecutableStatement,
        paging_state: Option<PagingState>,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResultCore, DriverExecuteError> {
        let paging_state = paging_state.unwrap_or_else(PagingState::start);

        let (result, paging_response) = self
            .execute_single_page(paging_state, statement.clone(), values.clone())
            .await?;

        Ok(RequestResultCore::new(
            result,
            Pager::paged(paging_response, self, statement, values),
            factory,
        ))
    }

    async fn spawn_on_runtime<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        // closure: takes Arc<ScyllaSession> and returns a future
        F: FnOnce(Arc<Session>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        // Error: Send + 'static, and also convertible from JoinError for better error handling
        E: From<tokio::task::JoinError> + Send + 'static,
    {
        let session_clone = Arc::clone(&self.inner);

        RUNTIME.spawn(async move { f(session_clone).await }).await?
    }

    pub(crate) async fn execute_single_page(
        &self,
        paging_state: PagingState,
        query_request: ExecutableStatement,
        values: PyValueList,
    ) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
        match query_request {
            ExecutableStatement::Prepared(p) => {
                let serialized_values = p
                    .serialize_values_unstable(&values)
                    .map_err(DriverExecuteError::serialization_failed)?;
                self.spawn_on_runtime(async move |s| {
                    s.execute_unstable(&p, &serialized_values, true, paging_state)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await
            }
            ExecutableStatement::Unprepared(q) => {
                self.spawn_on_runtime(async move |s| {
                    s.query_single_page(q.inner, values, paging_state)
                        .await
                        .map_err(DriverExecuteError::rust_driver_execution_error)
                })
                .await
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum ExecutableStatement {
    Prepared(PreparedStatement),
    Unprepared(PyStatement),
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PyPreparedStatement>() {
            let prepared = prepared.get();
            return Ok(ExecutableStatement::Prepared(prepared.inner.clone()));
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(ExecutableStatement::Unprepared(PyStatement::new(
                text.into(),
                false,
                None,
                None,
                None,
            )));
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(ExecutableStatement::Unprepared(statement.get().clone()));
        }

        let got = obj
            .get_type()
            .name()
            .map(|name| name.to_string())
            .unwrap_or_else(|_| "<unknown type>".to_string());

        Err(DriverStatementConversionError::invalid_statement_type(got))
    }
}

impl From<ExecutableStatement> for BatchStatement {
    fn from(s: ExecutableStatement) -> Self {
        match s {
            ExecutableStatement::Prepared(p) => BatchStatement::PreparedStatement(p),
            ExecutableStatement::Unprepared(q) => BatchStatement::Query(q.inner),
        }
    }
}
