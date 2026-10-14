//! The rows of a page of a `ResponseFuture`, as a reader takes them out of the lock.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use scylla::response::query_result::QueryResult;

use crate::core::results::page_rows;
use crate::deserialize::row_factory::PyRowFactory;
use crate::deserialize::rows::ResolvedPage;
use crate::errors::execution::DriverExecuteError;

/// The rows of a settled page: a list, `None` for a result without rows, or the error.
pub(super) type Rows = PyResult<Option<Py<PyList>>>;

/// The rows of a settled page, taken out of the lock.
pub(super) enum PageRows {
    Built(Rows),
    /// Not built yet: the response and the row factory to build them from.
    ToBuild(Arc<QueryResult>, PyRowFactory),
}

/// Builds the rows of a page. Runs the row factory, which is user Python, so
/// the caller must not hold the lock.
pub(super) fn build_rows(
    py: Python<'_>,
    query_result: Arc<QueryResult>,
    factory: &PyRowFactory,
) -> Rows {
    ResolvedPage::new(py, query_result, factory)
        .map_err(|err| DriverExecuteError::row_factory_failed(err).into())
        .and_then(|page| page_rows(py, page))
}

pub(super) fn clone_rows(py: Python<'_>, rows: &Rows) -> Rows {
    match rows {
        Ok(rows) => Ok(rows.as_ref().map(|list| list.clone_ref(py))),
        Err(err) => Err(err.clone_ref(py)),
    }
}
