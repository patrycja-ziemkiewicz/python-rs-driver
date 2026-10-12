use std::sync::Arc;

use scylla::frame::response::result::ColumnSpec;
use scylla::response::query_result::QueryResult;

use crate::core::results::{Pager, PendingRequestResult, columns};
use crate::deserialize::row_factory::PyRowFactory;

/// A page a `ResponseFuture` received, before its rows are built.
///
/// Everything but the rows is readable without the GIL.
#[expect(
    dead_code,
    reason = "ResponseFuture uses it; it lands in a later commit"
)]
pub(crate) struct PendingResponsePage {
    query_result: Arc<QueryResult>,
    query_pager: Pager,
    factory: PyRowFactory,
}

impl From<PendingRequestResult> for PendingResponsePage {
    fn from(result: PendingRequestResult) -> Self {
        let (query_result, query_pager, factory) = result.into_parts();
        Self {
            query_result: Arc::new(query_result),
            query_pager,
            factory,
        }
    }
}

#[expect(
    dead_code,
    reason = "ResponseFuture uses it; it lands in a later commit"
)]
impl PendingResponsePage {
    pub(crate) fn query_result(&self) -> &Arc<QueryResult> {
        &self.query_result
    }

    /// The row factory the request resolved to, the same for every page.
    pub(crate) fn factory(&self) -> &PyRowFactory {
        &self.factory
    }

    /// The pager positioned after this page.
    pub(crate) fn pager(&self) -> &Pager {
        &self.query_pager
    }

    /// Specifications of the page's columns, or `None` for a result without rows.
    pub(crate) fn columns(&self) -> Option<&[ColumnSpec<'_>]> {
        columns(&self.query_result)
    }
}
