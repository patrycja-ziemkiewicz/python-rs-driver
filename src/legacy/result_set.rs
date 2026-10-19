//! `ResultSet`: the synchronous, transparently paging row iterator of the legacy driver.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyList;

use crate::future::PyResponseFuture;
use crate::results::PyPagingState;

struct IterState {
    current_rows: Py<PyList>,
}

/// Rows of a request, iterated page by page with further pages fetched
/// transparently. Returned by `ResponseFuture.result()`.
#[pyclass(module = "scylla.legacy", name = "ResultSet", frozen)]
pub(crate) struct PyResultSet {
    response_future: Py<PyResponseFuture>,
    state: Mutex<IterState>,
}

impl PyResultSet {
    /// `rows` are the first page's, `None` for a result without rows.
    pub(crate) fn new(
        py: Python<'_>,
        response_future: Py<PyResponseFuture>,
        rows: Option<Py<PyList>>,
    ) -> Self {
        Self {
            response_future,
            state: Mutex::new(IterState {
                current_rows: rows_or_empty(py, rows),
            }),
        }
    }

    fn future(&self) -> &PyResponseFuture {
        self.response_future.get()
    }

    /// The rows list itself, to run Python code on without holding the state lock.
    fn current_rows_ref(&self, py: Python<'_>) -> Py<PyList> {
        self.state
            .lock_py_attached(py)
            .unwrap()
            .current_rows
            .clone_ref(py)
    }
}

#[pymethods]
impl PyResultSet {
    /// Whether the last page received says more follow.
    #[getter]
    fn has_more_pages(&self, py: Python<'_>) -> bool {
        self.future().has_more_pages(py)
    }

    /// Rows of the current page. Empty does not mean exhausted; see `has_more_pages`.
    #[getter]
    fn current_rows(&self, py: Python<'_>) -> Py<PyList> {
        self.current_rows_ref(py)
    }

    /// The first row of the current page, `None` if it is empty.
    fn one(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let state = self.state.lock_py_attached(py).unwrap();
        let rows = state.current_rows.bind(py);
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(rows.get_item(0)?.unbind()))
    }

    fn __bool__(&self, py: Python<'_>) -> bool {
        let state = self.state.lock_py_attached(py).unwrap();
        !state.current_rows.bind(py).is_empty()
    }

    /// Paging state of the last page, `None` if the query is not paged or exhausted.
    #[getter]
    fn paging_state(&self, py: Python<'_>) -> Option<PyPagingState> {
        self.future().paging_state(py)
    }

    /// Names of the result columns, `None` for a result without rows.
    #[getter]
    fn column_names(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        self.future().column_names(py)
    }

    /// CQL types of the result columns, `None` for a result without rows.
    #[getter]
    fn column_types(&self, py: Python<'_>) -> PyResult<Option<Py<PyList>>> {
        self.future().column_types(py)
    }
}

/// A result without rows iterates as an empty page.
fn rows_or_empty(py: Python<'_>, rows: Option<Py<PyList>>) -> Py<PyList> {
    rows.unwrap_or_else(|| PyList::empty(py).unbind())
}
