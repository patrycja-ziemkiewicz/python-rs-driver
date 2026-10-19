//! `ResultSet`: the synchronous, transparently paging row iterator of the legacy driver.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::PyList;

use crate::future::PyResponseFuture;
use crate::results::PyPagingState;

/// How far the set has been read.
enum Mode {
    /// Nothing read yet.
    NotStarted,
    /// Index of the next row of `current_rows` to yield.
    Iterating(usize),
}

struct IterState {
    current_rows: Py<PyList>,
    mode: Mode,
}

impl IterState {
    /// Replaces the page and starts it from its first row.
    fn start_page(&mut self, rows: Py<PyList>) {
        self.current_rows = rows;
        self.mode = Mode::Iterating(0);
    }
}

/// What `next_row` does after one step under the lock.
enum Step {
    Row(Py<PyAny>),
    End,
    FetchPage,
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
                mode: Mode::NotStarted,
            }),
        }
    }

    fn future(&self) -> &PyResponseFuture {
        self.response_future.get()
    }

    /// Yields the next row, fetching further pages as needed; `None` once exhausted.
    fn next_row(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        loop {
            match self.step(py)? {
                Step::Row(row) => return Ok(Some(row)),
                Step::End => return Ok(None),
                Step::FetchPage => {
                    let rows = self.fetch_page(py)?;
                    self.state.lock_py_attached(py).unwrap().start_page(rows);
                }
            }
        }
    }

    /// One step under the lock: the next row, the end, or a page to fetch.
    fn step(&self, py: Python<'_>) -> PyResult<Step> {
        let mut state = self.state.lock_py_attached(py).unwrap();

        let position = match state.mode {
            Mode::Iterating(position) => position,
            // `next()` before `iter()` starts from the first row.
            Mode::NotStarted => {
                state.mode = Mode::Iterating(0);
                0
            }
        };

        let rows = state.current_rows.bind(py);
        if position < rows.len() {
            let row = rows.get_item(position)?.unbind();
            state.mode = Mode::Iterating(position + 1);
            return Ok(Step::Row(row));
        }

        if self.future().has_more_pages(py) {
            return Ok(Step::FetchPage);
        }
        // The legacy driver also clears the rows at the end.
        state.start_page(PyList::empty(py).unbind());
        Ok(Step::End)
    }

    /// Requests the next page and blocks for its rows.
    fn fetch_page(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.future().start_fetching_next_page(py)?;
        let rows = self.future().wait_rows(py)?;
        Ok(rows_or_empty(py, rows))
    }

    /// Every row from the start of the current page onwards, as a new list.
    fn drain_from_start(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.restart_page(py);

        let all = PyList::empty(py);
        while let Some(row) = self.next_row(py)? {
            all.append(row)?;
        }
        Ok(all.unbind())
    }

    /// Starts iterating the current page from its first row.
    fn restart_page(&self, py: Python<'_>) {
        self.state.lock_py_attached(py).unwrap().mode = Mode::Iterating(0);
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

    /// Every remaining row as a list; `list(result_set)`.
    fn all(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.drain_from_start(py)
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

    fn __iter__(slf: Py<Self>, py: Python<'_>) -> Py<Self> {
        slf.get().restart_page(py);
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_row(py)
    }

    /// Fetches the next page synchronously into `current_rows`; not needed when iterating.
    fn fetch_next_page(&self, py: Python<'_>) -> PyResult<()> {
        let rows = if self.future().has_more_pages(py) {
            self.fetch_page(py)?
        } else {
            PyList::empty(py).unbind()
        };
        let mut state = self.state.lock_py_attached(py).unwrap();
        state.current_rows = rows;

        if let Mode::Iterating(position) = &mut state.mode {
            *position = 0;
        }
        Ok(())
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
