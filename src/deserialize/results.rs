use crate::cluster::metadata::query_metadata::column_spec_tuple;
use crate::core::results::{Pager, PendingRequestResult, RequestResultCore, next_row_with_paging};
use crate::deserialize::error::{DriverDeserializationError, DriverRowIterationError};
use crate::deserialize::row_factory::{
    PyDictRowFactory, PyRowFactory, PyRowFactoryBase, RowBuilder,
};
use crate::deserialize::value::{PyDeserializeValue, PyDeserializedValue};
use crate::future::{DriverFuture, boxed_py_future};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::sync::{MutexExt, PyOnceLock};
use pyo3::types::{PyList, PyTuple};
use pyo3::{Bound, Py, PyAny, PyErr, PyRef, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::deserialize::DeserializationError as ScyllaDeserializationError;
use scylla::response::query_result::QueryResult;
use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::result::RawRowIterator;
use scylla_cql::deserialize::row::{ColumnIterator, RawColumn};
use scylla_cql::frame::request::query::PagingState;
use stable_deref_trait::StableDeref;
use std::iter::Enumerate;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use yoke::{Yoke, Yokeable};

/// Database query result with paging support.
///
/// Represents a result frame from the database, providing access to rows
/// and support for fetching additional pages.
///
/// Python-facing facade over [`RequestResultCore`]: each method clones the core,
/// hands the work over, and awaits it.
#[pyclass(module = "scylla.results", frozen)]
pub(crate) struct RequestResult {
    core: RequestResultCore,

    /// Cached Python-side result column specifications.
    columns: PyOnceLock<Py<PyTuple>>,
}

impl From<RequestResultCore> for RequestResult {
    fn from(core: RequestResultCore) -> Self {
        Self {
            core,
            columns: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl RequestResult {
    /// Returns `true` if more pages are available.
    ///
    /// # Returns
    ///
    /// `true` if additional pages can be fetched, `false` otherwise.
    fn has_more_pages(&self) -> bool {
        self.core.has_more_pages()
    }

    /// Returns the current paging state.
    ///
    /// Can be `None` if there are no more pages available.
    /// The paging state can be passed to `execute()` to resume paging
    /// from a specific position.
    ///
    /// # Returns
    ///
    /// Current paging state or `None` if no more pages are available.
    fn paging_state(&self) -> Option<PyPagingState> {
        self.core
            .paging_state()
            .map(|inner| PyPagingState { inner })
    }

    /// Fetches the next page if available.
    ///
    /// Returns a new `RequestResult` with the next page's data if more pages
    /// are available. Returns `None` if no more pages exist.
    ///
    /// # Returns
    ///
    /// `Some(RequestResult)` with the next page data, or `None` if no more pages.
    ///
    /// # Errors
    ///
    /// Returns an error if the fetch operation fails.
    fn fetch_next_page(
        &self,
        py: Python<'_>,
    ) -> PyResult<DriverFuture<Option<PendingRequestResult>, PyErr>> {
        let core = self.core.clone();

        DriverFuture::spawn(
            py,
            boxed_py_future(async move { core.fetch_next_page().await }),
        )
    }

    /// Returns an iterator over rows in the current page.
    ///
    /// Creates a `SinglePageIterator` that yields deserialized rows
    /// from the current page only, without fetching additional pages.
    ///
    /// # Returns
    ///
    /// Iterator over rows in the current page.
    fn iter_current_page(&self, py: Python<'_>) -> PyResult<SinglePageIterator> {
        SinglePageIterator::new(py, self.core.page.clone())
    }

    /// Returns an async iterator over all rows with automatic paging.
    ///
    /// Creates an `AsyncRowsIterator` that transparently fetches
    /// subsequent pages as iteration progresses.
    ///
    /// # Returns
    ///
    /// Async iterator over all rows across all pages.
    pub fn __aiter__(&self, py: Python<'_>) -> PyResult<AsyncRowsIterator> {
        AsyncRowsIterator::new(
            py,
            self.core.query_pager.clone(),
            self.core.page.clone(),
            self.core.row_factory.clone(),
        )
    }

    /// Returns the first row starting from the current state.
    ///
    /// Fetches the first available row from the current page onwards,
    /// automatically retrieving additional pages as needed.
    /// Returns `None` if no more rows are available.
    ///
    /// # Returns
    ///
    /// The first row as a Python object from current state, or `None` if no more rows exist.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching or deserialization fails.
    pub fn first_row(&self, py: Python<'_>) -> PyResult<DriverFuture<Py<PyAny>, PyErr>> {
        let core = self.core.clone();

        DriverFuture::spawn(py, boxed_py_future(async move { core.first_row().await }))
    }

    /// Returns all rows from all pages with automatic paging.
    ///
    /// Fetches and returns all available rows across all pages as a Python list,
    /// automatically retrieving additional pages as needed.
    ///
    /// # Returns
    ///
    /// A list containing all rows as Python objects.
    ///
    /// # Errors
    ///
    /// Returns an error if fetching or deserialization fails.
    pub fn all(&self, py: Python<'_>) -> PyResult<DriverFuture<Py<PyList>, PyErr>> {
        let core = self.core.clone();

        DriverFuture::spawn(py, boxed_py_future(async move { core.all().await }))
    }

    /// Specifications of the columns in this result.
    ///
    /// Empty for a result that carries no rows, such as an `INSERT`.
    #[getter]
    fn get_columns(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        let columns = self.columns.get_or_try_init(py, || {
            match self
                .core
                .page
                .query_result()
                .deserialized_metadata_and_rows()
            {
                None => column_spec_tuple(py, &[]),
                Some(rows) => column_spec_tuple(py, rows.metadata().col_specs()),
            }
        })?;
        Ok(columns.clone_ref(py))
    }
}

/// Iterator over a single page of query results.
///
/// Yields rows materialized by the request's row factory.
#[pyclass(module = "scylla.results", frozen)]
struct SinglePageIterator {
    kind: std::sync::Mutex<RowsIteratorKind>,
}

impl SinglePageIterator {
    fn new(py: Python<'_>, page: ResolvedPage) -> PyResult<Self> {
        Ok(SinglePageIterator {
            kind: std::sync::Mutex::new(RowsIteratorKind::new(py, page)?),
        })
    }
}

#[pymethods]
impl SinglePageIterator {
    pub fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.kind.lock_py_attached(py).map_err(|_| {
            PyErr::new::<PyRuntimeError, _>("SinglePageIterator mutex was poisoned")
        })?;

        match guard.next(py) {
            Some(res) => res.map_err(Into::into),
            None => Err(PyErr::new::<PyStopIteration, _>("")),
        }
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Represents paging state for paged queries.
///
/// Used to continue a query from where the previous page ended.
/// Can be passed to execute() to resume paging from a specific position.
#[pyclass(module = "scylla.results", name = "PagingState", frozen)]
pub struct PyPagingState {
    pub(crate) inner: PagingState,
}

#[pymethods]
impl PyPagingState {
    /// Creates a new paging state starting from the first page.
    #[new]
    fn new() -> Self {
        PyPagingState {
            inner: PagingState::start(),
        }
    }

    /// Returns the inner representation of `PagingState` as bytes.
    ///
    /// Use this to store paging state for a longer time, and later restore it
    /// using `from_bytes()`. Returns `None` if this represents the start state.
    ///
    /// # Returns
    ///
    /// Raw paging state bytes, or `None` for the start state.
    pub fn as_bytes<'py>(&self, py: Python<'py>) -> Option<Bound<'py, pyo3::types::PyBytes>> {
        self.inner
            .as_bytes_slice()
            .map(|arc_slice| pyo3::types::PyBytes::new(py, arc_slice))
    }

    /// Creates `PagingState` from raw bytes.
    ///
    /// Use this to restore paging state after longer time, having previously
    /// stored it using `as_bytes()`.
    ///
    /// # Parameters
    ///
    /// raw_bytes : Raw paging state bytes previously obtained from `as_bytes()`.
    ///
    /// # Returns
    ///
    /// A new `PagingState` restored from the raw bytes.
    #[staticmethod]
    pub fn from_bytes(raw_bytes: &[u8]) -> Self {
        Self {
            inner: PagingState::new_from_raw_bytes(raw_bytes),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

/// Async iterator over all rows with automatic paging.
///
/// Fetches subsequent pages transparently as iteration progresses.
#[pyclass(module = "scylla.results", frozen)]
pub struct AsyncRowsIterator {
    state: Arc<Mutex<AsyncIteratorState>>,
}

impl AsyncRowsIterator {
    fn new(
        py: Python<'_>,
        paging_api: Pager,
        page: ResolvedPage,
        factory: PyRowFactory,
    ) -> PyResult<Self> {
        Ok(AsyncRowsIterator {
            state: Arc::new(Mutex::new(AsyncIteratorState {
                rows_iterator: RowsIteratorKind::new(py, page)?,
                query_pager: paging_api,
                factory,
            })),
        })
    }
}

#[pymethods]
impl AsyncRowsIterator {
    pub(crate) fn __anext__(&self, py: Python<'_>) -> PyResult<DriverFuture<Py<PyAny>, PyErr>> {
        if let Ok(state) = self.state.try_lock()
            && let Some(row_result) = state.rows_iterator.next(py)
        {
            let result = row_result.map_err(Into::into);
            return DriverFuture::ready(py, result);
        }

        let state_clone = self.state.clone();

        let future = boxed_py_future(async move {
            let mut state = state_clone.lock().await;

            let AsyncIteratorState {
                rows_iterator,
                query_pager,
                factory,
            } = &mut *state;

            match next_row_with_paging(rows_iterator, query_pager, factory).await {
                Some(res) => res.map_err(Into::into),
                None => Err(PyErr::new::<PyStopAsyncIteration, _>("")),
            }
        });

        DriverFuture::spawn(py, future)
    }

    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

/// Mutable state for async row iteration.
///
/// Holds current row iterator and pagination state.
#[derive(Clone)]
struct AsyncIteratorState {
    rows_iterator: RowsIteratorKind,
    query_pager: Pager,
    factory: PyRowFactory,
}

/// Iterator over the rows of a single page and the columns of the current row.
///
/// TODO: Still a pyclass only because it is held through `Py`; no Python code
/// sees it. The following commits own it directly and replace it with a Rust
/// column iterator.
#[pyclass(module = "scylla.results", name = "ColumnIterator")]
pub struct RowColumnCursor {
    // Yoke-backed container holding both row and column iterators.
    //
    // The yoke ensures that iterators can borrow directly from the
    // underlying query result frame without cloning buffers or allocating
    // intermediate representations.
    //
    // `Cursor` holds:
    // - a `RawRowIterator` to advance between rows
    // - a `ColumnIterator` for iterating columns of the current row
    yoked: Yoke<Cursor<'static>, QueryResultCart>,
}

impl RowColumnCursor {
    fn new(query_result: Arc<QueryResult>) -> Self {
        let cart = QueryResultCart(query_result);

        let yoked = Yoke::attach_to_cart(cart, |cart| {
            let raw_rows_with_metadata = cart.deserialized_metadata_and_rows().expect(
                "deserialized_metadata_and_rows can't be None after is_rows() returned true",
            );
            let frame_slice = FrameSlice::new(raw_rows_with_metadata.raw_rows());
            let col_specs = raw_rows_with_metadata.metadata().col_specs();
            let row_iterator =
                RawRowIterator::new(raw_rows_with_metadata.rows_count(), col_specs, frame_slice);

            let column_iterator = ColumnIterator::new(col_specs, frame_slice).enumerate();

            Cursor {
                row_iterator,
                column_iterator,
                current_raw_column: None,
            }
        });

        Self { yoked }
    }

    fn next_column(
        &mut self,
        py: Python<'_>,
    ) -> Option<Result<PyDeserializedValue, DriverDeserializationError>> {
        if let Err(err) = self
            .yoked
            .with_mut_return(|view: &mut Cursor<'_>| view.next_column())
            .map_err(DriverDeserializationError::scylla_decode_failed)
        {
            return Some(Err(err));
        }

        let cursor = self.yoked.get();

        // If `current_raw_column` is None, it means all columns of the current row have been exhausted.
        let (column_index, raw_col) = cursor.current_raw_column.as_ref()?;

        let value = match PyDeserializedValue::deserialize_py(raw_col.spec.typ(), raw_col.slice, py)
        {
            Ok(value) => value,
            Err(err) => {
                return Some(Err(err
                    .at_column_name(raw_col.spec.name())
                    .at_column_index(*column_index)));
            }
        };

        Some(Ok(value))
    }
}

/// A page together with the row builder resolved against its columns.
#[derive(Clone)]
pub(crate) struct ResolvedPage {
    query_result: Arc<QueryResult>,
    builder: Option<RowBuilder>,
}

impl ResolvedPage {
    pub(crate) fn new(
        py: Python<'_>,
        query_result: Arc<QueryResult>,
        factory: &PyRowFactory,
    ) -> PyResult<Self> {
        let builder = query_result
            .deserialized_metadata_and_rows()
            .map(|rows| RowBuilder::resolve(py, factory, rows.metadata().col_specs()))
            .transpose()?;

        Ok(Self {
            query_result,
            builder,
        })
    }

    pub(crate) fn query_result(&self) -> &QueryResult {
        &self.query_result
    }
}

/// Determines how to iterate over query results based on result type.
///
/// Dispatches to either row iteration or handles non-row results.
#[derive(Clone)]
pub(crate) enum RowsIteratorKind {
    Rows {
        row_col_cursor: Py<RowColumnCursor>,
        builder: RowBuilder,
    },
    NonRows,
}

impl RowsIteratorKind {
    pub(crate) fn new(py: Python<'_>, page: ResolvedPage) -> PyResult<Self> {
        let Some(builder) = page.builder else {
            return Ok(RowsIteratorKind::NonRows);
        };

        let row_col_cursor = Py::new(py, RowColumnCursor::new(page.query_result))?;

        Ok(RowsIteratorKind::Rows {
            row_col_cursor,
            builder,
        })
    }

    /// Switches to the next page, resolving the row builder against its columns.
    pub(crate) fn update(
        &mut self,
        py: Python,
        query_result: Arc<QueryResult>,
        factory: &PyRowFactory,
    ) -> PyResult<()> {
        *self = Self::new(py, ResolvedPage::new(py, query_result, factory)?)?;
        Ok(())
    }

    pub(crate) fn next(&self, py: Python) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
        match self {
            RowsIteratorKind::Rows {
                row_col_cursor,
                builder,
            } => {
                let res = row_col_cursor
                    .borrow_mut(py)
                    .yoked
                    .with_mut_return(|cursor| cursor.next_row())?;

                let mut cursor = row_col_cursor.borrow_mut(py);

                match res {
                    Ok(()) => {
                        // TODO: Collected into a Vec for now; a later commit hands
                        // the values to the builder as they are deserialized.
                        let out = std::iter::from_fn(|| cursor.next_column(py))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(DriverRowIterationError::Deserialization)
                            .and_then(|values| builder.build(py, values.into_iter().map(Ok)));

                        Some(out)
                    }
                    Err(err) => Some(Err(DriverRowIterationError::Deserialization(
                        DriverDeserializationError::scylla_decode_failed(err),
                    ))),
                }
            }
            RowsIteratorKind::NonRows => None,
        }
    }
}

/// Stable cart holding deserialized metadata and raw row data.
///
/// This type exists solely to serve as a `StableDeref` cart for `Yoke`.
struct QueryResultCart(Arc<QueryResult>);

impl Deref for QueryResultCart {
    type Target = QueryResult;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl StableDeref for QueryResultCart {}

/// Yoke-backed wrapper holding row and column iterators.
///
/// `Cursor` is stored inside a `Yoke` so that both the row iterator
///  and the column iterator can borrow from the same data without cloning.
///
/// - `next_row` advances the row iterator and switches the active column
///   iterator to the value received from row iterator.
/// - `next_column` advances the column iterator and caches the current raw
///   column; Python deserialization is performed by `RowColumnCursor::next_column`.
#[derive(Yokeable)]
struct Cursor<'a> {
    row_iterator: RawRowIterator<'a, 'a>,
    column_iterator: Enumerate<ColumnIterator<'a, 'a>>,
    current_raw_column: Option<(usize, RawColumn<'a, 'a>)>,
}

impl<'a> Cursor<'a> {
    fn next_column(&mut self) -> Result<(), ScyllaDeserializationError> {
        self.current_raw_column = self
            .column_iterator
            .next()
            .map(|(column_index, raw_column_result)| {
                raw_column_result.map(|raw_col| (column_index, raw_col))
            })
            .transpose()?;

        Ok(())
    }

    fn next_row(&mut self) -> Option<Result<(), ScyllaDeserializationError>> {
        let column_iterator = self.row_iterator.next()?;

        match column_iterator {
            Ok(column_iterator) => {
                self.column_iterator = column_iterator.enumerate();
                Some(Ok(()))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

#[pymodule]
pub(crate) fn results(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRowFactoryBase>()?;
    module.add_class::<PyDictRowFactory>()?;
    module.add_class::<RowColumnCursor>()?;
    module.add_class::<SinglePageIterator>()?;
    module.add_class::<PyPagingState>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<AsyncRowsIterator>()?;

    Ok(())
}
