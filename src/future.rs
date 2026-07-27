use crate::utils::PrependedIterator;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{BoundObject, Py, PyAny, PyResult};

/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Py<PyTuple>,
    kwargs: Option<Py<PyDict>>,
}

impl Callback {
    fn new(
        callable: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> Self {
        Self {
            callable,
            args: args.clone().unbind(),
            kwargs: kwargs.map(|k| k.clone().unbind()),
        }
    }

    /// Invoke this callback, passing `value` as the first argument
    /// followed by any extra args/kwargs. Errors are logged and swallowed.
    fn invoke(&self, py: Python<'_>, value: &Py<PyAny>) {
        let extra = self.args.bind(py);
        let first = value.clone_ref(py).into_any();
        let rest = extra.iter().map(|item| item.unbind());
        let exact_size_wrapper = PrependedIterator::new(first, rest);
        let args = PyTuple::new(py, exact_size_wrapper)
            .expect("failed to allocate PyTuple for callback args");

        let kwargs = self.kwargs.as_ref().map(|k| Bound::clone(k.bind(py)));
        if let Err(err) = self.callable.call(py, args, kwargs.as_ref()) {
            log::error!("ResponseFuture callback raised an exception: {}", err);
        }
    }
}

/// Discriminates whether a [`Callback`] fires on success or on error.
enum CallbackKind {
    /// Fired when the future resolves successfully. Passes the result value.
    OnSuccess(Callback),
    /// Fired when the future resolves with an error. Passes the exception instance.
    OnError(Callback),
}

impl CallbackKind {
    /// Invoke this callback if its variant matches the outcome of `result`.
    fn invoke(&self, py: Python<'_>, result: &PyResult<Py<PyAny>>) {
        match (self, result) {
            (CallbackKind::OnSuccess(cb), Ok(value)) => {
                cb.invoke(py, value);
            }
            (CallbackKind::OnError(cb), Err(err)) => {
                let exc_obj = err.value(py);
                cb.invoke(py, exc_obj.as_any().as_unbound());
            }
            _ => {}
        }
    }

    /// Fire every callback in `callbacks` that matches the outcome of `result`.
    fn fire_all(py: Python<'_>, callbacks: Vec<CallbackKind>, result: &PyResult<Py<PyAny>>) {
        for cb in &callbacks {
            cb.invoke(py, result);
        }
    }
}

