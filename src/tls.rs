use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use pyo3::prelude::*;
/// Selects the peer certificate verification mode for [`SslConfig`].
///
/// Mirrors the `ssl.CERT_NONE` and `ssl.CERT_REQUIRED` constants
/// from Python's standard library.
#[pyclass(frozen, eq, eq_int, from_py_object, name = "VerifyMode")]
#[derive(Clone, PartialEq, Eq, Copy, Debug)]
pub enum PyVerifyMode {
    /// Peer certificates are ignored and validation is disabled.
    /// Equivalent to ``ssl.CERT_NONE``.
    #[pyo3(name = "CERT_NONE")]
    None,

    /// Peer certificates are required and strictly validated. If no certificate
    /// is presented or validation fails, the handshake is aborted.
    /// Equivalent to ``ssl.CERT_REQUIRED``.
    #[pyo3(name = "CERT_REQUIRED")]
    Required,
}

#[pymethods]
impl PyVerifyMode {
    fn __repr__(&self) -> &'static str {
        match self {
            PyVerifyMode::None => "VerifyMode.CERT_NONE",
            PyVerifyMode::Required => "VerifyMode.CERT_REQUIRED",
        }
    }
}

impl From<PyVerifyMode> for SslVerifyMode {
    fn from(mode: PyVerifyMode) -> Self {
        match mode {
            PyVerifyMode::None => SslVerifyMode::NONE,
            PyVerifyMode::Required => SslVerifyMode::PEER,
        }
    }
}

impl From<SslVerifyMode> for PyVerifyMode {
    fn from(mode: SslVerifyMode) -> Self {
        if mode.contains(SslVerifyMode::FAIL_IF_NO_PEER_CERT) {
            PyVerifyMode::Required
        } else {
            PyVerifyMode::None
        }
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyVerifyMode>()?;
    Ok(())
}
