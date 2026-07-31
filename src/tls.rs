use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use pyo3::prelude::*;
/// Selects the TLS protocol mode for [`SslConfig`].
///
/// Mirrors the `ssl.PROTOCOL_TLS_CLIENT` / `ssl.PROTOCOL_TLS_SERVER` constants
/// from Python's standard library.
#[pyclass(frozen, eq, eq_int, from_py_object, name = "SslProtocol")]
#[derive(Clone, PartialEq, Eq, Copy)]
enum PySslProtocol {
    /// Client-side TLS. Sets ``verify_mode=required`` and ``check_hostname=True``
    /// by default — equivalent to ``ssl.PROTOCOL_TLS_CLIENT``.
    #[pyo3(name = "TLS_CLIENT")]
    TlsClient,
    /// Server-side TLS. Sets ``verify_mode=none`` and ``check_hostname=False``
    /// by default — equivalent to ``ssl.PROTOCOL_TLS_SERVER``.
    #[pyo3(name = "TLS_SERVER")]
    TlsServer,
}

#[pymethods]
impl PySslProtocol {
    fn __repr__(&self) -> &'static str {
        match self {
            PySslProtocol::TlsClient => "SslProtocol.TLS_CLIENT",
            PySslProtocol::TlsServer => "SslProtocol.TLS_SERVER",
        }
    }
}

impl PySslProtocol {
    fn ssl_method(self) -> SslMethod {
        match self {
            PySslProtocol::TlsClient => SslMethod::tls_client(),
            PySslProtocol::TlsServer => SslMethod::tls_server(),
        }
    }

    fn default_verify_mode(self) -> SslVerifyMode {
        match self {
            PySslProtocol::TlsClient => SslVerifyMode::PEER,
            PySslProtocol::TlsServer => SslVerifyMode::NONE,
        }
    }

    fn default_check_hostname(self) -> bool {
        matches!(self, PySslProtocol::TlsClient)
    }
}

/// Selects the peer certificate verification mode for [`SslConfig`].
///
/// Mirrors the `ssl.CERT_NONE`, `ssl.CERT_REQUIRED`, and `ssl.CERT_OPTIONAL`
/// constants from Python's standard library.
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

    /// Peer certificates are optional. If presented, they are validated, but
    /// missing certificates are ignored.
    /// Equivalent to ``ssl.CERT_OPTIONAL``.
    #[pyo3(name = "CERT_OPTIONAL")]
    Optional,
}

#[pymethods]
impl PyVerifyMode {
    fn __repr__(&self) -> &'static str {
        match self {
            PyVerifyMode::None => "VerifyMode.CERT_NONE",
            PyVerifyMode::Required => "VerifyMode.CERT_REQUIRED",
            PyVerifyMode::Optional => "VerifyMode.CERT_OPTIONAL",
        }
    }
}

impl From<PyVerifyMode> for SslVerifyMode {
    fn from(mode: PyVerifyMode) -> Self {
        match mode {
            PyVerifyMode::None => SslVerifyMode::NONE,
            PyVerifyMode::Optional => SslVerifyMode::PEER,
            PyVerifyMode::Required => SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT,
        }
    }
}

impl From<SslVerifyMode> for PyVerifyMode {
    fn from(mode: SslVerifyMode) -> Self {
        if mode.contains(SslVerifyMode::FAIL_IF_NO_PEER_CERT) {
            PyVerifyMode::Required
        } else if mode.contains(SslVerifyMode::PEER) {
            PyVerifyMode::Optional
        } else {
            PyVerifyMode::None
        }
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySslProtocol>()?;
    module.add_class::<PyVerifyMode>()?;
    Ok(())
}
