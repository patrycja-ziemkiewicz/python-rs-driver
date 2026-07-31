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
#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySslProtocol>()?;
    Ok(())
}
