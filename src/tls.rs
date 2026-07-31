use openssl::ssl::{SslContext, SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use std::path::PathBuf;
use std::sync::Mutex;

pub use crate::errors::TlsConfigError;

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

/// Internal config data stored inside [`PySslConfig`].
struct SslConfigInner {
    protocol: PySslProtocol,
    ca_file: Option<PathBuf>,
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    verify_mode: SslVerifyMode,
    check_hostname: bool,
}

impl SslConfigInner {
    fn new(protocol: PySslProtocol) -> Self {
        Self {
            protocol,
            ca_file: None,
            cert_file: None,
            key_file: None,
            verify_mode: protocol.default_verify_mode(),
            check_hostname: protocol.default_check_hostname(),
        }
    }
}

/// TLS configuration for a ScyllaDB session.
///
/// Mirrors the interface of Python's `ssl.SSLContext`. Pass an instance of this class to
/// ``SessionBuilder.ssl_context()`` — the actual OpenSSL context is built
/// internally at connection time.
#[pyclass(frozen, name = "SslConfig")]
pub(crate) struct PySslConfig {
    inner: Mutex<SslConfigInner>,
}

#[pymethods]
impl PySslConfig {
    #[new]
    #[pyo3(signature = (protocol = PySslProtocol::TlsClient))]
    fn new(protocol: PySslProtocol) -> Self {
        Self {
            inner: Mutex::new(SslConfigInner::new(protocol)),
        }
    }

    /// Load a CA certificate file used to verify the server's certificate.
    ///
    /// Equivalent to ``ssl.SSLContext.load_verify_locations(cafile=...)``.
    fn load_verify_locations<'py>(slf: PyRef<'py, Self>, py: Python<'py>, cafile: PathBuf) {
        slf.inner.lock_py_attached(py).unwrap().ca_file = Some(cafile);
    }

    /// Load the client certificate and optional private key for mutual TLS (mTLS).
    ///
    /// Equivalent to ``ssl.SSLContext.load_cert_chain(certfile, keyfile)``.
    #[pyo3(signature = (certfile, keyfile = None))]
    fn load_cert_chain<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        certfile: PathBuf,
        keyfile: Option<PathBuf>,
    ) {
        let mut inner = slf.inner.lock_py_attached(py).unwrap();
        inner.cert_file = Some(certfile);
        inner.key_file = keyfile;
    }

    #[setter]
    fn set_verify_mode(slf: PyRef<'_, Self>, py: Python<'_>, mode: PyVerifyMode) -> PyResult<()> {
        let verify: SslVerifyMode = mode.into();

        slf.inner.lock_py_attached(py).unwrap().verify_mode = verify;
        Ok(())
    }

    #[setter]
    fn set_check_hostname(slf: PyRef<'_, Self>, py: Python<'_>, value: bool) {
        slf.inner.lock_py_attached(py).unwrap().check_hostname = value;
    }

    #[getter]
    fn get_verify_mode(&self, py: Python<'_>) -> PyVerifyMode {
        let mode = self.inner.lock_py_attached(py).unwrap().verify_mode;
        mode.into()
    }

    #[getter]
    fn get_check_hostname(&self, py: Python<'_>) -> bool {
        self.inner.lock_py_attached(py).unwrap().check_hostname
    }

    #[getter]
    fn get_protocol(&self, py: Python<'_>) -> PySslProtocol {
        self.inner.lock_py_attached(py).unwrap().protocol
    }
}

impl PySslConfig {
    /// Build an [`SslContext`] from the stored configuration.
    ///
    /// Called internally by `SessionBuilder`.
    pub(crate) fn build(&self, py: Python<'_>) -> Result<SslContext, TlsConfigError> {
        let inner = self.inner.lock_py_attached(py).unwrap();

        let mut builder = SslContextBuilder::new(inner.protocol.ssl_method())
            .map_err(|e| TlsConfigError::ContextCreationFailed(e.to_string()))?;

        if let Some(ca_file) = &inner.ca_file {
            builder
                .set_ca_file(ca_file)
                .map_err(|e| TlsConfigError::CaFileLoadFailed {
                    path: ca_file.clone(),
                    cause: e.to_string(),
                })?;
        }

        if let Some(cert_file) = &inner.cert_file {
            builder
                .set_certificate_file(cert_file, SslFiletype::PEM)
                .map_err(|e| TlsConfigError::CertFileLoadFailed {
                    path: cert_file.clone(),
                    cause: e.to_string(),
                })?;

            // If no separate keyfile was given, OpenSSL reads the key from the
            // cert file itself — same behaviour as Python's ssl when keyfile=None.
            let key_path = inner.key_file.as_ref().unwrap_or(cert_file);
            builder
                .set_private_key_file(key_path, SslFiletype::PEM)
                .map_err(|e| TlsConfigError::KeyFileLoadFailed {
                    path: key_path.clone(),
                    cause: e.to_string(),
                })?;
        }

        builder.set_verify(inner.verify_mode);

        Ok(builder.build())
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySslProtocol>()?;
    module.add_class::<PySslConfig>()?;
    module.add_class::<PyVerifyMode>()?;
    Ok(())
}
