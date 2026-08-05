use openssl::ssl::{SslConnector, SslContext, SslFiletype, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use std::path::PathBuf;
use std::sync::Mutex;

pub use crate::errors::TlsConfigError;
use crate::utils::WithOriginalPyObject;

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

/// Internal config data stored inside [`PySslConfig`].
struct SslConfigInner {
    ca_file: Option<PathBuf>,
    ca_path: Option<PathBuf>,
    ca_data: Option<WithOriginalPyObject<CaData>>,
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
    verify_mode: SslVerifyMode,
}

impl SslConfigInner {
    fn new() -> Self {
        Self {
            ca_file: None,
            ca_path: None,
            ca_data: None,
            cert_file: None,
            key_file: None,
            verify_mode: SslVerifyMode::PEER,
        }
    }
}

/// TLS configuration for a ScyllaDB session.
///
/// Mirrors the interface of Python's `ssl.SSLContext`. Pass an instance of this class to
/// ``SessionBuilder.tls_context()`` — a snapshot of the configuration is taken
/// at that moment, and the actual OpenSSL context is built internally when needed.
#[pyclass(frozen, name = "TlsContext")]
pub(crate) struct PyTlsContext {
    inner: Mutex<SslConfigInner>,
}

#[pymethods]
impl PyTlsContext {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(SslConfigInner::new()),
        }
    }

    /// Load CA certificates used to verify the server's certificate.
    ///
    /// Equivalent to ``ssl.SSLContext.load_verify_locations()``.
    #[pyo3(signature = (cafile = None, capath = None, cadata = None))]
    fn load_verify_locations<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        cafile: Option<PathBuf>,
        capath: Option<PathBuf>,
        cadata: Option<WithOriginalPyObject<CaData>>,
    ) -> PyResult<()> {
        if cafile.is_none() && capath.is_none() && cadata.is_none() {
            return Err(TlsConfigError::NoCaLocationsSpecified.into());
        }

        let mut inner = slf.inner.lock_py_attached(py).unwrap();
        inner.ca_file = cafile;
        inner.ca_path = capath;
        inner.ca_data = cadata;
        Ok(())
    }

    /// Load the client certificate and optional private key for mutual TLS (mTLS).
    ///
    /// Equivalent to ``ssl.SSLContext.load_cert_chain()``.
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

    #[getter]
    fn get_verify_mode(&self, py: Python<'_>) -> PyVerifyMode {
        let mode = self.inner.lock_py_attached(py).unwrap().verify_mode;
        mode.into()
    }
}

#[derive(Clone)]
struct CaData(Vec<X509>);

impl<'py> FromPyObject<'_, 'py> for CaData {
    type Error = TlsConfigError;

    fn extract(ca_data: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let certificates = if let Ok(pem) = ca_data.extract::<String>() {
            X509::stack_from_pem(pem.as_bytes())
        } else if let Ok(der) = ca_data.extract::<Vec<u8>>() {
            X509::from_der(&der).map(|certificate| vec![certificate])
        } else {
            return Err(TlsConfigError::CaDataLoadFailed(
                "cadata must be a PEM string or DER bytes".to_string(),
            ));
        }
        .map_err(|e| TlsConfigError::CaDataLoadFailed(e.to_string()))?;
        Ok(Self(certificates))
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTlsContext>()?;
    module.add_class::<PyVerifyMode>()?;
    Ok(())
}
