from enum import IntEnum
from pathlib import Path

class SslProtocol(IntEnum):
    """TLS protocol mode used when creating an :class:`SslConfig`."""

    TLS_CLIENT = ...
    """
    Client-side TLS. Defaults to ``verify_mode = CERT_REQUIRED`` and
    ``check_hostname = True``.
    """

    TLS_SERVER = ...
    """
    Server-side TLS. Defaults to ``verify_mode = CERT_NONE`` and
    ``check_hostname = False``.
    """

class VerifyMode(IntEnum):
    """Controls how the peer certificate is verified during the TLS handshake."""

    CERT_NONE = ...
    """Do not verify the peer certificate."""

    CERT_OPTIONAL = ...
    """Verify the peer certificate if one is provided; succeed if none is sent."""

    CERT_REQUIRED = ...
    """Require and verify the peer certificate."""

class SslConfig:
    """
    TLS configuration for a ScyllaDB session.

    Pass an instance to :meth:`~scylla.session_builder.SessionBuilder.tls_context` —
    the OpenSSL context is built internally at connection time.
    """

    def __init__(self, protocol: SslProtocol = SslProtocol.TLS_CLIENT) -> None:
        """Create a new ``SslConfig`` with the given protocol mode."""

    def load_verify_locations(self, cafile: str | Path) -> None:
        """Set the CA certificate file used to verify the peer's certificate."""

    def load_cert_chain(
        self,
        certfile: str | Path,
        keyfile: str | Path | None = None,
    ) -> None:
        """
        Set the client certificate and private key for mutual TLS (mTLS).

        If ``keyfile`` is ``None``, the private key is read from ``certfile``.
        """

    @property
    def verify_mode(self) -> VerifyMode:
        """The current peer-certificate verification mode."""

    @verify_mode.setter
    def verify_mode(self, mode: VerifyMode) -> None: ...
    @property
    def check_hostname(self) -> bool:
        """
        Whether the server hostname is verified against the certificate CN/SAN.

        Has no effect when ``verify_mode`` is ``CERT_NONE``.
        Defaults to ``True`` for ``TLS_CLIENT``, ``False`` for ``TLS_SERVER``.
        """

    @check_hostname.setter
    def check_hostname(self, value: bool) -> None: ...
    @property
    def protocol(self) -> SslProtocol:
        """The TLS protocol mode set at construction time."""
