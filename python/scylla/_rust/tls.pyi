from enum import IntEnum

class VerifyMode(IntEnum):
    """Controls how the peer certificate is verified during the TLS handshake."""

    CERT_NONE = ...
    """Do not verify the peer certificate."""

    CERT_REQUIRED = ...
    """Require and verify the peer certificate."""

