from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from scylla.errors import SessionConfigError, TlsError
from scylla.session_builder import SessionBuilder
from scylla.tls import SslConfig, SslProtocol, VerifyMode
from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)

pytestmark = pytest.mark.requires_ccm

SCYLLA_VERSION = "release:6.2.2"


# ─────────────────────────────────────────────────────────────────────────────
# Certificate generation helpers
# ─────────────────────────────────────────────────────────────────────────────


def _generate_private_key() -> RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _generate_ca(key: RSAPrivateKey) -> x509.Certificate:
    now = datetime.now(timezone.utc)
    name = _name("Test CA")
    return (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )


def _generate_leaf(
    *,
    common_name: str,
    key: RSAPrivateKey,
    ca_cert: x509.Certificate,
    ca_key: RSAPrivateKey,
    usage: x509.ObjectIdentifier,
    san: x509.GeneralName | list[x509.GeneralName] | None = None,
    not_valid_before: datetime | None = None,
    not_valid_after: datetime | None = None,
) -> x509.Certificate:
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name(common_name))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_valid_before or (now - timedelta(minutes=1)))
        .not_valid_after(not_valid_after or (now + timedelta(days=1)))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
    )
    if san is not None:
        san_list = san if isinstance(san, list) else [san]
        builder = builder.add_extension(
            x509.SubjectAlternativeName(san_list),
            critical=False,
        )
    return builder.sign(ca_key, hashes.SHA256())


def _write_key(path: Path, key: RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def certs_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate ephemeral CA + server + client certs."""
    d = tmp_path_factory.mktemp("tls-certs")

    # CA
    ca_key = _generate_private_key()
    ca_cert = _generate_ca(ca_key)

    # Server cert (SAN includes all IPs used by CCM clusters)
    server_key = _generate_private_key()
    server_cert = _generate_leaf(
        common_name="127.0.0.1",
        key=server_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.SERVER_AUTH,
        san=[
            x509.IPAddress(ip_address("127.0.0.1")),
            x509.IPAddress(ip_address("127.0.1.1")),
            x509.IPAddress(ip_address("127.0.2.1")),
        ],
    )

    # Client cert (for mutual TLS)
    client_key = _generate_private_key()
    client_cert = _generate_leaf(
        common_name="Test Client",
        key=client_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )

    # Expired client cert (for negative test)
    expired_client_key = _generate_private_key()
    expired_client_cert = _generate_leaf(
        common_name="Expired Client",
        key=expired_client_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
        not_valid_before=datetime.now(timezone.utc) - timedelta(days=2),
        not_valid_after=datetime.now(timezone.utc) - timedelta(hours=1),
    )

    # Unrelated CA (for wrong-CA test)
    wrong_ca_key = _generate_private_key()
    wrong_ca_cert = _generate_ca(wrong_ca_key)

    _write_cert(d / "ca.crt", ca_cert)
    _write_key(d / "ca.key", ca_key)
    _write_cert(d / "server.crt", server_cert)
    _write_key(d / "server.key", server_key)
    _write_cert(d / "client.crt", client_cert)
    _write_key(d / "client.key", client_key)
    _write_cert(d / "expired_client.crt", expired_client_cert)
    _write_key(d / "expired_client.key", expired_client_key)
    _write_cert(d / "wrong_ca.crt", wrong_ca_cert)

    return d


@pytest.fixture(scope="module")
def tls_cluster_mutual(
    certs_dir: Path,
) -> Generator[list[tuple[str, int]], None, None]:
    """ScyllaDB cluster with mutual TLS (server verifies client cert)."""
    cluster = create_scylla_cluster(
        name="tls_mutual",
        scylla_version=SCYLLA_VERSION,
        nodes=1,
        ipprefix="127.0.1.",
        config={
            "client_encryption_options": {
                "enabled": True,
                "require_client_auth": True,
                "certificate": str(certs_dir / "server.crt"),
                "keyfile": str(certs_dir / "server.key"),
                "truststore": str(certs_dir / "ca.crt"),
            }
        },
    )
    try:
        start_cluster(cluster)
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


@pytest.fixture(scope="module")
def tls_cluster_server_only(
    certs_dir: Path,
) -> Generator[list[tuple[str, int]], None, None]:
    """ScyllaDB cluster with server-side TLS only (no client cert required)."""
    cluster = create_scylla_cluster(
        name="tls_server_only",
        scylla_version=SCYLLA_VERSION,
        nodes=1,
        ipprefix="127.0.2.",
        config={
            "client_encryption_options": {
                "enabled": True,
                "require_client_auth": False,
                "certificate": str(certs_dir / "server.crt"),
                "keyfile": str(certs_dir / "server.key"),
            }
        },
    )
    try:
        start_cluster(cluster)
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


@pytest.fixture(scope="module")
def tls_cluster_wrong_san(
    certs_dir: Path,
) -> Generator[list[tuple[str, int]], None, None]:
    """ScyllaDB cluster on an IP NOT in the server cert's SAN.

    The server cert only has SANs for 127.0.0.1, 127.0.1.1, 127.0.2.1.
    This cluster runs on 127.0.3.1, so hostname verification should fail.
    """
    cluster = create_scylla_cluster(
        name="tls_wrong_san",
        scylla_version=SCYLLA_VERSION,
        nodes=1,
        ipprefix="127.0.3.",
        config={
            "client_encryption_options": {
                "enabled": True,
                "require_client_auth": False,
                "certificate": str(certs_dir / "server.crt"),
                "keyfile": str(certs_dir / "server.key"),
            }
        },
    )
    try:
        start_cluster(cluster)
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_tls_server_auth_only(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connect to a TLS-enabled cluster verifying the server's certificate.

    What this proves:
    - load_verify_locations() correctly loads the CA into the SslConfig
    - The OpenSSL context built from SslConfig at connect time is valid
    - The TLS handshake completes successfully through the Rust driver

    If this fails:
    - load_verify_locations() is silently ignored
    - OR the CA cert wasn't properly passed into the OpenSSL context builder
    - OR the server's cert doesn't match the CA (cert generation issue)
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None, "Expected at least one row from system.local"


@pytest.mark.asyncio
async def test_tls_mutual_auth(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connect with mutual TLS (client presents a certificate).

    What this proves:
    - load_cert_chain() correctly loads the client cert + key into SslConfig
    - Server accepts our client cert (signed by the same CA)
    - The full mutual TLS handshake works end-to-end

    If this fails:
    - load_cert_chain() is silently ignored
    - OR the client cert/key weren't passed into the OpenSSL context builder
    - OR the server doesn't trust our client cert's CA
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.load_cert_chain(certs_dir / "client.crt", certs_dir / "client.key")
    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None


@pytest.mark.asyncio
async def test_tls_wrong_ca_rejects_connection(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connection should FAIL when we use a CA that didn't sign the server's cert.

    What this proves:
    - verify_mode=CERT_REQUIRED is actually enforced by the built OpenSSL context
    - The Rust driver correctly propagates the TLS handshake failure

    If this PASSES (no exception):
    - CRITICAL: verify_mode is not being respected
    - The SslConfig.verify_mode setter is not propagating into the context builder
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "wrong_ca.crt")
    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "verify",
            "ssl",
            "tls",
            "handshake",
            "broken",
            "channel",
            "eof",
            "connection",
        ]
    ), f"Expected a TLS/connection error, got: {exc_info.value}"


@pytest.mark.asyncio
async def test_tls_check_hostname_succeeds_with_correct_san(
    tls_cluster_server_only: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connection succeeds when check_hostname=True and server cert SAN matches.

    What this proves:
    - check_hostname=True is propagated through SslConfig into the context
    - The server cert's SAN (which includes the cluster IP) passes verification
    - Hostname verification doesn't accidentally break valid connections

    If this fails:
    - check_hostname=True causes false negatives (even for correct SANs)
    - The SslConfig.check_hostname setter isn't wired up correctly
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.check_hostname = True
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None


@pytest.mark.asyncio
async def test_tls_verifies_hostname_rejects_wrong_san(
    tls_cluster_wrong_san: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connection should FAIL when the server cert SAN doesn't include the node's IP.

    What this proves:
    - Hostname/IP verification is enforced through SslConfig
    - The driver rejects connections where the server certificate doesn't match
      the actual connection IP address

    If this PASSES (no exception):
    - Hostname verification is not being enforced
    - check_hostname setting isn't propagated through SslConfig
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.check_hostname = True
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    # The cluster is on 127.0.3.1 but the server cert only has SANs for
    # 127.0.0.1, 127.0.1.1, 127.0.2.1 — hostname verification should fail.
    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_wrong_san).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "hostname",
            "ssl",
            "tls",
            "handshake",
            "verify",
            "broken",
            "channel",
            "eof",
            "connection",
            "mismatch",
        ]
    ), f"Expected a hostname verification error, got: {exc_info.value}"


@pytest.mark.asyncio
async def test_tls_no_client_cert_rejected_by_mutual_tls(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connection should FAIL when server requires client cert but we don't provide one.

    What this proves:
    - The server-side mutual TLS enforcement works
    - SslConfig without load_cert_chain() correctly does NOT present a client cert

    If this PASSES (no exception):
    - The SslConfig somehow carries a stale client cert from another instance
    - Or the server isn't actually requiring client auth
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")

    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "ssl",
            "tls",
            "handshake",
            "alert",
            "broken",
            "channel",
            "eof",
            "connection",
        ]
    ), f"Expected a connection/TLS error, got: {exc_info.value}"


@pytest.mark.asyncio
async def test_tls_expired_client_cert_rejected(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Connection should FAIL when we present an expired client certificate.

    What this proves:
    - The server validates the client cert's validity period
    - Certificate data loaded via load_cert_chain() is faithfully transmitted
      through the SslConfig builder to the Rust driver

    If this PASSES (no exception):
    - The server isn't validating cert expiry (unlikely for ScyllaDB)
    - Or a valid cert is somehow used instead of the expired one
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.load_cert_chain(
        certs_dir / "expired_client.crt",
        certs_dir / "expired_client.key",
    )

    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    with pytest.raises(Exception) as exc_info:
        await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    error_msg = str(exc_info.value).lower()
    assert any(
        keyword in error_msg
        for keyword in [
            "certificate",
            "expire",
            "ssl",
            "tls",
            "handshake",
            "alert",
            "broken",
            "channel",
            "eof",
            "connection",
        ]
    ), f"Expected a certificate expiry/TLS error, got: {exc_info.value}"


@pytest.mark.asyncio
async def test_tls_no_verify_connects(
    tls_cluster_server_only: list[tuple[str, int]],
) -> None:
    """Connect with verify_mode=CERT_NONE (skip server cert verification).

    What this proves:
    - The verify_mode setter on SslConfig propagates correctly
    - Even without loading any CA, connection succeeds because we don't verify

    If this fails:
    - verify_mode=CERT_NONE isn't being respected by the context builder
    - Or the driver adds its own verification on top of SslConfig
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_NONE
    # Deliberately NOT loading any CA certs — should still connect

    session = await SessionBuilder().contact_points(tls_cluster_server_only).tls_context(tls).connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None


@pytest.mark.asyncio
async def test_tls_query_data_integrity(
    tls_cluster_mutual: list[tuple[str, int]],
    certs_dir: Path,
) -> None:
    """Execute INSERT + SELECT over TLS to verify data integrity.

    What this proves:
    - The encrypted channel correctly transmits CQL data in both directions
    - Not just the handshake, but actual application-layer data works

    If this fails:
    - TLS handshake succeeded but the encrypted stream is corrupted
    - Could indicate an OpenSSL context built from SslConfig is misconfigured
      in a way that only manifests under real data transfer
    """
    tls = SslConfig(SslProtocol.TLS_CLIENT)
    tls.load_verify_locations(certs_dir / "ca.crt")
    tls.load_cert_chain(certs_dir / "client.crt", certs_dir / "client.key")

    tls.check_hostname = False
    tls.verify_mode = VerifyMode.CERT_REQUIRED

    session = await SessionBuilder().contact_points(tls_cluster_mutual).tls_context(tls).connect()

    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS tls_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    await session.execute("CREATE TABLE IF NOT EXISTS tls_test.data (id int PRIMARY KEY, value text)")
    await session.execute("INSERT INTO tls_test.data (id, value) VALUES (1, 'hello over TLS')")

    result = await session.execute("SELECT value FROM tls_test.data WHERE id = 1")
    row = await result.first_row()
    assert row is not None

    row_str = str(row)
    assert "hello over TLS" in row_str, f"Data integrity check failed, got: {row_str}"

    await session.execute("DROP KEYSPACE IF EXISTS tls_test")


def test_tls_context_snapshot_preservation_and_clearing(certs_dir: Path) -> None:
    cfg = SslConfig(SslProtocol.TLS_SERVER)
    cfg.verify_mode = VerifyMode.CERT_NONE
    cfg.check_hostname = False
    cfg.load_verify_locations(certs_dir / "ca.crt")

    builder = SessionBuilder().tls_context(cfg)
    snapshot = builder.get_config()

    assert snapshot.tls_context is not None
    assert snapshot.tls_context.protocol == SslProtocol.TLS_SERVER
    assert snapshot.tls_context.verify_mode == VerifyMode.CERT_NONE
    assert snapshot.tls_context.check_hostname is False

    cleared_snapshot = builder.tls_context(None).get_config()
    assert cleared_snapshot.tls_context is None


def test_nonexistent_ca_file_raises_session_config_error() -> None:
    bad_path = "/absolutely/does/not/exist/ca.crt"
    cfg = SslConfig()
    cfg.load_verify_locations(bad_path)

    with pytest.raises(SessionConfigError) as exc_info:
        SessionBuilder().tls_context(cfg)

    cause = exc_info.value.__cause__
    assert cause is not None, "Expected __cause__ to be set on SessionConfigError"
    assert isinstance(cause, TlsError), f"Expected TlsError as __cause__, got {type(cause).__name__}: {cause}"
    assert bad_path in str(cause), f"Expected path '{bad_path}' in error message, got: {cause}"
