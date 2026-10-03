import ipaddress
import socket
from datetime import UTC, datetime, timedelta

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc.server import make_server
from worker.transport import Transport


def certificates(root):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=2))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.DNSName("rpc"),
                    x509.DNSName("tls-rpc"),
                    x509.DNSName("host.docker.internal"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = root / "certificate.pem", root / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def test_real_tls_channel_trust_and_bearer_authentication(service, tmp_path, monkeypatch):
    cert, key = certificates(tmp_path)
    service.settings.tls_cert, service.settings.tls_key = cert, key
    with socket.socket() as socket_:
        socket_.bind(("127.0.0.1", 0))
        port = socket_.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    monkeypatch.setenv("STRATA_RPC_CA", str(cert))
    transport = Transport(f"localhost:{port}", service.settings.worker_token)
    try:
        reply = transport.call(
            "Register", pb.RegisterRequest(worker_id="tls-worker", cpu_total=1, memory_total_mb=128)
        )
        assert reply.session_id
        transport.token = "invalid"
        with pytest.raises(grpc.RpcError) as exc:
            transport.call(
                "Poll", pb.PollRequest(worker_id="tls-worker", session_id=reply.session_id)
            )
        assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED
        monkeypatch.delenv("STRATA_RPC_CA")
        insecure = Transport(f"localhost:{port}", service.settings.worker_token)
        with pytest.raises(grpc.RpcError):
            insecure.call(
                "Register", pb.RegisterRequest(worker_id="wrong", cpu_total=1, memory_total_mb=128)
            )
        insecure.channel.close()
    finally:
        transport.channel.close()
        server.stop(0).wait()
