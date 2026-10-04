"""Generate short-lived, synthetic credentials for the private Docker failover lab."""

import argparse
import ipaddress
import json
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def generate(root: Path) -> None:
    if root.exists() and any(root.iterdir()):
        raise ValueError("credential destination must be empty; existing keys are never replaced")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Strata private failover lab")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(issuer)
        .issuer_name(issuer)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=7))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, True, True, None, None), True
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_bytes = ca.public_bytes(serialization.Encoding.PEM)

    def certificate(name: str, server: bool):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(days=7))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
            )
            .add_extension(
                x509.KeyUsage(True, False, True, False, False, False, False, None, None), True
            )
            .add_extension(
                x509.ExtendedKeyUsage(
                    [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
                    if server
                    else [ExtendedKeyUsageOID.CLIENT_AUTH]
                ),
                True,
            )
        )
        if server:
            builder = builder.add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName(name),
                        x509.DNSName("postgres-writer"),
                        x509.DNSName("localhost"),
                        x509.DNSName("host.docker.internal"),
                        x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    ]
                ),
                False,
            )
        cert = builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM)
        private = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        return cert, private

    client_cert, client_key = certificate("strata-ha-client", False)
    for name in ("pg-a", "pg-b", "pg-c", "etcd-a", "etcd-b", "etcd-c"):
        directory = root / "tls" / name
        # Only the credential root is host-private. Mounted children must be readable by
        # the unprivileged postgres/haproxy UIDs; PostgreSQL copies its key to mode 0600.
        directory.mkdir(parents=True, mode=0o755)
        cert, key = certificate(name, True)
        for filename, data in {
            "ca.pem": ca_bytes,
            "server.pem": cert,
            "server.key": key,
            "client.pem": client_cert,
            "client.key": client_key,
        }.items():
            path = directory / filename
            path.write_bytes(data)
            path.chmod(0o644)
    gateway = root / "tls" / "gateway"
    gateway.mkdir(mode=0o755)
    (gateway / "ca.pem").write_bytes(ca_bytes)
    health = gateway / "health.pem"
    health.write_bytes(client_cert + client_key)
    health.chmod(0o644)
    secret_file = root / "secrets.json"
    secret_file.write_text(
        json.dumps(
            {
                name: secrets.token_urlsafe(36)
                for name in ("superuser", "replication", "application", "rest")
            }
        )
    )
    secret_file.chmod(0o644)
    # The disposable CA private key is deliberately not persisted.


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/ha-lab"))
    args = parser.parse_args()
    generate(args.output)
    print(f"Private seven-day lab credentials created in {args.output.resolve()}")


if __name__ == "__main__":
    main()
