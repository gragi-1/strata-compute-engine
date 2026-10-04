"""Production coordinator boundaries fail before starting an insecure replica."""

import pytest

from control_plane.config import Settings
from control_plane.coordinator import Coordinator, ReplicaSettings


@pytest.mark.parametrize(
    ("database", "storage", "message"),
    [
        ("sqlite:///development.db", "s3", "PostgreSQL"),
        ("postgresql+psycopg://localhost/strata", "s3", "verify-full"),
        ("postgresql+psycopg://localhost/strata?sslmode=require", "s3", "verify-full"),
        ("postgresql+psycopg://localhost/strata?sslmode=verify-full", "filesystem", "shared S3"),
    ],
)
def test_production_replica_rejects_unverified_or_unshared_state(
    database, storage, message, tmp_path
):
    settings = Settings(
        production=True,
        identity_enabled=True,
        worker_token="synthetic-strong-worker-secret-32",
        tls_cert=tmp_path / "rpc.pem",
        tls_key=tmp_path / "rpc.key",
        database_url=database,
        storage_backend=storage,
        s3_bucket="synthetic-private-bucket",
        artifact_root=tmp_path / "artifacts",
    )
    with pytest.raises(ValueError, match=message):
        Coordinator(settings, ReplicaSettings())
