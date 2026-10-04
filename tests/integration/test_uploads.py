import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from control_plane.api import create_app
from control_plane.datasets import DatasetService
from control_plane.models import DatasetFile, UploadPart
from control_plane.schemas import NamedResource
from control_plane.services import DomainError
from control_plane.uploads import UploadService
from tests.integration.test_identity import headers, prepare


def draft(service):
    datasets = DatasetService(service)
    data = datasets.create(NamedResource(name="Resumable evidence"))
    return datasets.version(data.id, "v1")


def test_resume_after_service_restart_and_replay_chunks(service):
    version = draft(service)
    service.settings.upload_chunk_bytes = 65536
    content = b"a" * 65536 + b"b" * 123
    sha = hashlib.sha256(content).hexdigest()
    uploads = UploadService(service)
    row = uploads.create(version.id, "data.csv", len(content), sha)
    chunk = content[:65536]
    digest = hashlib.sha256(chunk).hexdigest()
    uploads.chunk(row.id, 0, chunk, digest)
    restarted = UploadService(service)
    assert restarted.get(row.id).received_bytes == 65536
    assert restarted.chunk(row.id, 0, chunk, digest).received_bytes == 65536
    with pytest.raises(DomainError, match="different content"):
        restarted.chunk(row.id, 0, b"c" * 65536, hashlib.sha256(b"c" * 65536).hexdigest())
    last = content[65536:]
    restarted.chunk(row.id, 65536, last, hashlib.sha256(last).hexdigest())
    file = restarted.complete(row.id)
    assert restarted.complete(row.id).id == file.id
    assert restarted.chunk(row.id, 0, chunk, digest).file_id == file.id
    assert file.sha256 == sha
    assert DatasetService(service).file(file.id)[1].read_bytes() == content
    assert DatasetService(service).seal(version.id).status == "SEALED"
    with service.factory() as session:
        assert session.scalar(select(func.count()).select_from(DatasetFile)) == 1
        assert session.scalar(select(func.count()).select_from(UploadPart)) == 2


def test_incomplete_checksum_failure_expiry_cancellation_and_zero_byte_files(service):
    uploads = UploadService(service)
    version = draft(service)
    row = uploads.create(version.id, "data.csv", 3, "0" * 64)
    with pytest.raises(DomainError, match="incomplete"):
        uploads.complete(row.id)
    with pytest.raises(DomainError, match="checksum"):
        uploads.chunk(row.id, 0, b"123", "1" * 64)
    assert uploads.get(row.id).received_bytes == 0
    uploads.chunk(row.id, 0, b"123", hashlib.sha256(b"123").hexdigest())
    with pytest.raises(DomainError, match="checksum"):
        uploads.complete(row.id)
    assert DatasetService(service).validate_upload(version.id) is None
    with pytest.raises(DomainError, match="open uploads"):
        DatasetService(service).seal(version.id)
    uploads.cancel(row.id)
    assert uploads.get(row.id).status == "CANCELLED"
    expired = uploads.create(version.id, "data.csv", 3, None)
    service.clock.advance(service.settings.upload_lifetime_seconds)
    with pytest.raises(DomainError, match="expired"):
        uploads.chunk(expired.id, 0, b"123", hashlib.sha256(b"123").hexdigest())
    fresh = uploads.create(version.id, "data.csv", 0, hashlib.sha256(b"").hexdigest())
    assert fresh.id != expired.id
    assert uploads.complete(fresh.id).size == 0
    with pytest.raises(DomainError, match="completed"):
        uploads.cancel(fresh.id)


def test_chunk_http_bounds_and_order(service):
    service.settings.upload_chunk_bytes = 65536
    row = UploadService(service).create(draft(service).id, "data.csv", 65537, None)
    client = TestClient(create_app(service.settings, service))
    assert (
        client.put(
            f"/uploads/{row.id}/chunks/0",
            content=b"a" * 65537,
            headers={"X-Chunk-SHA256": "0" * 64},
        ).status_code
        == 413
    )
    digest = hashlib.sha256(b"a").hexdigest()
    assert (
        client.put(
            f"/uploads/{row.id}/chunks/65536", content=b"a", headers={"X-Chunk-SHA256": digest}
        ).status_code
        == 409
    )
    assert (
        client.put(
            f"/uploads/{row.id}/chunks/1", content=b"a", headers={"X-Chunk-SHA256": digest}
        ).status_code
        == 422
    )
    assert client.get("/uploads").json()[0]["received_bytes"] == 0


def test_upload_privacy_and_reserved_storage_budget(service):
    client, _, root, alpha, beta, _, tokens = prepare(service)
    try:
        auth = headers(tokens["alice"], alpha)
        client.patch(f"/projects/{alpha}", headers=headers(root), json={"storage_limit_bytes": 5})
        data = client.post("/datasets", headers=auth, json={"name": "Reserved quota"}).json()
        version = client.post(
            f"/datasets/{data['id']}/versions", headers=auth, json={"label": "v1"}
        ).json()
        path = f"/dataset-versions/{version['id']}/uploads"
        body = {
            "name": "first.csv",
            "total_bytes": 4,
            "sha256": hashlib.sha256(b"1234").hexdigest(),
        }
        row = client.post(path, headers=auth, json=body).json()
        assert client.post(path, headers=auth, json=body).json()["id"] == row["id"]
        assert (
            client.post(
                path, headers=auth, json={"name": "second.csv", "total_bytes": 4}
            ).status_code
            == 413
        )
        other = headers(root, beta)
        assert client.get(f"/uploads/{row['id']}", headers=other).status_code == 404
        assert client.get("/uploads", headers=other).json() == []
        assert client.delete(f"/uploads/{row['id']}", headers=other).status_code == 404
        assert (
            client.put(
                f"/uploads/{row['id']}/chunks/0",
                headers=auth | {"X-Chunk-SHA256": body["sha256"]},
                content=b"1234",
            ).status_code
            == 200
        )
        assert client.post(f"/uploads/{row['id']}/complete", headers=auth).status_code == 200
        assert (
            client.post(
                path, headers=auth, json={"name": "second.csv", "total_bytes": 4}
            ).status_code
            == 413
        )
    finally:
        client.close()


def test_postgres_concurrent_chunk_replay_records_one_part(postgres_service):
    uploads = UploadService(postgres_service)
    row = uploads.create(draft(postgres_service).id, "data.csv", 3, None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(
            pool.map(
                lambda _: uploads.chunk(row.id, 0, b"123", hashlib.sha256(b"123").hexdigest()),
                range(8),
            )
        )
    assert all(value.received_bytes == 3 for value in values)
    with postgres_service.factory() as session:
        assert session.scalar(select(func.count()).select_from(UploadPart)) == 1
    assert uploads.complete(row.id).size == 3
