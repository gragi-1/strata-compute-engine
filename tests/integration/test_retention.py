import hashlib
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch

import pytest
from sqlalchemy import select

from control_plane.datasets import DatasetService
from control_plane.models import DatasetFile, UploadPart, UploadSession
from control_plane.retention import RetentionService
from control_plane.schemas import NamedResource
from control_plane.services import DomainError
from control_plane.storage import BlobStore
from control_plane.uploads import UploadService


def old_blob(svc, content):
    digest = hashlib.sha256(content).hexdigest()
    store = BlobStore(svc.settings)
    store.put_bytes(digest, content)
    when = svc.clock().timestamp() - 2 * svc.settings.blob_retention_seconds
    os.utime(store.root / digest, (when, when))
    return digest


def test_collection_is_explicit_bounded_and_preserves_references(service):
    datasets = DatasetService(service)
    dataset = datasets.create(NamedResource(name="Preserved measurements"))
    version = datasets.version(dataset.id, "v1")
    row = datasets.upload(version.id, "data.csv", [b"x\n1\n"])
    old_blob(service, b"x\n1\n")
    orphan = old_blob(service, b"orphan")
    collector = RetentionService(service)
    plan = collector.collect(limit=1)
    assert plan["dry_run"] and plan["candidates"] == [orphan]
    assert (service.settings.artifact_root / orphan).is_file()
    result = collector.collect(apply=True, limit=1)
    assert result["deleted"] == [orphan]
    assert datasets.file(row.id)[1].read_bytes() == b"x\n1\n"
    assert collector.collect(apply=True)["deleted"] == []
    with pytest.raises(DomainError):
        collector.collect(limit=0)


def test_expired_transfer_history_releases_chunks_but_keeps_completed_data(service):
    datasets = DatasetService(service)
    dataset = datasets.create(NamedResource(name="Upload retention"))
    version = datasets.version(dataset.id, "v1")
    uploads = UploadService(service)
    first = uploads.create(version.id, "complete.csv", 4, None)
    uploads.chunk(first.id, 0, b"x\n1\n", hashlib.sha256(b"x\n1\n").hexdigest())
    file = uploads.complete(first.id)
    abandoned = uploads.create(version.id, "abandoned.csv", 6, None)
    orphan = hashlib.sha256(b"unused").hexdigest()
    uploads.chunk(abandoned.id, 0, b"unused", orphan)
    service.clock.advance(
        service.settings.upload_lifetime_seconds + service.settings.history_retention_seconds + 1
    )
    collector = RetentionService(service)
    assert collector.history()["uploads"] == 2
    with service.factory() as session:
        assert session.get(UploadSession, first.id) is not None
    assert collector.history(apply=True)["uploads"] == 2
    with service.factory() as session:
        assert session.get(DatasetFile, file.id) is not None
        assert list(session.scalars(select(UploadPart))) == []
    old_blob(service, b"unused")
    assert collector.collect(apply=True)["deleted"] == [orphan]
    assert datasets.file(file.id)[1].read_bytes() == b"x\n1\n"


def test_local_storage_budget_is_atomic_for_concurrent_writes(service):
    service.settings.storage_max_local_bytes = 1500
    store = BlobStore(service.settings)

    def put(index):
        data = bytes([index]) * 600
        try:
            store.put_bytes(hashlib.sha256(data).hexdigest(), data)
            return True
        except DomainError as exc:
            assert exc.code == 503 and "budget" in str(exc)
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(put, range(4)))
    assert sum(results) == 1  # A second write needs 600 staging + 600 durable bytes.
    assert sum(size for _, size in store.inventory()) == 600


def test_temporary_reaper_preserves_active_lifetimes_and_reaps_process_crashes(service):
    store = BlobStore(service.settings)
    with store.temporary("assembly-", directory=True) as path:
        (path / "content").write_bytes(b"active assembly")
        lease = store.root / (".lease-" + path.name)
        old = service.clock().timestamp() - 2 * service.settings.blob_retention_seconds
        os.utime(lease, (old, old))
        report = store.collect_temporary(apply=True, now=service.clock())
        assert report["active_skipped"] == 1 and report["deleted"] == []
        assert (path / "content").read_bytes() == b"active assembly"
    assert not path.exists() and not lease.exists()
    # os._exit simulates a killed owner: finally blocks do not execute, OS locks release.
    program = """
import os, sys
from pathlib import Path
from control_plane.config import Settings
from control_plane.storage import BlobStore
store = BlobStore(Settings(artifact_root=Path(sys.argv[1])))
with store.temporary("staging-", directory=True) as path:
    (path / "content").write_bytes(b"crash evidence")
    print(path.name, flush=True)
    os._exit(0)
"""
    process = subprocess.run(
        [sys.executable, "-c", program, str(store.root)], check=True, capture_output=True, text=True
    )
    name = process.stdout.strip()
    lease = store.root / (".lease-" + name)
    os.utime(lease, (old, old))
    assert store.collect_temporary(now=service.clock())["candidates"] == [name]
    assert (store.root / name / "content").is_file()
    assert store.collect_temporary(apply=True, now=service.clock())["deleted"] == [name]
    assert not (store.root / name).exists() and not lease.exists()
    # Unmanaged paths and authoritative hash names are outside this operation.
    unmanaged = store.root / "assembly-user-directory"
    unmanaged.mkdir()
    assert store.collect_temporary(apply=True, now=service.clock())["deleted"] == []
    assert unmanaged.is_dir()


def test_temporary_cleanup_refuses_to_follow_links_or_external_paths(service, tmp_path):
    store = BlobStore(service.settings)
    outside = tmp_path / "outside"
    outside.mkdir()
    with pytest.raises(DomainError, match="external"):
        store.remove_temporary(outside)
    assert outside.is_dir()


@pytest.mark.postgres
def test_collector_waits_for_new_blob_reference_to_commit(postgres_service):
    svc = postgres_service
    datasets = DatasetService(svc)
    dataset = datasets.create(NamedResource(name="Concurrent collection"))
    version = datasets.version(dataset.id, "v1")
    written, release, collecting = Event(), Event(), Event()
    put_file = BlobStore.put_file

    def paused_write(store, digest, source):
        put_file(store, digest, source)
        when = svc.clock().timestamp() - 2 * svc.settings.blob_retention_seconds
        os.utime(store.root / digest, (when, when))
        written.set()
        assert release.wait(10)

    def collect():
        collecting.set()
        return RetentionService(svc).collect(apply=True)

    with patch.object(BlobStore, "put_file", paused_write), ThreadPoolExecutor(2) as pool:
        upload = pool.submit(datasets.upload, version.id, "data.csv", [b"x\n1\n"])
        assert written.wait(10)
        collection = pool.submit(collect)
        try:
            assert collecting.wait(5)
            with svc.factory() as session:
                session.execute(select(1))
            assert not collection.done()
        finally:
            release.set()
        file = upload.result(timeout=10)
        assert collection.result(timeout=10)["deleted"] == []
    assert datasets.file(file.id)[1].read_bytes() == b"x\n1\n"
