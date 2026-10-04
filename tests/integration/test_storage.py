import hashlib
import os
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.config import Settings
from control_plane.datasets import DatasetService
from control_plane.schemas import NamedResource
from control_plane.services import DomainError
from control_plane.storage import BlobStore
from tests.helpers import assigned


def test_filesystem_verification_and_capacity(service, tmp_path):
    content = b"measurements\n1\n2\n"
    digest = hashlib.sha256(content).hexdigest()
    store = BlobStore(service.settings)
    store.put_bytes(digest, content)
    assert store.get_path(digest, len(content)).read_bytes() == content
    path = store.root / digest
    path.write_bytes(b"corrupt")
    with pytest.raises(DomainError, match="checksum"):
        store.get_path(digest)
    store.put_bytes(digest, content)
    assert store.get_path(digest).read_bytes() == content
    assert dict(store.inventory()) == {digest: len(content)}
    with pytest.raises(DomainError, match="digest"):
        store.get_path("../escape")
    source = tmp_path / "wrong"
    source.write_bytes(b"not the declared data")
    with pytest.raises(DomainError, match="checksum"):
        store.put_file(digest, source)
    with patch("control_plane.storage.shutil.disk_usage") as usage:
        usage.return_value.free = 1
        with pytest.raises(DomainError, match="capacity"):
            store.put_bytes(digest, content)


@pytest.fixture
def s3(service, monkeypatch):
    endpoint = os.getenv("STRATA_TEST_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("set STRATA_TEST_S3_ENDPOINT for a real S3-compatible service")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.getenv("STRATA_TEST_S3_KEY", "strata-test"))
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY", os.getenv("STRATA_TEST_S3_SECRET", "strata-test-secret")
    )
    service.settings.storage_backend = "s3"
    service.settings.s3_endpoint_url = endpoint
    service.settings.s3_bucket = f"strata-test-{uuid4().hex}"
    store = BlobStore(service.settings)
    store.client.create_bucket(Bucket=service.settings.s3_bucket)
    yield store
    for page in store.client.get_paginator("list_objects_v2").paginate(
        Bucket=service.settings.s3_bucket
    ):
        for row in page.get("Contents", []):
            store.client.delete_object(Bucket=service.settings.s3_bucket, Key=row["Key"])
    store.client.delete_bucket(Bucket=service.settings.s3_bucket)


def test_live_s3_datasets_artifacts_and_worker_inputs(service, s3):
    client = TestClient(create_app(service.settings, service))
    datasets = DatasetService(service)
    data = datasets.create(NamedResource(name="S3 transfer"))
    version = datasets.version(data.id, "v1")
    content = b"x,y\n1,2\n" + b"3,4\n" * (3 * 1024**2)
    row = datasets.upload(version.id, "data.csv", [content])
    datasets.seal(version.id)
    assert row.size == len(content) and row.sha256 == hashlib.sha256(content).hexdigest()
    assert not (s3.root / row.sha256).exists()  # Object bytes are authoritative on S3.
    response = client.get(f"/dataset-files/{row.id}")
    assert response.status_code == 200 and response.content == content
    (s3.root / row.sha256).unlink()
    from control_plane.inputs import input_path
    from control_plane.rpc import engine_pb2 as pb
    from tests.helpers import submit

    job = submit(service, inputs=[{"version_id": version.id, "alias": "measurements"}])
    _, worker, assignment = assigned(service, job=job)
    credentials = pb.AttemptRequest(
        attempt_id=assignment["attempt_id"],
        session_id=worker.session_id,
        lease_token=assignment["lease_token"],
    )
    path, size = input_path(service, credentials, row.sha256)
    assert size == len(content) and hashlib.sha256(path.read_bytes()).hexdigest() == row.sha256
    artifact = service.artifact(
        assignment["attempt_id"],
        worker.session_id,
        assignment["lease_token"],
        "result.json",
        "application/json",
        b'{"value":42}',
    )
    assert client.get(f"/artifacts/{artifact.id}").content == b'{"value":42}'
    inventory = dict(s3.inventory())
    assert inventory[row.sha256] == len(content) and inventory[artifact.sha256] == artifact.size


def test_live_s3_refuses_corrupt_remote_bytes(service, s3):
    content = b"original"
    digest = hashlib.sha256(content).hexdigest()
    s3.put_bytes(digest, content)
    s3.client.put_object(Bucket=service.settings.s3_bucket, Key=s3.key(digest), Body=b"modified")
    with pytest.raises(DomainError, match="checksum"):
        s3.get_path(digest, len(content))
    assert not (s3.root / digest).exists()
    assert not list(s3.root.glob("download-*"))


def test_live_s3_cache_eviction_preserves_pinned_reader_and_remote_data(service, s3):
    content = b"retained-pinned-data"
    digest = hashlib.sha256(content).hexdigest()
    s3.put_bytes(digest, content)
    with s3.materialized(digest, len(content)) as pinned:
        assert pinned.read_bytes() == content
        plan = s3.trim_cache(apply=False)
        assert plan["candidates"] == [digest] and (s3.root / digest).exists()
        assert s3.trim_cache()["candidates"] == [digest]
        assert not (s3.root / digest).exists()
        assert pinned.read_bytes() == content
        assert dict(s3.inventory()) == {digest: len(content)}
    assert not list(s3.root.glob("pin-*"))
    assert s3.get_path(digest).read_bytes() == content
    service.settings.storage_cache_bytes = 2
    s3.trim_cache()
    with pytest.raises(DomainError, match="cache budget"):
        s3.get_path(digest)


def test_live_s3_campaign_results_materialize_uncached_outputs(service, s3):
    from control_plane.campaigns import CampaignService
    from control_plane.schemas import CampaignSubmit
    from tests.helpers import complete

    campaigns = CampaignService(service)
    campaign, _ = campaigns.create(
        CampaignSubmit(
            name="Remote results",
            template={
                "name": "simulation",
                "image": "strata/python-workloads:local",
                "command": ["run"],
            },
            matrix={"seed": [1]},
        ),
        None,
    )
    _, worker, assignment = assigned(service, job=campaigns.jobs(campaign.id)[0])
    artifact = service.artifact(
        assignment["attempt_id"],
        worker.session_id,
        assignment["lease_token"],
        "result.json",
        "application/json",
        b'{"value":42}',
    )
    complete(service, worker, assignment)
    assert not (s3.root / artifact.sha256).exists()
    assert campaigns.results(campaign.id)[0]["result.value"] == 42
    assert not list(s3.root.glob("pin-*"))


def test_s3_settings_require_bucket_and_production_https():
    with pytest.raises(ValueError, match="bucket"):
        Settings(storage_backend="s3")
    with pytest.raises(ValueError, match="HTTPS"):
        Settings(
            storage_backend="s3",
            s3_bucket="data",
            s3_endpoint_url="http://storage",
            production=True,
        )


def test_live_s3_postgres_backup_and_restore(service, s3, tmp_path):
    from sqlalchemy import create_engine, select, text
    from sqlalchemy.engine import make_url

    from control_plane.database import Base, make_engine, sessions
    from control_plane.maintenance import backup_store, restore_store, verify_store
    from control_plane.models import Admission, DatasetFile
    from control_plane.services import EngineService

    url = os.getenv("STRATA_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set STRATA_TEST_POSTGRES_URL for live backup/restore")
    # These databases are uniquely created by this test and always removed afterwards.
    names = ["strata_backup_" + uuid4().hex, "strata_restore_" + uuid4().hex]
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    engines = []
    with admin.connect() as connection:
        for name in names:
            connection.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        source = service.settings.model_copy(
            update={
                "database_url": make_url(url)
                .set(database=names[0])
                .render_as_string(hide_password=False),
            }
        )
        engine = make_engine(source.database_url)
        engines.append(engine)
        Base.metadata.create_all(engine)
        factory = sessions(engine)
        with factory.begin() as session:
            session.add(Admission(id=1))
        datasets = DatasetService(EngineService(factory, source))
        data = datasets.create(NamedResource(name="Restore evidence"))
        version = datasets.version(data.id, "v1")
        row = datasets.upload(version.id, "data.csv", [b"x,y\n1,2\n"])
        datasets.seal(version.id)
        backup = tmp_path / "backup"
        record = backup_store(source, backup)
        assert verify_store(backup) == record
        destination = source.model_copy(
            update={
                "database_url": make_url(url)
                .set(database=names[1])
                .render_as_string(hide_password=False),
                "artifact_root": tmp_path / "restored-cache",
                "s3_prefix": "restore-" + uuid4().hex,
            }
        )
        restore_store(backup, destination)
        engine2 = make_engine(destination.database_url)
        engines.append(engine2)
        with sessions(engine2)() as session:
            restored = session.scalar(select(DatasetFile).where(DatasetFile.id == row.id))
            assert restored.sha256 == row.sha256
        assert BlobStore(destination).get_path(row.sha256).read_bytes() == b"x,y\n1,2\n"
        with pytest.raises(ValueError, match="empty"):
            restore_store(backup, destination)
    finally:
        for engine in engines:
            engine.dispose()
        with admin.connect() as connection:
            for name in names:
                connection.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()
