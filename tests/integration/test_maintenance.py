import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url

from control_plane.config import Settings
from control_plane.datasets import DatasetService
from control_plane.maintenance import (
    backup_store,
    checksum,
    postgres_environment,
    restore_store,
    verify_store,
)
from control_plane.models import Dataset
from control_plane.schemas import NamedResource


@pytest.mark.postgres
def test_consistent_backup_verified_restore_and_refusal_to_overwrite(postgres_service, tmp_path):
    svc = postgres_service
    ds = DatasetService(svc)
    row = ds.create(NamedResource(name="Recoverable data"))
    version = ds.version(row.id, "v1")
    file = ds.upload(version.id, "observations.csv", [b"x,y\n1,2\n"])
    ds.seal(version.id)
    schema = svc.factory.kw["bind"].url  # The fixture configures a private PostgreSQL schema.
    with svc.factory() as session:
        actual = session.scalar(text("SELECT current_schema()"))
    source = make_url(svc.settings.database_url).update_query_dict(
        {"options": f"-csearch_path={actual}"}
    )
    config = Settings(
        database_url=source.render_as_string(hide_password=False),
        artifact_root=svc.settings.artifact_root,
    )
    output = tmp_path / "backup"
    record = backup_store(config, output)
    assert record["blobs"] == {file.sha256: len(b"x,y\n1,2\n")}
    assert verify_store(output) == record
    with pytest.raises(ValueError, match="new directory"):
        backup_store(config, output)
    target_name = "strata_restore_" + uuid4().hex
    admin = create_engine(
        make_url(config.database_url).set(database="postgres").update_query_dict({"options": ""}),
        isolation_level="AUTOCOMMIT",
    )
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{target_name}"'))
    target = Settings(
        database_url=make_url(config.database_url)
        .set(database=target_name)
        .render_as_string(hide_password=False),
        artifact_root=tmp_path / "restored",
    )
    try:
        restore_store(output, target)
        engine = create_engine(target.database_url)
        with engine.connect() as connection:
            assert (
                connection.scalar(select(Dataset.name).where(Dataset.id == row.id))
                == "Recoverable data"
            )
        engine.dispose()
        assert checksum(target.artifact_root / file.sha256) == file.sha256
        with pytest.raises(ValueError, match="empty"):
            restore_store(output, target)
        empty_root = tmp_path / "empty"
        with pytest.raises(ValueError, match="database must be empty"):
            restore_store(output, target.model_copy(update={"artifact_root": empty_root}))
        (output / "blobs" / file.sha256).write_bytes(b"corrupt")
        with pytest.raises(ValueError, match="checksum mismatch"):
            verify_store(output)
    finally:
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{target_name}"'))
        admin.dispose()
    assert schema.database


def test_backup_manifest_and_database_integrity(tmp_path):
    with pytest.raises(ValueError):
        postgres_environment("sqlite:///test.db")
    (tmp_path / "manifest.json").write_text(json.dumps({"format": 2}))
    with pytest.raises(ValueError, match="unsupported"):
        verify_store(tmp_path)
    (tmp_path / "database.dump").write_bytes(b"dump")
    record = {"format": 1, "database_sha256": "wrong", "blobs": {}}
    (tmp_path / "manifest.json").write_text(json.dumps(record))
    with pytest.raises(ValueError, match="database dump"):
        verify_store(tmp_path)
    record["database_sha256"] = checksum(tmp_path / "database.dump")
    record["blobs"] = {"../traversal": 3}
    (tmp_path / "manifest.json").write_text(json.dumps(record))
    with pytest.raises(ValueError, match="invalid backup blob"):
        verify_store(tmp_path)
