"""Verified backups, storage inspection and explicit retention."""

import hashlib
import json
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import typer
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import Connection, make_url

from control_plane.config import Settings
from control_plane.models import Artifact, DatasetFile, UploadPart
from control_plane.storage import BlobStore

app = typer.Typer(help="Back up, verify and inspect Strata persistence.", no_args_is_help=True)


@app.command("migrate")
def migrate(check: bool = False) -> None:
    """Upgrade the configured database, or check it against the installed schema."""
    from control_plane.upgrade import upgrade

    upgrade(check=check)
    typer.echo("Schema matches installed models." if check else "Schema upgraded to head.")


@app.command("bootstrap-admin")
def bootstrap_admin(username: str, display_name: str = "") -> None:
    """Create the first administrator locally; refuses an existing account database."""
    from control_plane.database import make_engine, sessions
    from control_plane.identity import IdentityService
    from control_plane.services import EngineService

    password = typer.prompt("Initial password", hide_input=True, confirmation_prompt=True)
    config = Settings()
    engine = make_engine(config.database_url)
    try:
        user = IdentityService(EngineService(sessions(engine), config)).create_user(
            username, password, display_name, is_admin=True, bootstrap=True
        )
        typer.echo(f"Created platform administrator {user.username} ({user.id}).")
    finally:
        engine.dispose()


@app.command("adopt-legacy")
def adopt_legacy(project_id: str) -> None:
    """Move unowned v2 resources into a project while workers and schedulers are stopped."""
    from datetime import timedelta

    from sqlalchemy import func, update

    from control_plane.access import audit, scoped_key
    from control_plane.database import make_engine, sessions
    from control_plane.domain import ACTIVE
    from control_plane.models import (
        Admission,
        Campaign,
        Dataset,
        DatasetVersion,
        Experiment,
        ExperimentRun,
        Job,
        JobSchedule,
        Project,
        UploadSession,
        Worker,
        WorkflowExpansion,
    )
    from control_plane.services import EngineService

    config = Settings()
    engine = make_engine(config.database_url)
    svc = EngineService(sessions(engine), config)
    try:
        with svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            project = session.scalar(
                select(Project).where(Project.id == project_id).with_for_update()
            )
            if project is None or not project.enabled:
                raise ValueError("an enabled target project is required")
            now = svc.now(session)
            if session.scalar(select(func.count()).select_from(Job).where(Job.status.in_(ACTIVE))):
                raise ValueError("finish or recover all active jobs before adopting legacy data")
            if session.scalar(
                select(func.count())
                .select_from(Worker)
                .where(Worker.last_heartbeat > now - timedelta(seconds=config.worker_timeout))
            ):
                raise ValueError("stop workers and schedulers before adopting legacy data")
            counts = {}
            for model in (
                Job,
                Campaign,
                Dataset,
                DatasetVersion,
                DatasetFile,
                Artifact,
                UploadSession,
                Experiment,
                ExperimentRun,
                JobSchedule,
                WorkflowExpansion,
            ):
                rows = list(session.scalars(select(model).where(model.project_id.is_(None))))
                for row in rows:
                    if isinstance(row, JobSchedule):
                        row.enabled = False
                        row.last_error = "assign an individual owner before resuming"
                    if isinstance(row, Job | Campaign | ExperimentRun):
                        row.idempotency_key = scoped_key(row.idempotency_key, project_id)
                session.flush()
                session.execute(
                    update(model).where(model.project_id.is_(None)).values(project_id=project_id)
                )
                counts[model.__tablename__] = len(rows)
            audit(session, now, "LEGACY_RESOURCES_ADOPTED", project_id, project_id, counts=counts)
        typer.echo(json.dumps(counts, indent=2))
    finally:
        engine.dispose()


def checksum(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def postgres_environment(url: str) -> dict[str, str]:
    parsed = make_url(url)
    if not parsed.drivername.startswith("postgresql"):
        raise ValueError("maintenance requires PostgreSQL")
    extra = {
        env: str(parsed.query[key])
        for key, env in {
            "sslmode": "PGSSLMODE",
            "sslrootcert": "PGSSLROOTCERT",
            "options": "PGOPTIONS",
        }.items()
        if key in parsed.query
    }
    return (
        os.environ
        | extra
        | {
            "PGHOST": parsed.host or "localhost",
            "PGPORT": str(parsed.port or 5432),
            "PGUSER": parsed.username or "strata",
            "PGPASSWORD": parsed.password or "",
            "PGDATABASE": parsed.database or "strata",
        }
    )


def binary(name: str) -> str:
    found = shutil.which(name)
    windows = Path(r"C:\Program Files\PostgreSQL\17\bin") / f"{name}.exe"
    if found:
        return found
    if windows.is_file():
        return str(windows)
    raise ValueError(f"{name} is required (PostgreSQL client tools)")


@contextmanager
def backup_snapshot(engine: Engine) -> Iterator[Connection]:
    # Acquire the session lock before the REPEATABLE READ snapshot is established.
    # A transaction-level SELECT could capture a snapshot while waiting for collection.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        schema_key = connection.scalar(text("SELECT hashtext(current_schema())"))
        connection.execute(
            text("SELECT pg_advisory_lock_shared(:schema, 1398035009)"), {"schema": schema_key}
        )
        connection.commit()
        try:
            connection.execution_options(isolation_level="REPEATABLE READ")
            with connection.begin():
                yield connection
        finally:
            connection.execution_options(isolation_level="AUTOCOMMIT")
            connection.execute(
                text("SELECT pg_advisory_unlock_shared(:schema, 1398035009)"),
                {"schema": schema_key},
            )
            connection.commit()


def backup_store(config: Settings, output: Path) -> dict[str, Any]:
    if output.exists():
        raise ValueError("backup destination must be a new directory")
    output.mkdir(parents=True)
    blobs = output / "blobs"
    blobs.mkdir()
    engine = create_engine(config.database_url)
    try:
        with backup_snapshot(engine) as connection:
            snapshot = connection.execute(text("SELECT pg_export_snapshot()")).scalar_one()
            process = subprocess.Popen(
                [
                    binary("pg_dump"),
                    "--format=custom",
                    "--no-owner",
                    "--no-acl",
                    "--snapshot",
                    snapshot,
                    "--file",
                    str(output / "database.dump"),
                ],
                env=postgres_environment(config.database_url),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + config.backup_timeout_seconds
            try:
                while process.poll() is None:
                    dump = output / "database.dump"
                    if dump.exists() and dump.stat().st_size > config.backup_max_bytes:
                        raise ValueError("backup exceeds the configured byte budget")
                    if shutil.disk_usage(output).free < config.storage_min_free_bytes:
                        raise ValueError("backup destination has insufficient free space")
                    if time.monotonic() > deadline:
                        raise ValueError("database backup exceeded its execution deadline")
                    time.sleep(0.1)
                _, error = process.communicate()
                if process.returncode:
                    raise subprocess.CalledProcessError(process.returncode, "pg_dump", stderr=error)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
            hashes = (
                set(connection.scalars(select(Artifact.sha256)))
                | set(connection.scalars(select(DatasetFile.sha256)))
                | set(connection.scalars(select(UploadPart.sha256)))
            )
            manifest = {}
            copied = (output / "database.dump").stat().st_size
            if copied > config.backup_max_bytes:
                raise ValueError("backup exceeds the configured byte budget")
            for digest in sorted(hashes):
                with BlobStore(config).materialized(digest) as source:
                    if not source.is_file() or checksum(source) != digest:
                        raise ValueError(f"missing or corrupt referenced blob: {digest}")
                    size = source.stat().st_size
                    copied += size
                    if copied > config.backup_max_bytes:
                        raise ValueError("backup exceeds the configured byte budget")
                    if shutil.disk_usage(output).free < size + config.storage_min_free_bytes:
                        raise ValueError("backup destination has insufficient free space")
                    shutil.copyfile(source, blobs / digest)
                    manifest[digest] = source.stat().st_size
            record = {
                "format": 1,
                "created_at": datetime.now(UTC).isoformat(),
                "blobs": manifest,
                "database_sha256": checksum(output / "database.dump"),
                "database_bytes": (output / "database.dump").stat().st_size,
            }
            (output / "manifest.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
            return record
    finally:
        engine.dispose()


def verify_store(output: Path) -> dict[str, Any]:
    record: dict[str, Any] = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    if record.get("format") != 1:
        raise ValueError("unsupported backup format")
    if checksum(output / "database.dump") != record["database_sha256"]:
        raise ValueError("database dump checksum mismatch")
    import re

    for digest, size in record["blobs"].items():
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid backup blob name")
        path = output / "blobs" / digest
        if not path.is_file() or path.stat().st_size != size or checksum(path) != digest:
            raise ValueError(f"backup blob checksum mismatch: {digest}")
    return record


def restore_store(output: Path, config: Settings) -> None:
    record = verify_store(output)
    if config.artifact_root.exists() and any(config.artifact_root.iterdir()):
        raise ValueError("restore artifact directory must be empty")
    store = BlobStore(config)
    if store.remote and next(store.inventory(), None) is not None:
        raise ValueError("restore object storage prefix must be empty")
    engine = create_engine(config.database_url)
    try:
        with engine.connect() as connection:
            count = connection.execute(
                text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
                )
            ).scalar_one()
            if count:
                raise ValueError("restore database must be empty")
        # A single restore transaction makes SQL failures roll back. Existing databases are refused.
        subprocess.run(
            [
                binary("pg_restore"),
                "--single-transaction",
                "--exit-on-error",
                "--no-owner",
                "--no-acl",
                "--dbname",
                make_url(config.database_url).database or "strata",
                str(output / "database.dump"),
            ],
            env=postgres_environment(config.database_url),
            check=True,
            capture_output=True,
        )
        config.artifact_root.mkdir(parents=True, exist_ok=True)
        for digest in record["blobs"]:
            store.put_file(digest, output / "blobs" / digest)
    finally:
        engine.dispose()


@app.command("backup")
def backup_command(output: Path) -> None:
    typer.echo(json.dumps(backup_store(Settings(), output), indent=2))


@app.command("verify-backup")
def verify_command(output: Path) -> None:
    record = verify_store(output)
    typer.echo(f"Verified {len(record['blobs'])} immutable blobs and database dump")


@app.command("restore")
def restore_command(backup: Path) -> None:
    """Restore into an explicitly configured EMPTY database and artifact directory."""
    restore_store(backup, Settings())
    typer.echo("Restore completed; keep workers stopped until recovery has expired old sessions.")


@app.command("storage-audit")
def storage_audit() -> None:
    config = Settings()
    engine = create_engine(config.database_url)
    try:
        with engine.connect() as connection:
            hashes = (
                set(connection.scalars(select(Artifact.sha256)))
                | set(connection.scalars(select(DatasetFile.sha256)))
                | set(connection.scalars(select(UploadPart.sha256)))
            )
        inventory = dict(BlobStore(config).inventory())
        missing = sorted(hashes - inventory.keys())
        unreferenced = sorted(inventory.keys() - hashes)
        typer.echo(
            json.dumps(
                {
                    "referenced_blobs": len(hashes),
                    "missing": missing,
                    "unreferenced": unreferenced,
                    "action": "inspection only; no files deleted",
                },
                indent=2,
            )
        )
    finally:
        engine.dispose()


def retention_command(operation: str, apply: bool, limit: int) -> None:
    from control_plane.database import make_engine, sessions
    from control_plane.retention import RetentionService
    from control_plane.services import EngineService

    config = Settings()
    engine = make_engine(config.database_url)
    try:
        service = RetentionService(EngineService(sessions(engine), config))
        result = (
            service.collect(apply=apply, limit=limit)
            if operation == "blobs"
            else (service.history(apply=apply, limit=limit))
        )
        typer.echo(json.dumps(result, indent=2))
    finally:
        engine.dispose()


@app.command("storage-collect")
def storage_collect(apply: bool = False, limit: int = 1000) -> None:
    """Preview old unreferenced blobs; --apply permanently removes eligible bytes."""
    retention_command("blobs", apply, limit)


@app.command("history-prune")
def history_prune(apply: bool = False, limit: int = 1000) -> None:
    """Preview expired upload history, credentials, logs and heartbeats; --apply prunes."""
    retention_command("history", apply, limit)


@app.command("cache-prune")
def cache_prune(apply: bool = False, target_bytes: int = 0) -> None:
    """Preview S3 local cache eviction; --apply removes copies while pinning active readers."""
    if target_bytes < 0:
        raise typer.BadParameter("target bytes must be non-negative")
    typer.echo(json.dumps(BlobStore(Settings()).trim_cache(target_bytes, apply=apply), indent=2))


@app.command("staging-collect")
def staging_collect(apply: bool = False, limit: int = 1000) -> None:
    """Preview crashed staging/pin leases; --apply removes only inactive managed paths."""
    typer.echo(
        json.dumps(BlobStore(Settings()).collect_temporary(apply=apply, limit=limit), indent=2)
    )


@app.command("operations-once")
def operations_once() -> None:
    """Run due supervised maintenance/backup work using the configured policies."""
    from control_plane.database import make_engine, sessions
    from control_plane.operations import OperationsService
    from control_plane.services import EngineService

    config = Settings()
    engine = make_engine(config.database_url)
    try:
        typer.echo(json.dumps(OperationsService(EngineService(sessions(engine), config)).tick()))
    finally:
        engine.dispose()


@app.command("restore-drill")
def restore_drill_command(backup: Path) -> None:
    """Verify an actual restore in an automatically cleaned, fresh PostgreSQL database."""
    from control_plane.operations import restore_drill

    typer.echo(json.dumps(restore_drill(Settings(), backup), indent=2))
