"""Restart-aware maintenance, verified backups and disposable PostgreSQL restore drills."""

import logging
import re
import shutil
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url

from control_plane.access import audit, principal
from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.errors import DomainError
from control_plane.logging import configure_logging
from control_plane.maintenance import backup_store, restore_store, verify_store
from control_plane.models import Admission, MaintenanceState
from control_plane.retention import RetentionService, references
from control_plane.services import EngineService
from control_plane.storage import BlobStore

logger = logging.getLogger(__name__)
BACKUP_NAME = re.compile(r"snapshot-[0-9a-f]{32}")


def owned_directory(root: Path, path: Path) -> None:
    if path.is_symlink() or path.resolve().parent != root.resolve():
        raise ValueError("operational directory must be an immediate unlinked child")
    if any(
        child.is_symlink() or not child.resolve().is_relative_to(root.resolve())
        for child in path.rglob("*")
    ):
        raise ValueError("operational directory contains a linked path")


def restore_drill(config: Settings, backup: Path) -> dict[str, Any]:
    """Never restore over a deployment: create a fresh, owned database and file root."""
    record = verify_store(backup)
    name = "strata_drill_" + uuid4().hex
    assert re.fullmatch(r"strata_drill_[0-9a-f]{32}", name)
    root = backup.parent.resolve()
    artifacts = root / (".drill-" + uuid4().hex)
    source = make_url(config.database_url)
    admin = create_engine(
        source.set(database="postgres").update_query_dict({"options": ""}),
        isolation_level="AUTOCOMMIT",
    )
    created = False
    try:
        with admin.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{name}"'))
            created = True
        target = config.model_copy(
            update={
                "database_url": source.set(database=name).render_as_string(hide_password=False),
                "artifact_root": artifacts,
                "storage_backend": "filesystem",
                "s3_bucket": "",
            }
        )
        restore_store(backup, target)
        engine = make_engine(target.database_url)
        try:
            with sessions(engine)() as session:
                assert session.get(Admission, 1) is not None
                restored = references(session)
                if restored != set(record["blobs"]):
                    raise ValueError("restored database references do not match the manifest")
            store = BlobStore(target)
            for digest, size in record["blobs"].items():
                store.get_path(digest, size)
        finally:
            engine.dispose()
        return {"verified_blobs": len(record["blobs"]), "database_restored": True}
    finally:
        try:
            if created:
                with admin.connect() as connection:
                    connection.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        finally:
            admin.dispose()
            if artifacts.exists():
                owned_directory(root, artifacts)
                shutil.rmtree(artifacts)


class OperationsService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    @contextmanager
    def guard(self, name: str) -> Iterator[bool]:
        with self.svc.factory() as session:
            engine = session.get_bind()
        if not isinstance(engine, Engine):
            raise DomainError(422, "supervision requires an engine-bound session factory")
        if engine.dialect.name != "postgresql":
            raise DomainError(422, "supervised operations require PostgreSQL")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            schema = connection.scalar(text("SELECT hashtext(current_schema())"))
            key = connection.scalar(text("SELECT hashtext(:name)"), {"name": "strata-ops-" + name})
            locked = connection.scalar(
                text("SELECT pg_try_advisory_lock(:schema, :key)"), {"schema": schema, "key": key}
            )
            try:
                yield bool(locked)
            finally:
                if locked:
                    connection.execute(
                        text("SELECT pg_advisory_unlock(:schema, :key)"),
                        {"schema": schema, "key": key},
                    )

    def operation(self, name: str, interval: int, action: Callable[[], dict[str, Any]]) -> bool:
        if principal() is not None:
            raise DomainError(403, "supervision is a local administrator operation")
        with self.guard(name) as locked:
            if not locked:
                return False
            with self.svc.factory.begin() as session:
                now = self.svc.now(session)
                row = session.get(MaintenanceState, name)
                if row and row.status != "RUNNING" and row.next_run_at > now:
                    return False
                if row is None:
                    row = MaintenanceState(name=name, next_run_at=now, result={})
                    session.add(row)
                # A RUNNING row after obtaining this lock means its prior process exited.
                row.status, row.started_at, row.last_error = "RUNNING", now, None
                row.next_run_at = now + timedelta(seconds=interval)
            try:
                result = action()
            except Exception as exc:
                # Exception messages can contain connection strings; expose only the class.
                with self.svc.factory.begin() as session:
                    row = session.get(MaintenanceState, name)
                    assert row is not None
                    row.status, row.last_error = "FAILED", type(exc).__name__
                    row.next_run_at = self.svc.now(session) + timedelta(seconds=60)
                    audit(
                        session,
                        self.svc.now(session),
                        "OPERATION_FAILED",
                        name,
                        error=type(exc).__name__,
                    )
                logger.error("operation_failed: %s (%s)", name, type(exc).__name__)
                return True
            with self.svc.factory.begin() as session:
                row = session.get(MaintenanceState, name)
                assert row is not None
                row.status, row.succeeded_at, row.result = (
                    "SUCCEEDED",
                    self.svc.now(session),
                    result,
                )
                audit(session, self.svc.now(session), "OPERATION_SUCCEEDED", name)
            return True

    def maintenance(self) -> dict[str, Any]:
        config, retention = self.svc.settings, RetentionService(self.svc)
        return {
            "staging": BlobStore(config).collect_temporary(apply=True),
            "history": retention.history(apply=config.maintenance_apply_history),
            "blobs": retention.collect(apply=config.maintenance_apply_blobs),
        }

    def backup(self) -> dict[str, Any]:
        config, root = self.svc.settings, self.svc.settings.backup_root
        if root is None:
            raise ValueError("backup root is not configured")
        root = root.resolve()
        artifacts = config.artifact_root.resolve()
        if root.is_relative_to(artifacts) or artifacts.is_relative_to(root):
            raise ValueError("backup and artifact roots must be separate")
        root.mkdir(parents=True, exist_ok=True)
        with self.svc.factory() as session:
            state = session.get(MaintenanceState, "backup")
            checkpoint = state.result if state else {}
        if checkpoint.get("pending_drill") and BACKUP_NAME.fullmatch(
            checkpoint.get("snapshot", "")
        ):
            output = root / checkpoint["snapshot"]
            owned_directory(root, output)
            record = verify_store(output)
        else:
            output = root / ("snapshot-" + uuid4().hex)
            try:
                record = backup_store(config, output)
                verify_store(output)
            except Exception:
                if output.exists() and not (output / "manifest.json").exists():
                    owned_directory(root, output)
                    shutil.rmtree(output)
                raise
        if config.backup_restore_drill:
            with self.svc.factory.begin() as session:
                state = session.get(MaintenanceState, "backup")
                if state:
                    state.result = {"snapshot": output.name, "pending_drill": True}
        drill = restore_drill(config, output) if config.backup_restore_drill else None
        # Failed/incomplete backups survive for inspection. Only verified predecessors prune.
        previous = []
        for path in root.iterdir():
            if path == output or not BACKUP_NAME.fullmatch(path.name) or not path.is_dir():
                continue
            owned_directory(root, path)
            try:
                manifest = verify_store(path)
            except (OSError, ValueError, KeyError):
                continue
            previous.append((manifest["created_at"], path))
        deleted = []
        for _, path in sorted(previous, reverse=True)[config.backup_keep - 1 :]:
            owned_directory(root, path)
            shutil.rmtree(path)
            deleted.append(path.name)
        return {
            "snapshot": output.name,
            "verified_blobs": len(record["blobs"]),
            "database_bytes": record["database_bytes"],
            "restore_drill": drill,
            "pruned_snapshots": deleted,
        }

    def tick(self) -> dict[str, bool]:
        config = self.svc.settings
        results = {
            "maintenance": self.operation(
                "maintenance", config.maintenance_interval_seconds, self.maintenance
            )
        }
        if config.backup_root is not None:
            results["backup"] = self.operation(
                "backup", config.backup_interval_seconds, self.backup
            )
        return results


def main() -> None:
    configure_logging()
    config = Settings()
    engine = make_engine(config.database_url)
    service = OperationsService(EngineService(sessions(engine), config))
    stopped = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    try:
        while not stopped.is_set():
            try:
                service.tick()
            except Exception as exc:
                logger.error("supervision_unavailable: %s", type(exc).__name__)
            stopped.wait(5)
    finally:
        engine.dispose()
