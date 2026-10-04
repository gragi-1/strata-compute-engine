"""Explicit, reference-safe storage collection and bounded history retention."""

from datetime import timedelta
from typing import Any

from sqlalchemy import delete, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from control_plane.access import audit, principal
from control_plane.domain import TERMINAL
from control_plane.errors import DomainError
from control_plane.models import (
    AccessToken,
    Artifact,
    Attempt,
    DatasetFile,
    Job,
    LoginThrottle,
    OIDCHandoff,
    OIDCState,
    UploadPart,
    UploadSession,
    WebhookDelivery,
    WorkerHeartbeat,
)
from control_plane.services import EngineService
from control_plane.storage import BlobStore


def storage_fence(session: Session | Connection, *, exclusive: bool = False) -> None:
    """Acquire before resource locks; held until the SQL transaction ends.

    PostgreSQL readers share a schema-scoped advisory lock. Collection is exclusive.
    SQLite serializes writers using its existing admission row.
    """
    dialect = session.get_bind().dialect if isinstance(session, Session) else session.dialect
    if dialect.name == "postgresql":
        function = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
        session.execute(text(f"SELECT {function}(hashtext(current_schema()), 1398035009)"))
    else:
        session.execute(text("UPDATE admission SET id=id WHERE id=1"))


def references(session: Session) -> set[str]:
    return (
        set(session.scalars(select(Artifact.sha256)))
        | set(session.scalars(select(DatasetFile.sha256)))
        | set(session.scalars(select(UploadPart.sha256)))
    )


class RetentionService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def collect(self, *, apply: bool = False, limit: int = 1000) -> dict[str, Any]:
        """Collect orphan bytes only, never datasets, jobs or output metadata."""
        if principal() is not None:
            raise DomainError(403, "storage collection is a local administrator operation")
        if not 1 <= limit <= 10000:
            raise DomainError(422, "collection limit must be 1..10000")
        store = BlobStore(self.svc.settings)
        with self.svc.factory.begin() as session:
            storage_fence(session, exclusive=True)
            now = self.svc.now(session)
            cutoff = now - timedelta(seconds=self.svc.settings.blob_retention_seconds)
            known = references(session)
            candidates = []
            total_bytes = 0
            for digest, size, modified in store.inventory_details():
                if digest in known or modified > cutoff:
                    continue
                candidates.append(digest)
                total_bytes += size
                if len(candidates) >= limit:
                    break
            deleted = []
            if apply:
                for digest in candidates:
                    store.delete(digest)
                    deleted.append(digest)
                audit(session, now, "ORPHAN_BLOBS_COLLECTED", count=len(deleted), bytes=total_bytes)
            return {
                "dry_run": not apply,
                "candidates": candidates,
                "deleted": deleted,
                "candidate_bytes": total_bytes,
                "limit": limit,
                "grace_seconds": self.svc.settings.blob_retention_seconds,
            }

    def history(self, *, apply: bool = False, limit: int = 1000) -> dict[str, Any]:
        """Expire transfers and prune their chunks, old credentials, logs and heartbeats.

        Scientific data, output files, job attempts/events and audit records are retained.
        Batches bound transaction size; repeat the command to process further history.
        """
        if principal() is not None:
            raise DomainError(403, "history retention is a local administrator operation")
        if not 1 <= limit <= 10000:
            raise DomainError(422, "retention limit must be 1..10000")
        with self.svc.factory.begin() as session:
            storage_fence(session, exclusive=True)
            now = self.svc.now(session)
            cutoff = now - timedelta(seconds=self.svc.settings.history_retention_seconds)
            uploads = list(
                session.scalars(
                    select(UploadSession)
                    .where(UploadSession.expires_at < cutoff)
                    .order_by(UploadSession.expires_at, UploadSession.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            heartbeats = list(
                session.scalars(
                    select(WorkerHeartbeat.id)
                    .where(WorkerHeartbeat.created_at < cutoff)
                    .order_by(WorkerHeartbeat.id)
                    .limit(limit)
                )
            )
            tokens = list(
                session.scalars(
                    select(AccessToken.id)
                    .where(AccessToken.expires_at < cutoff)
                    .order_by(AccessToken.expires_at)
                    .limit(limit)
                )
            )
            logs = list(
                session.scalars(
                    select(Attempt)
                    .join(Job)
                    .where(
                        Attempt.finished_at < cutoff,
                        Attempt.status.in_(TERMINAL),
                        Job.status.in_(TERMINAL),
                        Attempt.logs != "",
                    )
                    .order_by(Attempt.finished_at)
                    .limit(limit)
                    .with_for_update(of=Attempt)
                )
            )
            extra = {
                "oidc_states": (OIDCState, OIDCState.state_hash, OIDCState.expires_at),
                "oidc_handoffs": (OIDCHandoff, OIDCHandoff.token_hash, OIDCHandoff.expires_at),
                "login_throttles": (LoginThrottle, LoginThrottle.key, LoginThrottle.window_start),
                "webhook_deliveries": (
                    WebhookDelivery,
                    WebhookDelivery.id,
                    WebhookDelivery.completed_at,
                ),
            }
            selected = {}
            for name, (model, key, timestamp) in extra.items():
                selected[name] = list(
                    session.scalars(
                        select(key)
                        .where(timestamp < cutoff)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                )
                if apply:
                    session.execute(delete(model).where(key.in_(selected[name])))
            if apply:
                for row in uploads:
                    session.execute(delete(UploadPart).where(UploadPart.upload_id == row.id))
                    session.delete(row)
                session.execute(delete(WorkerHeartbeat).where(WorkerHeartbeat.id.in_(heartbeats)))
                session.execute(delete(AccessToken).where(AccessToken.id.in_(tokens)))
                for attempt in logs:
                    attempt.logs = ""
                audit(
                    session,
                    now,
                    "HISTORY_PRUNED",
                    uploads=len(uploads),
                    heartbeats=len(heartbeats),
                    tokens=len(tokens),
                    logs=len(logs),
                    ephemeral={key: len(value) for key, value in selected.items()},
                )
            return {
                "dry_run": not apply,
                "uploads": len(uploads),
                "heartbeats": len(heartbeats),
                "tokens": len(tokens),
                "logs": len(logs),
                "cutoff": cutoff.isoformat(),
                **{key: len(value) for key, value in selected.items()},
            }
