"""Durable, ordered, retryable dataset transfers with atomic quota reservation."""

import hashlib
import re
from collections.abc import Iterator
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from control_plane.access import actor_id, audit
from control_plane.datasets import DatasetService, storage_error
from control_plane.errors import DomainError
from control_plane.models import (
    Admission,
    DatasetFile,
    DatasetVersion,
    UploadPart,
    UploadSession,
    identifier,
)
from control_plane.quotas import storage_admission
from control_plane.services import EngineService
from control_plane.storage import BlobStore


class UploadService:
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def row(self, session: Session, upload_id: str, *, lock: bool = False) -> UploadSession:
        query = select(UploadSession).where(UploadSession.id == upload_id)
        row = session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise DomainError(404, "upload not found")
        return row

    def check_open(self, session: Session, row: UploadSession) -> None:
        if row.expires_at <= self.svc.now(session):
            raise DomainError(410, "upload expired; start a new transfer")
        if row.status != "OPEN":
            raise DomainError(409, "upload is no longer open")

    def create(self, version_id: str, name: str, size: int, sha256: str | None) -> UploadSession:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", name):
            raise DomainError(422, "invalid file name")
        if size > self.svc.settings.dataset_max_bytes:
            raise DomainError(413, "dataset file exceeds configured limit")
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            version = session.scalar(
                select(DatasetVersion)
                .where(
                    DatasetVersion.id == version_id,
                )
                .with_for_update()
            )
            if version is None:
                raise DomainError(404, "dataset version not found")
            if version.status != "DRAFT":
                raise DomainError(409, "sealed versions are immutable")
            now = self.svc.now(session)
            existing = session.scalar(
                select(UploadSession)
                .where(
                    UploadSession.version_id == version_id,
                    UploadSession.name == name,
                    UploadSession.status.in_(["OPEN", "COMPLETED"]),
                    UploadSession.expires_at > now,
                )
                .order_by(UploadSession.created_at.desc())
                .limit(1)
            )
            if existing:
                if existing.total_bytes != size or existing.expected_sha256 != sha256:
                    raise DomainError(409, "existing upload declares different content")
                return existing
            if session.scalar(
                select(DatasetFile.id).where(
                    DatasetFile.version_id == version_id,
                    DatasetFile.name == name,
                )
            ):
                raise DomainError(409, "dataset file name already exists")
            storage_admission(session, version.project_id, size, now=now)
            active = (
                session.scalar(
                    select(func.count())
                    .select_from(UploadSession)
                    .where(
                        UploadSession.status == "OPEN",
                        UploadSession.expires_at > now,
                    )
                    .execution_options(strata_unscoped=True)
                )
                or 0
            )
            if active >= self.svc.settings.upload_max_active:
                raise DomainError(429, "active upload capacity reached")
            row = UploadSession(
                id=identifier(),
                project_id=version.project_id,
                version_id=version_id,
                created_by=actor_id(),
                name=name,
                total_bytes=size,
                received_bytes=0,
                expected_sha256=sha256,
                chunk_bytes=self.svc.settings.upload_chunk_bytes,
                status="OPEN",
                created_at=now,
                expires_at=now + timedelta(seconds=self.svc.settings.upload_lifetime_seconds),
            )
            session.add(row)
            audit(session, now, "UPLOAD_CREATED", row.id, row.project_id, size=size)
            return row

    def get(self, upload_id: str) -> UploadSession:
        with self.svc.factory() as session:
            return self.row(session, upload_id)

    def chunk(self, upload_id: str, offset: int, content: bytes, expected: str) -> UploadSession:
        if hashlib.sha256(content).hexdigest() != expected:
            raise DomainError(422, "chunk checksum mismatch")
        with self.svc.factory.begin() as session:
            from control_plane.retention import storage_fence

            storage_fence(session)
            row = self.row(session, upload_id, lock=True)
            if row.status != "COMPLETED":
                self.check_open(session, row)
            if offset < 0 or offset % row.chunk_bytes or offset >= row.total_bytes:
                raise DomainError(422, "invalid chunk offset")
            if len(content) != min(row.chunk_bytes, row.total_bytes - offset):
                raise DomainError(422, "chunk size does not match the declared transfer")
            existing = session.get(UploadPart, (row.id, offset))
            if existing:
                if existing.sha256 != expected:
                    raise DomainError(409, "chunk offset already contains different content")
                return row
            self.check_open(session, row)
            if offset != row.received_bytes:
                raise DomainError(409, "send the next chunk at received_bytes")
            try:
                BlobStore(self.svc.settings).put_bytes(expected, content)
            except OSError as exc:
                storage_error(exc)
            session.add(
                UploadPart(upload_id=row.id, offset=offset, sha256=expected, size=len(content))
            )
            row.received_bytes += len(content)
            audit(
                session,
                self.svc.now(session),
                "UPLOAD_CHUNK_STORED",
                row.id,
                row.project_id,
                offset=offset,
                size=len(content),
            )
            return row

    def complete(self, upload_id: str) -> DatasetFile:
        with self.svc.factory.begin() as session:
            from control_plane.retention import storage_fence

            storage_fence(session)
            row = self.row(session, upload_id, lock=True)
            if row.status == "COMPLETED":
                result = session.get(DatasetFile, row.file_id)
                if result is None:
                    raise DomainError(503, "completed upload metadata is unavailable")
                return result
            self.check_open(session, row)
            if row.received_bytes != row.total_bytes:
                raise DomainError(409, "upload is incomplete")
            store = BlobStore(self.svc.settings)
            store.ensure_space(row.total_bytes)
            try:
                with store.temporary("assembly-", directory=True) as directory:
                    path = directory / "content"
                    digest, size = hashlib.sha256(), 0
                    with path.open("wb") as output:
                        for chunk in self.parts(session, row, store):
                            digest.update(chunk)
                            size += len(chunk)
                            with store.local_guard():
                                store.ensure_space(len(chunk))
                                output.write(chunk)
                                output.flush()
                    sha = digest.hexdigest()
                    if size != row.total_bytes or (
                        row.expected_sha256 and row.expected_sha256 != sha
                    ):
                        raise DomainError(422, "completed upload checksum or size mismatch")
                    result = DatasetService(self.svc).commit_file(
                        session,
                        row.version_id,
                        row.name,
                        sha,
                        size,
                        path,
                        reservation=row.id,
                    )
                    session.flush()
                    row.file_id, row.status = result.id, "COMPLETED"
                    audit(
                        session,
                        self.svc.now(session),
                        "UPLOAD_COMPLETED",
                        row.id,
                        row.project_id,
                        file_id=result.id,
                        sha256=sha,
                    )
                    return result
            except OSError as exc:
                storage_error(exc)

    def parts(self, session: Session, row: UploadSession, store: BlobStore) -> Iterator[bytes]:
        offset = 0
        for part in session.scalars(
            select(UploadPart)
            .where(
                UploadPart.upload_id == row.id,
            )
            .order_by(UploadPart.offset)
        ):
            if part.offset != offset:
                raise DomainError(503, "upload part metadata is inconsistent")
            with store.materialized(part.sha256, part.size) as path, path.open("rb") as stream:
                yield from iter(lambda: stream.read(1024**2), b"")
            offset += part.size

    def cancel(self, upload_id: str) -> None:
        with self.svc.factory.begin() as session:
            row = self.row(session, upload_id, lock=True)
            if row.status == "COMPLETED":
                raise DomainError(409, "a completed upload cannot be cancelled")
            row.status = "CANCELLED"
            audit(session, self.svc.now(session), "UPLOAD_CANCELLED", row.id, row.project_id)
