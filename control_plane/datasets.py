"""Immutable, content-addressed dataset versions and bounded inspection."""

import csv
import errno
import hashlib
import json
import math
import re
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from control_plane.access import actor_id, audit, project_id
from control_plane.models import Dataset, DatasetFile, DatasetVersion, UploadSession, identifier
from control_plane.quotas import storage_admission
from control_plane.schemas import NamedResource
from control_plane.services import DomainError, EngineService
from control_plane.storage import BlobStore


def storage_error(exc: OSError) -> NoReturn:
    if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
        raise DomainError(503, "storage capacity exhausted; contact the administrator") from exc
    raise exc


def preview_value(value: Any) -> Any:
    """Keep native data previews JSON-safe without changing stored dataset bytes."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, bytes):
        return "hex:" + value.hex()
    if isinstance(value, dict):
        return {str(key): preview_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [preview_value(item) for item in value]
    return value


class DatasetService:
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def create(self, body: NamedResource) -> Dataset:
        with self.svc.factory.begin() as session:
            row = Dataset(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                **body.model_dump(),
                created_at=self.svc.now(session),
            )
            session.add(row)
            audit(session, self.svc.now(session), "DATASET_CREATED", row.id, row.project_id)
            return row

    def version(self, dataset_id: str, label: str) -> DatasetVersion:
        with self.svc.factory.begin() as session:
            dataset = session.get(Dataset, dataset_id)
            if dataset is None:
                raise DomainError(404, "dataset not found")
            row = DatasetVersion(
                id=identifier(),
                project_id=dataset.project_id,
                dataset_id=dataset_id,
                label=label,
                status="DRAFT",
                created_at=self.svc.now(session),
            )
            session.add(row)
            audit(session, self.svc.now(session), "DATASET_VERSION_CREATED", row.id, row.project_id)
            return row

    def validate_upload(self, version_id: str) -> None:
        with self.svc.factory() as session:
            version = session.get(DatasetVersion, version_id)
            if version is None:
                raise DomainError(404, "dataset version not found")
            if version.status != "DRAFT":
                raise DomainError(409, "sealed versions are immutable")

    def upload(self, version_id: str, name: str, chunks: Iterable[bytes]) -> DatasetFile:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", name):
            raise DomainError(422, "invalid file name; use a flat portable name")
        self.validate_upload(version_id)
        store = BlobStore(self.svc.settings)
        digest, size = hashlib.sha256(), 0
        try:
            store.ensure_space()
            with store.temporary("upload-") as temporary, temporary.open("wb") as output:
                for chunk in chunks:
                    size += len(chunk)
                    if size > self.svc.settings.dataset_max_bytes:
                        raise DomainError(413, "dataset file exceeds configured limit")
                    digest.update(chunk)
                    with store.local_guard():
                        store.ensure_space(len(chunk))
                        output.write(chunk)
                        output.flush()
                output.close()
                sha = digest.hexdigest()
                with self.svc.factory.begin() as session:
                    from control_plane.retention import storage_fence

                    storage_fence(session)
                    return self.commit_file(session, version_id, name, sha, size, temporary)
        except OSError as exc:
            storage_error(exc)

    def commit_file(
        self,
        session: Session,
        version_id: str,
        name: str,
        sha: str,
        size: int,
        source: Path,
        reservation: str | None = None,
    ) -> DatasetFile:
        version = session.scalar(
            select(DatasetVersion).where(DatasetVersion.id == version_id).with_for_update()
        )
        if version is None:
            raise DomainError(404, "dataset version not found")
        if version.status != "DRAFT":
            raise DomainError(409, "sealed versions are immutable")
        existing = session.scalar(
            select(DatasetFile).where(
                DatasetFile.version_id == version_id,
                DatasetFile.name == name,
            )
        )
        if existing:
            if existing.sha256 != sha:
                raise DomainError(409, "file name already contains different bytes")
            return existing
        count = (
            session.scalar(
                select(func.count())
                .select_from(DatasetFile)
                .where(
                    DatasetFile.version_id == version_id,
                )
            )
            or 0
        )
        if count >= self.svc.settings.dataset_max_files:
            raise DomainError(413, "dataset file count limit reached")
        now = self.svc.now(session)
        storage_admission(session, version.project_id, size, now=now, exclude_upload=reservation)
        BlobStore(self.svc.settings).put_file(sha, source)
        row = DatasetFile(
            id=identifier(),
            project_id=version.project_id,
            version_id=version_id,
            name=name,
            sha256=sha,
            size=size,
            created_at=now,
        )
        session.add(row)
        audit(session, now, "DATASET_FILE_UPLOADED", row.id, row.project_id, size=size, sha256=sha)
        return row

    def seal(self, version_id: str) -> DatasetVersion:
        with self.svc.factory.begin() as session:
            version = session.scalar(
                select(DatasetVersion).where(DatasetVersion.id == version_id).with_for_update()
            )
            if version is None:
                raise DomainError(404, "dataset version not found")
            if session.scalar(
                select(UploadSession.id)
                .where(
                    UploadSession.version_id == version_id,
                    UploadSession.status == "OPEN",
                    UploadSession.expires_at > self.svc.now(session),
                )
                .limit(1)
            ):
                raise DomainError(409, "complete or cancel open uploads before sealing")
            files = list(
                session.scalars(
                    select(DatasetFile)
                    .where(DatasetFile.version_id == version_id)
                    .order_by(DatasetFile.name)
                )
            )
            if not files:
                raise DomainError(409, "upload at least one file before sealing")
            manifest = [{"name": f.name, "sha256": f.sha256, "size": f.size} for f in files]
            version.manifest_hash = hashlib.sha256(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            version.status = "SEALED"
            audit(
                session,
                self.svc.now(session),
                "DATASET_VERSION_SEALED",
                version.id,
                version.project_id,
            )
            return version

    def file(self, file_id: str) -> tuple[DatasetFile, Path]:
        with self.svc.factory() as session:
            row = session.get(DatasetFile, file_id)
            if row is None:
                raise DomainError(404, "dataset file not found")
            path = BlobStore(self.svc.settings).get_path(row.sha256, row.size)
            return row, path

    def preview(self, file_id: str, limit: int = 50) -> dict[str, Any]:
        with self.open_file(file_id) as (row, path):
            return self.preview_path(row, path, limit)

    @contextmanager
    def open_file(self, file_id: str) -> Iterator[tuple[DatasetFile, Path]]:
        with self.svc.factory() as session:
            row = session.get(DatasetFile, file_id)
            if row is None:
                raise DomainError(404, "dataset file not found")
        with BlobStore(self.svc.settings).materialized(row.sha256, row.size) as path:
            yield row, path

    def preview_path(self, row: DatasetFile, path: Path, limit: int) -> dict[str, Any]:
        if row.size > self.svc.settings.preview_max_bytes:
            raise DomainError(413, "preview limit exceeded; download or process in a job")
        try:
            suffix = Path(row.name).suffix.lower()
            if suffix in {".csv", ".tsv"}:
                with path.open(encoding="utf-8-sig", newline="") as stream:
                    reader = csv.reader(stream, delimiter="\t" if suffix == ".tsv" else ",")
                    columns = next(reader, [])[:100]
                    rows = []
                    for record in reader:
                        rows.append(record[:100])
                        if len(rows) >= limit:
                            break
                return {"format": "table", "columns": columns, "rows": rows}
            if suffix == ".json":

                def invalid_constant(value: str) -> None:
                    raise ValueError(f"non-finite JSON constant: {value}")

                value = json.loads(
                    path.read_text(encoding="utf-8"), parse_constant=invalid_constant
                )
                return {
                    "format": "json",
                    "value": preview_value(value[:limit] if isinstance(value, list) else value),
                    "truncated": isinstance(value, list) and len(value) > limit,
                }
            if suffix == ".npy":
                import numpy as np

                array = np.load(path, mmap_mode="r", allow_pickle=False)
                return {
                    "format": "numpy",
                    "shape": list(array.shape),
                    "dtype": str(array.dtype),
                    "values": array.reshape(-1)[:limit].astype(str).tolist(),
                }
            if suffix == ".parquet":
                import pyarrow.parquet as pq

                parquet = pq.ParquetFile(path)
                if len(parquet.schema) > 100 or parquet.metadata.num_rows > 5_000_000:
                    raise DomainError(413, "Parquet metadata exceeds preview limits")
                batch = next(parquet.iter_batches(batch_size=limit), None)
                return {
                    "format": "table",
                    "columns": parquet.schema.names,
                    "rows": preview_value([list(r.values()) for r in batch.to_pylist()])
                    if batch
                    else [],
                    "total_rows": parquet.metadata.num_rows,
                }
        except (ValueError, OSError, UnicodeError, csv.Error, StopIteration) as exc:
            raise DomainError(422, "file cannot be previewed in its declared format") from exc
        raise DomainError(422, "preview supports CSV, TSV, JSON, NPY and Parquet")
