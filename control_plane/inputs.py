from pathlib import Path
from typing import Any

from sqlalchemy import select

from control_plane.models import Artifact, Attempt, DatasetFile
from control_plane.services import DomainError, EngineService


def manifest(svc: EngineService, credentials: Any) -> list[dict[str, Any]]:
    with svc.factory.begin() as session:
        job, _, _, _ = svc.credentials(
            session, credentials.attempt_id, credentials.session_id, credentials.lease_token
        )
        files = []
        for item in job.inputs:
            rows: list[DatasetFile | Artifact]
            if "version_id" in item:
                rows = list(
                    session.scalars(
                        select(DatasetFile)
                        .where(DatasetFile.version_id == item["version_id"])
                        .order_by(DatasetFile.name)
                    )
                )
            else:
                parent = svc.job(session, item["job_id"])
                if parent.status != "SUCCEEDED":
                    raise DomainError(409, "upstream artifact is not ready")
                attempt = session.scalar(
                    select(Attempt)
                    .where(Attempt.job_id == parent.id)
                    .order_by(Attempt.number.desc())
                    .limit(1)
                )
                artifact = (
                    session.scalar(
                        select(Artifact).where(
                            Artifact.attempt_id == attempt.id, Artifact.name == item["name"]
                        )
                    )
                    if attempt
                    else None
                )
                if artifact is None:
                    raise DomainError(422, "upstream job did not produce the requested artifact")
                rows = [artifact]
            for row in rows:
                files.append(
                    {
                        "alias": item["alias"],
                        "name": row.name,
                        "sha256": row.sha256,
                        "size": row.size,
                    }
                )
        return files


def input_path(svc: EngineService, credentials: Any, sha256: str) -> tuple[Path, int]:
    row = next((f for f in manifest(svc, credentials) if f["sha256"] == sha256), None)
    if row is None:
        raise DomainError(404, "file is not an input of this attempt")
    path = svc.settings.artifact_root / sha256
    if not path.is_file():
        raise DomainError(503, "input bytes are unavailable")
    return path, row["size"]
