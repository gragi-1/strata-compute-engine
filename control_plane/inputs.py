from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from control_plane.models import (
    Artifact,
    Attempt,
    DatasetFile,
    Job,
    JobDependency,
    WorkflowExpansion,
)
from control_plane.services import DomainError, EngineService
from control_plane.storage import BlobStore


def manifest(svc: EngineService, credentials: Any) -> list[dict[str, Any]]:
    with svc.factory.begin() as session:
        job, attempt, _, _ = svc.credentials(
            session, credentials.attempt_id, credentials.session_id, credentials.lease_token
        )
        files = attempt.provenance.get("inputs")
        if files is None:
            files = resolved_manifest(svc, session, job)
            attempt.provenance = attempt.provenance | {"inputs": files}
        return [{key: row[key] for key in ("alias", "name", "sha256", "size")} for row in files]


def resolved_manifest(svc: EngineService, session: Session, job: Job) -> list[dict[str, Any]]:
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
        elif item.get("artifact_id"):
            pinned = session.get(Artifact, item["artifact_id"])
            if pinned is None:
                raise DomainError(422, "pinned input artifact is unavailable")
            rows = [pinned]
        else:
            parent = svc.job(session, item["job_id"])
            if parent.status != "SUCCEEDED":
                raise DomainError(409, "upstream artifact is not ready")
            if parent.execution_kind == "barrier":
                expansion = session.scalar(
                    select(WorkflowExpansion).where(WorkflowExpansion.gate_job_id == parent.id)
                )
                if expansion is None or expansion.status != "EXPANDED":
                    raise DomainError(422, "expansion inputs are unavailable")
                children = list(
                    session.scalars(
                        select(Job)
                        .join(JobDependency, JobDependency.parent_id == Job.id)
                        .where(JobDependency.job_id == parent.id, Job.id != expansion.source_job_id)
                        .order_by(Job.parameters["item"].as_integer())
                    )
                )
                for child in children:
                    attempt = session.scalar(
                        select(Attempt)
                        .where(Attempt.job_id == child.id, Attempt.status == "SUCCEEDED")
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
                        raise DomainError(
                            422, "expanded job did not produce the requested artifact"
                        )
                    files.append(
                        {
                            "alias": f"{item['alias']}-{child.parameters['item']}",
                            "name": artifact.name,
                            "sha256": artifact.sha256,
                            "size": artifact.size,
                            "artifact_id": artifact.id,
                            "job_id": artifact.job_id,
                        }
                    )
                continue
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
                    **(
                        {"artifact_id": row.id, "job_id": row.job_id}
                        if isinstance(row, Artifact)
                        else {"version_id": row.version_id}
                    ),
                }
            )
    if len({(file["alias"], file["name"]) for file in files}) != len(files):
        raise DomainError(422, "resolved input paths overlap")
    return files


def input_path(svc: EngineService, credentials: Any, sha256: str) -> tuple[Path, int]:
    row = next((f for f in manifest(svc, credentials) if f["sha256"] == sha256), None)
    if row is None:
        raise DomainError(404, "file is not an input of this attempt")
    path = BlobStore(svc.settings).get_path(sha256, row["size"])
    return path, row["size"]


@contextmanager
def open_input(svc: EngineService, credentials: Any, sha256: str) -> Iterator[tuple[Path, int]]:
    row = next((f for f in manifest(svc, credentials) if f["sha256"] == sha256), None)
    if row is None:
        raise DomainError(404, "file is not an input of this attempt")
    with BlobStore(svc.settings).materialized(sha256, row["size"]) as path:
        yield path, row["size"]
