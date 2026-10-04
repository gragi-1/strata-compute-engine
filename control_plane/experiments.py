"""Project-scoped experiment runs with immutable submissions and verified replay."""

import hashlib
import json
import math
import re
from typing import Annotated, Any

from pydantic import Field, ValidationError, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from control_plane.access import actor_id, audit, project_id, scoped_key
from control_plane.domain import TERMINAL
from control_plane.models import (
    Admission,
    Artifact,
    Attempt,
    DatasetVersion,
    Experiment,
    ExperimentRun,
    identifier,
)
from control_plane.schemas import ArtifactInput, JobSubmit, NamedResource, StrictModel
from control_plane.services import DomainError, EngineService


class RunSubmit(StrictModel):
    job: JobSubmit
    source_revision: str | None = Field(default=None, pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict, max_length=50)

    @field_validator("metadata")
    @classmethod
    def bounded_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, allow_nan=False)) > 16384:
            raise ValueError("run metadata is limited to 16 KiB")
        return value


class MetricsUpdate(StrictModel):
    values: dict[str, float] = Field(min_length=1, max_length=100)

    @field_validator("values")
    @classmethod
    def valid_metrics(cls, value: dict[str, float]) -> dict[str, float]:
        if any(
            not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_.-]{0,63}", key) or not math.isfinite(item)
            for key, item in value.items()
        ):
            raise ValueError("metrics need portable names and finite numeric values")
        return value


class Replay(StrictModel):
    attempt_id: str | None = Field(default=None, max_length=36)
    checkpoints: list[ArtifactInput] = Field(default_factory=list, max_length=16)
    command: list[Annotated[str, Field(min_length=1, max_length=4096)]] | None = Field(
        default=None, min_length=1, max_length=128
    )


class ExperimentService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def create(self, body: NamedResource) -> Experiment:
        with self.svc.factory.begin() as session:
            row = Experiment(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                **body.model_dump(),
                created_at=self.svc.now(session),
            )
            session.add(row)
            audit(session, self.svc.now(session), "EXPERIMENT_CREATED", row.id, row.project_id)
            return row

    def run(
        self,
        experiment_id: str,
        body: RunSubmit,
        key: str | None,
        *,
        replay_of: str | None = None,
    ) -> tuple[ExperimentRun, bool]:
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        key = scoped_key(key)
        digest = hashlib.sha256(
            json.dumps(
                {
                    "experiment": experiment_id,
                    "body": body.model_dump(exclude_none=True),
                    "replay_of": replay_of,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self.svc.factory.begin() as session:
            session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
            if session.get(Experiment, experiment_id) is None:
                raise DomainError(404, "experiment not found")
            if key:
                existing = session.scalar(
                    select(ExperimentRun).where(ExperimentRun.idempotency_key == key)
                )
                if existing:
                    if existing.request_hash != digest:
                        raise DomainError(409, "idempotency key belongs to a different run")
                    return existing, False
            self.svc.admission(session)
            job = self.svc.create_job(session, body.job, None, digest)
            row = ExperimentRun(
                id=identifier(),
                project_id=project_id(),
                experiment_id=experiment_id,
                created_by=actor_id(),
                job_id=job.id,
                source_revision=body.source_revision,
                metadata_json=body.metadata,
                specification=body.job.model_dump(exclude_none=True),
                metrics={},
                request_hash=digest,
                idempotency_key=key,
                replay_of=replay_of,
                created_at=self.svc.now(session),
            )
            session.add(row)
            audit(
                session,
                self.svc.now(session),
                "EXPERIMENT_RUN_CREATED",
                row.id,
                row.project_id,
                job_id=job.id,
                replay_of=replay_of,
            )
            return row, True

    def row(self, session: Session, run_id: str, *, lock: bool = False) -> ExperimentRun:
        query = select(ExperimentRun).where(ExperimentRun.id == run_id)
        row = session.scalar(query.with_for_update() if lock else query)
        if row is None:
            raise DomainError(404, "experiment run not found")
        return row

    def get(self, run_id: str) -> dict[str, Any]:
        with self.svc.factory() as session:
            row = self.row(session, run_id)
            job = self.svc.job(session, row.job_id)
            attempts = list(
                session.scalars(
                    select(Attempt).where(Attempt.job_id == job.id).order_by(Attempt.number)
                )
            )
            versions = []
            for item in row.specification.get("inputs", []):
                version = session.get(DatasetVersion, item["version_id"])
                if version:
                    versions.append(
                        {
                            "version_id": version.id,
                            "alias": item["alias"],
                            "manifest_sha256": version.manifest_hash,
                        }
                    )
            return {
                "id": row.id,
                "experiment_id": row.experiment_id,
                "job_id": job.id,
                "status": job.status,
                "source_revision": row.source_revision,
                "metadata": row.metadata_json,
                "metrics": row.metrics,
                "specification": row.specification,
                "datasets": versions,
                "replay_of": row.replay_of,
                "created_at": row.created_at,
                "attempts": [
                    {
                        "id": attempt.id,
                        "number": attempt.number,
                        "status": attempt.status,
                        "worker_id": attempt.worker_id,
                        "provenance": attempt.provenance,
                    }
                    for attempt in attempts
                ],
                "artifacts": [
                    {
                        "id": artifact.id,
                        "attempt_id": artifact.attempt_id,
                        "name": artifact.name,
                        "size": artifact.size,
                        "sha256": artifact.sha256,
                        "uri": f"/artifacts/{artifact.id}",
                    }
                    for artifact in session.scalars(
                        select(Artifact)
                        .where(Artifact.job_id == job.id)
                        .order_by(Artifact.created_at, Artifact.id)
                    )
                ],
            }

    def metrics(self, run_id: str, body: MetricsUpdate) -> dict[str, float]:
        with self.svc.factory.begin() as session:
            row = self.row(session, run_id, lock=True)
            for key, value in body.values.items():
                if key in row.metrics and row.metrics[key] != value:
                    raise DomainError(409, "recorded metric values are immutable")
            values = row.metrics | body.values
            if len(values) > 100:
                raise DomainError(413, "a run supports at most 100 summary metrics")
            row.metrics = values
            audit(
                session,
                self.svc.now(session),
                "RUN_METRICS_RECORDED",
                row.id,
                row.project_id,
                names=sorted(body.values),
            )
            return row.metrics

    def replay(self, run_id: str, body: Replay, key: str | None) -> tuple[ExperimentRun, bool]:
        with self.svc.factory() as session:
            row = self.row(session, run_id)
            query = select(Attempt).where(Attempt.job_id == row.job_id)
            if body.attempt_id:
                query = query.where(Attempt.id == body.attempt_id)
            attempt = session.scalar(query.order_by(Attempt.number.desc()).limit(1))
            if attempt is None or attempt.status not in TERMINAL:
                raise DomainError(409, "replay requires a terminal source attempt")
            image = attempt.provenance.get("image_digest")
            if not image:
                raise DomainError(409, "source attempt did not record its resolved image")
            spec = dict(row.specification)
            spec["expected_image_digest"] = image
            spec["depends_on"] = []
            spec["dependency_policy"] = "all_succeeded"
            spec["artifact_inputs"] = [
                {key: file[key] for key in ("job_id", "name", "alias", "artifact_id")}
                for file in attempt.provenance.get("inputs", [])
                if "artifact_id" in file
            ]
            for checkpoint in body.checkpoints:
                artifact = (
                    session.get(Artifact, checkpoint.artifact_id)
                    if checkpoint.artifact_id
                    else None
                )
                if (
                    artifact is None
                    or artifact.job_id != row.job_id
                    or artifact.attempt_id != attempt.id
                    or checkpoint.job_id != row.job_id
                    or artifact.name != checkpoint.name
                ):
                    raise DomainError(422, "checkpoint must belong to the selected source attempt")
                spec["artifact_inputs"].append(checkpoint.model_dump(exclude_none=True))
            if body.command is not None:
                spec["command"] = body.command
            try:
                replay_body = RunSubmit(
                    job=JobSubmit.model_validate(spec),
                    source_revision=row.source_revision,
                    metadata=row.metadata_json | {"source_attempt": attempt.id},
                )
            except ValidationError as exc:
                raise DomainError(422, "invalid replay inputs, command or metadata") from exc
        return self.run(row.experiment_id, replay_body, key, replay_of=row.id)
