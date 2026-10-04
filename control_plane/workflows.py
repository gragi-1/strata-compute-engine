"""Transactional, bounded expansion from immutable successful-attempt artifacts."""

import hashlib
import json
import math
import re
from datetime import datetime, timedelta
from typing import Any

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from control_plane.access import access_scope, actor_id, audit
from control_plane.domain import TERMINAL, WAITING, JobStatus
from control_plane.models import Admission, Artifact, Attempt, Job, JobDependency, WorkflowExpansion
from control_plane.periodic import PeriodicService
from control_plane.schemas import JobSubmit
from control_plane.services import DomainError, EngineService
from control_plane.storage import BlobStore


def reset_barrier(session: Session, job: Job, now: datetime) -> None:
    if job.execution_kind != "barrier":
        return
    row = session.scalar(select(WorkflowExpansion).where(WorkflowExpansion.gate_job_id == job.id))
    if row is not None and row.status != "EXPANDED":
        row.status = "WAITING"
        row.last_error = None
        row.next_check_at = now
        if row.created_by is None:
            row.created_by = actor_id()


class WorkflowService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def manifest(
        self, session: Session, row: WorkflowExpansion
    ) -> tuple[Artifact, list[dict[str, Any]]]:
        artifact = session.scalar(
            select(Artifact)
            .join(Attempt, Attempt.id == Artifact.attempt_id)
            .where(
                Artifact.job_id == row.source_job_id,
                Artifact.name == row.artifact_name,
                Attempt.status == JobStatus.SUCCEEDED,
            )
            .order_by(Attempt.number.desc())
            .limit(1)
        )
        if artifact is None or artifact.size > 1024**2:
            raise DomainError(422, "expansion requires a successful JSON artifact of at most 1 MiB")
        with BlobStore(self.svc.settings).materialized(artifact.sha256) as path:
            content = path.read_bytes()
        if len(content) != artifact.size or hashlib.sha256(content).hexdigest() != artifact.sha256:
            raise DomainError(503, "expansion artifact integrity check failed")
        try:
            value = json.loads(content)
        except (ValueError, UnicodeError) as exc:
            raise DomainError(
                422, "expansion artifact must contain a JSON array of parameter objects"
            ) from exc
        if not isinstance(value, list) or len(value) > row.max_jobs:
            raise DomainError(413, "expansion manifest exceeds its configured job limit")
        for item in value:
            if not isinstance(item, dict) or len(item) > 16:
                raise DomainError(
                    422, "each expansion item must be an object with at most 16 parameters"
                )
            for key, scalar in item.items():
                if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]{0,31}", key) or key in {
                    "node",
                    "expansion",
                    "item",
                }:
                    raise DomainError(422, "invalid or reserved expansion parameter")
                if (
                    not isinstance(scalar, str | int | float | bool)
                    or isinstance(scalar, float)
                    and not math.isfinite(scalar)
                ):
                    raise DomainError(422, "expansion parameters must be finite JSON scalars")
            if len(json.dumps(item).encode()) > 16384:
                raise DomainError(413, "expansion parameter object exceeds 16 KiB")
        return artifact, value

    def tick(self, *, limit: int = 20) -> int:
        expanded = 0
        with self.svc.factory() as session:
            ids = list(
                session.scalars(
                    select(WorkflowExpansion.id)
                    .join(Job, Job.id == WorkflowExpansion.source_job_id)
                    .where(
                        WorkflowExpansion.status == "WAITING",
                        Job.status.in_(TERMINAL),
                        WorkflowExpansion.next_check_at <= self.svc.now(session),
                    )
                    .order_by(WorkflowExpansion.created_at, WorkflowExpansion.id)
                    .limit(limit)
                )
            )
        for row_id in ids:
            with self.svc.factory.begin() as session:
                session.scalar(select(Admission).where(Admission.id == 1).with_for_update())
                row = session.scalar(
                    select(WorkflowExpansion)
                    .where(WorkflowExpansion.id == row_id)
                    .with_for_update()
                )
                now = self.svc.now(session)
                if row is None or row.status != "WAITING" or row.next_check_at > now:
                    continue
                source = self.svc.job(session, row.source_job_id)
                if source.status not in TERMINAL:
                    continue
                try:
                    actor = PeriodicService(self.svc).actor(session, row)
                    with access_scope(actor):
                        # Lock project budgets before the gate, matching scheduler lock order.
                        self.svc.admission(session, 0)
                        gate = self.svc.job(session, row.gate_job_id, lock=True)
                        if gate.status in TERMINAL:
                            row.status = (
                                "CANCELLED" if gate.status == JobStatus.CANCELLED else "FAILED"
                            )
                            continue
                        if source.status != JobStatus.SUCCEEDED:
                            raise DomainError(422, "expansion source did not succeed")
                        artifact, values = self.manifest(session, row)
                        total = (
                            session.scalar(
                                select(func.count())
                                .select_from(Job)
                                .where(Job.campaign_id == row.campaign_id)
                            )
                            or 0
                        )
                        if total + len(values) > self.svc.settings.campaign_max_jobs:
                            raise DomainError(413, "workflow exceeds configured total job limit")
                        # Roll back every child if validation/admission fails partway through.
                        with session.begin_nested():
                            self.svc.admission(session, len(values))
                            parents = set(gate.depends_on)
                            for index, params in enumerate(values):
                                spec = dict(row.template)

                                def render(arg: str, parameters: dict[str, Any] = params) -> str:
                                    def replace(match: re.Match[str]) -> str:
                                        if match[1] not in parameters:
                                            raise DomainError(
                                                422, "expansion template uses a missing parameter"
                                            )
                                        return str(parameters[match[1]])

                                    return re.sub(r"\$\{([a-zA-Z][a-zA-Z0-9_]*)\}", replace, arg)

                                spec["command"] = [render(arg) for arg in spec["command"]]
                                spec["name"] = f"{row.name}-{index}"
                                spec["depends_on"] = [source.id]
                                try:
                                    body = JobSubmit.model_validate(spec)
                                except ValidationError as exc:
                                    raise DomainError(
                                        422, "expanded job specification is invalid"
                                    ) from exc
                                job = self.svc.create_job(
                                    session,
                                    body,
                                    None,
                                    artifact.sha256,
                                    row.campaign_id,
                                    params | {"expansion": row.name, "item": index},
                                )
                                parents.add(job.id)
                                session.add(JobDependency(job_id=gate.id, parent_id=job.id))
                            gate.depends_on = sorted(parents)
                        row.status = "EXPANDED"
                        row.generated_count = len(values)
                        row.manifest_sha256 = artifact.sha256
                        row.last_error = None
                        audit(
                            session,
                            now,
                            "WORKFLOW_EXPANDED",
                            row.id,
                            row.project_id,
                            count=len(values),
                            sha256=artifact.sha256,
                        )
                        expanded += 1
                except DomainError as exc:
                    row.last_error = str(exc)[:256]
                    if exc.code in {429, 503}:
                        row.next_check_at = now + timedelta(seconds=30)
                    else:
                        row.status = "FAILED"
                        gate = self.svc.job(session, row.gate_job_id, lock=True)
                        if gate.status in WAITING:
                            gate.finished_at = now
                            self.svc.transition(
                                session, gate, JobStatus.FAILED, now, reason=row.last_error
                            )
                    audit(
                        session,
                        now,
                        "WORKFLOW_EXPANSION_DEFERRED"
                        if row.status == "WAITING"
                        else "WORKFLOW_EXPANSION_FAILED",
                        row.id,
                        row.project_id,
                        code=exc.code,
                    )
        self.complete_barriers(limit=limit)
        return expanded

    def complete_barriers(self, *, limit: int = 100) -> int:
        completed = 0
        with self.svc.factory.begin() as session:
            parent = aliased(Job)
            unfinished = (
                select(JobDependency.job_id)
                .join(parent, parent.id == JobDependency.parent_id)
                .where(parent.status.not_in(TERMINAL))
            )
            gates = list(
                session.scalars(
                    select(Job)
                    .join(WorkflowExpansion, WorkflowExpansion.gate_job_id == Job.id)
                    .where(
                        Job.execution_kind == "barrier",
                        Job.status.in_(WAITING),
                        WorkflowExpansion.status == "EXPANDED",
                        Job.id.not_in(unfinished),
                    )
                    .order_by(Job.id)
                    .limit(limit)
                    .with_for_update(of=Job, skip_locked=True)
                )
            )
            for gate in gates:
                parents = list(
                    session.scalars(select(Job.status).where(Job.id.in_(gate.depends_on)))
                )
                if any(status not in TERMINAL for status in parents):
                    continue
                now = self.svc.now(session)
                gate.finished_at = now
                self.svc.transition(
                    session,
                    gate,
                    JobStatus.SUCCEEDED
                    if all(status == JobStatus.SUCCEEDED for status in parents)
                    else JobStatus.FAILED,
                    now,
                    reason="workflow expansion joined",
                )
                completed += 1
        return completed
