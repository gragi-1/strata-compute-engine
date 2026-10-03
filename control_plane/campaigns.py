"""Atomic parameter sweeps, explicit provenance and result exports."""

import hashlib
import itertools
import json
import math
import re
from typing import Any

from sqlalchemy import func, select

from control_plane.domain import TERMINAL, JobStatus
from control_plane.models import Admission, Artifact, Attempt, Campaign, Job, identifier
from control_plane.schemas import CampaignSubmit, JobSubmit, WorkflowSubmit
from control_plane.services import DomainError, EngineService


class CampaignService:
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def create(self, body: CampaignSubmit, key: str | None) -> tuple[Campaign, bool]:
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        count = math.prod(len(v) for v in body.matrix.values()) * body.repeats
        if count > self.svc.settings.campaign_max_jobs:
            raise DomainError(413, "campaign exceeds configured job limit")
        digest = hashlib.sha256(body.model_dump_json().encode()).hexdigest()
        with self.svc.factory.begin() as session:
            session.execute(select(Admission).where(Admission.id == 1).with_for_update()).one()
            if key:
                existing = session.scalar(select(Campaign).where(Campaign.idempotency_key == key))
                if existing:
                    if existing.request_hash != digest:
                        raise DomainError(409, "idempotency key was used with a different campaign")
                    return existing, False
            depth = (
                session.scalar(
                    select(func.count()).select_from(Job).where(Job.status.not_in(TERMINAL))
                )
                or 0
            )
            if depth + count > self.svc.settings.queue_limit:
                raise DomainError(429, "campaign exceeds outstanding job capacity")
            row = Campaign(
                id=identifier(),
                name=body.name,
                description=body.description,
                specification=body.model_dump(),
                request_hash=digest,
                idempotency_key=key,
                created_at=self.svc.now(session),
            )
            session.add(row)
            session.flush()
            keys = list(body.matrix)
            for values in itertools.product(*body.matrix.values()):
                for repeat in range(body.repeats):
                    params = dict(zip(keys, values, strict=True)) | {"repeat": repeat}

                    def render(
                        arg: str, parameters: dict[str, str | int | float | bool] = params
                    ) -> str:
                        def replacement(match: re.Match[str]) -> str:
                            name = match.group(1)
                            if name not in parameters:
                                raise DomainError(422, f"unknown template parameter: {name}")
                            return str(parameters[name])

                        return re.sub(r"\$\{([a-zA-Z][a-zA-Z0-9_]*)\}", replacement, arg)

                    specification = body.template.model_dump()
                    specification["command"] = [render(arg) for arg in body.template.command]
                    specification["name"] = f"{body.name[:80]}-{identifier()[:8]}"
                    self.svc.create_job(
                        session,
                        JobSubmit.model_validate(specification),
                        None,
                        digest,
                        row.id,
                        params,
                    )
            return row, True

    def get(self, campaign_id: str) -> dict[str, Any]:
        with self.svc.factory() as session:
            row = session.get(Campaign, campaign_id)
            if row is None:
                raise DomainError(404, "campaign not found")
            counts: dict[str, int] = {
                state: count
                for state, count in (
                    session.execute(
                        select(Job.status, func.count())
                        .where(Job.campaign_id == campaign_id)
                        .group_by(Job.status)
                    ).all()
                )
            }
            total = sum(counts.values())
            done = sum(n for state, n in counts.items() if state in TERMINAL)
            return {
                "id": row.id,
                "name": row.name,
                "description": row.description,
                "created_at": row.created_at,
                "specification": row.specification,
                "counts": counts,
                "total": total,
                "completed": done,
                "progress": done / total if total else 0,
                "status": "COMPLETED" if done == total else "ACTIVE",
            }

    def workflow(self, body: WorkflowSubmit, key: str | None) -> tuple[Campaign, bool]:
        from graphlib import CycleError, TopologicalSorter

        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "invalid idempotency key")
        graph = {
            name: set(node.depends_on) | {i.job_id for i in node.artifact_inputs}
            for name, node in body.nodes.items()
        }
        if any(not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,63}", name) for name in graph):
            raise DomainError(422, "invalid workflow node name")
        if any(parent not in graph for parents in graph.values() for parent in parents):
            raise DomainError(422, "workflow dependencies must refer to its nodes")
        try:
            order = list(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise DomainError(422, "workflow contains a cycle") from exc
        digest = hashlib.sha256(body.model_dump_json().encode()).hexdigest()
        with self.svc.factory.begin() as session:
            session.execute(select(Admission).where(Admission.id == 1).with_for_update()).one()
            existing = (
                session.scalar(select(Campaign).where(Campaign.idempotency_key == key))
                if key
                else None
            )
            if existing:
                if existing.request_hash != digest:
                    raise DomainError(409, "idempotency key was used with a different workflow")
                return existing, False
            depth = (
                session.scalar(
                    select(func.count()).select_from(Job).where(Job.status.not_in(TERMINAL))
                )
                or 0
            )
            if depth + len(order) > self.svc.settings.queue_limit:
                raise DomainError(429, "workflow exceeds outstanding job capacity")
            row = Campaign(
                id=identifier(),
                name=body.name,
                description=body.description,
                specification=body.model_dump(),
                request_hash=digest,
                idempotency_key=key,
                created_at=self.svc.now(session),
            )
            session.add(row)
            session.flush()
            ids: dict[str, str] = {}
            for name in order:
                specification = body.nodes[name].model_dump()
                specification["depends_on"] = [ids[parent] for parent in graph[name]]
                for item in specification["artifact_inputs"]:
                    item["job_id"] = ids[item["job_id"]]
                job = self.svc.create_job(
                    session,
                    JobSubmit.model_validate(specification),
                    None,
                    digest,
                    row.id,
                    {"node": name},
                )
                ids[name] = job.id
            return row, True

    def jobs(self, campaign_id: str, limit: int = 100, offset: int = 0) -> list[Job]:
        self.get(campaign_id)
        with self.svc.factory() as session:
            return list(
                session.scalars(
                    select(Job)
                    .where(Job.campaign_id == campaign_id)
                    .order_by(Job.created_at, Job.id)
                    .limit(limit)
                    .offset(offset)
                )
            )

    def retry(self, campaign_id: str) -> int:
        with self.svc.factory.begin() as session:
            session.execute(select(Admission).where(Admission.id == 1).with_for_update()).one()
            if session.get(Campaign, campaign_id) is None:
                raise DomainError(404, "campaign not found")
            jobs = list(
                session.scalars(
                    select(Job)
                    .where(
                        Job.campaign_id == campaign_id,
                        Job.status.in_(
                            [JobStatus.FAILED, JobStatus.TIMED_OUT, JobStatus.CANCELLED]
                        ),
                    )
                    .order_by(Job.id)
                    .with_for_update()
                )
            )
            depth = (
                session.scalar(
                    select(func.count()).select_from(Job).where(Job.status.not_in(TERMINAL))
                )
                or 0
            )
            if depth + len(jobs) > self.svc.settings.queue_limit:
                raise DomainError(429, "campaign retry exceeds outstanding job capacity")
            now = self.svc.now(session)
            for job in jobs:
                job.retry_count = 0
                job.eligible_at = now
                job.started_at = job.scheduled_at = job.finished_at = None
                self.svc.transition(session, job, JobStatus.QUEUED, now, manual=1)
            # All predecessor/dependant statuses become visible in the same commit.
            return len(jobs)

    def results(self, campaign_id: str) -> list[dict[str, Any]]:
        rows = []
        for job in self.jobs(campaign_id, self.svc.settings.campaign_max_jobs):
            result: dict[str, Any] = {
                "job_id": job.id,
                "status": job.status,
                **job.parameters,
                "attempts": job.attempts_count,
            }
            with self.svc.factory() as session:
                attempt = session.scalar(
                    select(Attempt)
                    .where(Attempt.job_id == job.id)
                    .order_by(Attempt.number.desc())
                    .limit(1)
                )
                if attempt:
                    result["worker_id"] = attempt.worker_id
                    result["reason"] = attempt.reason
                    if attempt.started_at and attempt.finished_at:
                        result["execution_seconds"] = (
                            attempt.finished_at - attempt.started_at
                        ).total_seconds()
                    artifact = session.scalar(
                        select(Artifact).where(
                            Artifact.attempt_id == attempt.id, Artifact.name == "result.json"
                        )
                    )
                    if artifact and artifact.size <= 1024 * 1024:
                        try:
                            value = json.loads(
                                (self.svc.settings.artifact_root / artifact.sha256).read_bytes()
                            )
                            if isinstance(value, dict):
                                for k, v in value.items():
                                    if isinstance(v, (str, int, float, bool)) or v is None:
                                        result[f"result.{k}"] = (
                                            None
                                            if isinstance(v, float) and not math.isfinite(v)
                                            else v
                                        )
                        except (OSError, ValueError):
                            result["result_error"] = "result.json is unavailable or invalid"
            rows.append(result)
        return rows
