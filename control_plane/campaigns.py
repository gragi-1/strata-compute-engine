"""Atomic parameter sweeps, explicit provenance and result exports."""

import hashlib
import itertools
import json
import math
import re
from typing import Any

from sqlalchemy import func, select

from control_plane.access import actor_id, audit, project_id, scoped_key
from control_plane.domain import TERMINAL, JobStatus
from control_plane.models import (
    Admission,
    Artifact,
    Attempt,
    Campaign,
    Job,
    WorkflowExpansion,
    identifier,
)
from control_plane.schemas import CampaignSubmit, JobSubmit, WorkflowSubmit
from control_plane.services import DomainError, EngineService


def request_digest(body: CampaignSubmit | WorkflowSubmit) -> str:
    # Strip only additive defaults so existing v2 idempotency keys still replay.
    value = body.model_dump()
    if value.get("expansions") == {}:
        value.pop("expansions")
    nodes = [value["template"]] if "template" in value else list(value["nodes"].values())
    for node in nodes:
        for field in ("gpus", "gpu_memory_mb"):
            if node["resources"].get(field) == 0:
                node["resources"].pop(field)
        if node["expected_image_digest"] is None:
            node.pop("expected_image_digest")
        if node["dependency_policy"] == "all_succeeded":
            node.pop("dependency_policy")
        for item in node["artifact_inputs"]:
            if item["artifact_id"] is None:
                item.pop("artifact_id")
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


class CampaignService:
    def __init__(self, service: EngineService) -> None:
        self.svc = service

    def create(self, body: CampaignSubmit, key: str | None) -> tuple[Campaign, bool]:
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        key = scoped_key(key)
        count = math.prod(len(v) for v in body.matrix.values()) * body.repeats
        if count > self.svc.settings.campaign_max_jobs:
            raise DomainError(413, "campaign exceeds configured job limit")
        digest = request_digest(body)
        with self.svc.factory.begin() as session:
            session.execute(select(Admission).where(Admission.id == 1).with_for_update()).one()
            if key:
                existing = session.scalar(select(Campaign).where(Campaign.idempotency_key == key))
                if existing:
                    if existing.request_hash != digest:
                        raise DomainError(409, "idempotency key was used with a different campaign")
                    return existing, False
            self.svc.admission(session, count)
            row = Campaign(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                name=body.name,
                description=body.description,
                specification=body.model_dump(),
                request_hash=digest,
                idempotency_key=key,
                created_at=self.svc.now(session),
            )
            session.add(row)
            session.flush()
            audit(session, self.svc.now(session), "CAMPAIGN_CREATED", row.id, row.project_id)
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
        key = scoped_key(key)
        graph = {
            name: set(node.depends_on)
            | {i.job_id for i in node.artifact_inputs if not i.artifact_id}
            for name, node in body.nodes.items()
        }
        if set(body.nodes) & set(body.expansions):
            raise DomainError(422, "workflow node and expansion names must be distinct")
        for name, expansion in body.expansions.items():
            if expansion.source not in body.nodes:
                raise DomainError(422, "expansion source must be a container node")
            graph[name] = {expansion.source}
        if any(not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]{0,63}", name) for name in graph):
            raise DomainError(422, "invalid workflow node name")
        if any(parent not in graph for parents in graph.values() for parent in parents):
            raise DomainError(422, "workflow dependencies must refer to its nodes")
        try:
            order = list(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise DomainError(422, "workflow contains a cycle") from exc
        if len(order) > self.svc.settings.campaign_max_jobs:
            raise DomainError(413, "workflow exceeds configured total job limit")
        digest = request_digest(body)
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
            self.svc.admission(session, len(order))
            row = Campaign(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                name=body.name,
                description=body.description,
                specification=body.model_dump(),
                request_hash=digest,
                idempotency_key=key,
                created_at=self.svc.now(session),
            )
            session.add(row)
            session.flush()
            audit(session, self.svc.now(session), "WORKFLOW_CREATED", row.id, row.project_id)
            ids: dict[str, str] = {}
            for name in order:
                if name in body.expansions:
                    expansion = body.expansions[name]
                    specification = expansion.template.model_dump()
                    specification["name"] = f"{body.name[:60]}-{name}-join"
                    specification["command"] = ["barrier"]
                else:
                    specification = body.nodes[name].model_dump()
                specification["depends_on"] = [ids[parent] for parent in graph[name]]
                for item in specification["artifact_inputs"]:
                    if not item["artifact_id"]:
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
                if name in body.expansions:
                    job.execution_kind = "barrier"
                    expansion = body.expansions[name]
                    session.add(
                        WorkflowExpansion(
                            id=identifier(),
                            project_id=project_id(),
                            created_by=actor_id(),
                            campaign_id=row.id,
                            source_job_id=ids[expansion.source],
                            gate_job_id=job.id,
                            name=name,
                            artifact_name=expansion.artifact,
                            template=expansion.template.model_dump(exclude_none=True),
                            max_jobs=expansion.max_jobs,
                            status="WAITING",
                            generated_count=0,
                            next_check_at=self.svc.now(session),
                            created_at=self.svc.now(session),
                        )
                    )
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
            self.svc.admission(session, len(jobs))
            now = self.svc.now(session)
            for job in jobs:
                job.retry_count = 0
                job.eligible_at = now
                job.started_at = job.scheduled_at = job.finished_at = None
                from control_plane.workflows import reset_barrier

                reset_barrier(session, job, now)
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
                        from control_plane.storage import BlobStore

                        try:
                            with BlobStore(self.svc.settings).materialized(
                                artifact.sha256, artifact.size
                            ) as path:
                                value = json.loads(path.read_bytes())
                            if isinstance(value, dict):
                                for k, v in value.items():
                                    if isinstance(v, (str, int, float, bool)) or v is None:
                                        result[f"result.{k}"] = (
                                            None
                                            if isinstance(v, float) and not math.isfinite(v)
                                            else v
                                        )
                        except (OSError, ValueError, DomainError):
                            result["result_error"] = "result.json is unavailable or invalid"
            rows.append(result)
        return rows
