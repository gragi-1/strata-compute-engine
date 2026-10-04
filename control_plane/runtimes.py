"""Atomic group admission and lease-scoped, durable interactive/collective messages."""

import hashlib
import json
import math
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, field_validator, model_validator
from sqlalchemy import func, or_, select

from control_plane.access import actor_id, project_id, scoped_key
from control_plane.domain import ACTIVE, TERMINAL, WAITING, JobStatus, execution_overhead, fits
from control_plane.models import (
    Admission,
    Attempt,
    CollectiveRound,
    ComputeGroup,
    GPUDevice,
    GroupMember,
    InteractiveSession,
    Job,
    Project,
    SessionCell,
    Worker,
    identifier,
)
from control_plane.schemas import JobSubmit, StrictModel
from control_plane.services import DomainError, EngineService

MESSAGE_BYTES = 16384


class GroupSubmit(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    nodes: int = Field(default=2, ge=2, le=16)
    job: JobSubmit

    @model_validator(mode="after")
    def independent_inputs(self) -> "GroupSubmit":
        if self.job.depends_on or any(not i.artifact_id for i in self.job.artifact_inputs):
            raise ValueError("group inputs must be sealed datasets or pinned terminal artifacts")
        return self


class SessionSubmit(StrictModel):
    kind: Literal["python", "service"] = "python"
    job: JobSubmit
    idle_seconds: int = Field(default=900, ge=10, le=3600)
    service_port: int = Field(default=8080, ge=1024, le=65535)

    @model_validator(mode="after")
    def independent_inputs(self) -> "SessionSubmit":
        if self.job.depends_on or any(not i.artifact_id for i in self.job.artifact_inputs):
            raise ValueError("session inputs must be sealed datasets or pinned terminal artifacts")
        return self


class CellSubmit(StrictModel):
    code: str = Field(min_length=1, max_length=8192)
    timeout_seconds: int = Field(default=30, ge=1, le=300)

    @field_validator("code")
    @classmethod
    def code_bytes(cls, value: str) -> str:
        if len(value.encode()) > 8192:
            raise ValueError("cell code exceeds 8 KiB")
        return value


class ProxySubmit(StrictModel):
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "GET"
    path: str = Field(default="/", min_length=1, max_length=2048, pattern=r"^/[^\x00-\x20\x7f\\]*$")
    body: str = Field(default="", max_length=4096)
    timeout_seconds: int = Field(default=5, ge=1, le=10)

    @field_validator("path")
    @classmethod
    def local_path(cls, value: str) -> str:
        if value.startswith("//"):
            raise ValueError("service path must remain container-local")
        return value

    @field_validator("body")
    @classmethod
    def body_bytes(cls, value: str) -> str:
        if len(value.encode()) > 4096:
            raise ValueError("service body exceeds 4 KiB")
        return value


class CollectiveMessage(StrictModel):
    kind: Literal["collective"]
    sequence: int = Field(ge=0, le=1023)
    operation: Literal["barrier", "sum", "min", "max"]
    values: list[Annotated[float, Field(allow_inf_nan=False)]] = Field(max_length=256)

    @model_validator(mode="after")
    def width(self) -> "CollectiveMessage":
        if (self.operation == "barrier") != (len(self.values) == 0):
            raise ValueError("barriers require no values; reductions require 1..256 values")
        return self


class SessionMessage(StrictModel):
    kind: Literal["session"]
    sequence: int = Field(ge=0, le=1000)
    result: dict[str, Any] | None = None


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


class RuntimeService:
    def __init__(self, svc: EngineService) -> None:
        self.svc = svc

    def create_group(self, body: GroupSubmit, key: str | None) -> tuple[ComputeGroup, bool]:
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        digest, key = fingerprint(body.model_dump()), scoped_key(key)
        with self.svc.factory.begin() as s:
            s.execute(select(Admission).where(Admission.id == 1).with_for_update()).scalar_one()
            old = (
                s.scalar(select(ComputeGroup).where(ComputeGroup.idempotency_key == key))
                if key
                else None
            )
            if old:
                if old.request_hash != digest:
                    raise DomainError(409, "idempotency key has a different group request")
                return old, False
            self.svc.admission(s, body.nodes)
            now = self.svc.now(s)
            group = ComputeGroup(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                name=body.name,
                nodes=body.nodes,
                status="QUEUED",
                specification=body.model_dump(),
                created_at=now,
                idempotency_key=key,
                request_hash=digest,
                next_sequence=0,
            )
            s.add(group)
            s.flush()
            for rank in range(body.nodes):
                request = body.job.model_copy(
                    update={
                        "max_retries": 0,
                        "name": f"{body.name[:112]} rank {rank}",
                        "capabilities": sorted(set(body.job.capabilities) | {"runtime-bridge"}),
                    }
                )
                job = self.svc.create_job(s, request, None, digest)
                job.execution_kind = "collective"
                s.add(GroupMember(group_id=group.id, job_id=job.id, rank=rank))
            return group, True

    def group(self, group_id: str) -> dict[str, Any]:
        from control_plane.api import row_view

        with self.svc.factory() as s:
            group = s.get(ComputeGroup, group_id)
            if group is None:
                raise DomainError(404, "compute group not found")
            result = row_view(group)
            result["members"] = [
                dict(rank=rank, **row_view(job))
                for rank, job in s.execute(
                    select(GroupMember.rank, Job)
                    .join(Job, Job.id == GroupMember.job_id)
                    .where(GroupMember.group_id == group_id)
                    .order_by(GroupMember.rank)
                )
            ]
            return result

    def cancel_group(self, group_id: str) -> dict[str, Any]:
        with self.svc.factory.begin() as s:
            group = s.scalar(
                select(ComputeGroup).where(ComputeGroup.id == group_id).with_for_update()
            )
            if group is None:
                raise DomainError(404, "compute group not found")
            if group.status not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                group.status = "CANCELLING"
                ids = list(
                    s.scalars(
                        select(GroupMember.job_id)
                        .where(GroupMember.group_id == group.id)
                        .order_by(GroupMember.job_id)
                    )
                )
                for job_id in ids:
                    self.svc.cancel_single(job_id, s)
        return self.group(group_id)

    def retry_group(self, group_id: str, key: str | None) -> tuple[ComputeGroup, bool]:
        if key is None or not 1 <= len(key) <= 256:
            raise DomainError(422, "group retry requires a 1..256 character idempotency key")
        original = self.group(group_id)
        if any(j["status"] not in TERMINAL for j in original["members"]):
            raise DomainError(
                409, "all group members must be terminal before a fresh group can start"
            )
        retry_key = f"group-retry:{group_id}:{hashlib.sha256(key.encode()).hexdigest()}"
        return self.create_group(GroupSubmit.model_validate(original["specification"]), retry_key)

    def schedule_groups(self) -> int:
        svc, assigned = self.svc, 0
        with svc.factory.begin() as s:
            state = s.execute(
                select(Admission).where(Admission.id == 1).with_for_update(read=True)
            ).scalar_one()
            if not state.scheduling_enabled:
                return 0
            if not s.scalar(
                select(ComputeGroup.id).where(ComputeGroup.status == "QUEUED").limit(1)
            ):
                return 0
            projects = {
                p.id: p
                for p in s.scalars(
                    select(Project)
                    .where(Project.enabled.is_(True))
                    .order_by(Project.id)
                    .with_for_update(skip_locked=True)
                )
            }
            groups = list(
                s.scalars(
                    select(ComputeGroup)
                    .where(ComputeGroup.status == "QUEUED")
                    .order_by(ComputeGroup.created_at, ComputeGroup.id)
                    .limit(svc.settings.scheduler_batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            jobs_by_group = {
                g.id: list(
                    s.scalars(
                        select(Job)
                        .join(GroupMember, GroupMember.job_id == Job.id)
                        .where(GroupMember.group_id == g.id)
                        .order_by(Job.id)
                        .with_for_update(of=Job)
                    )
                )
                for g in groups
            }
            now = svc.now(s)
            workers = list(
                s.scalars(
                    select(Worker)
                    .where(
                        Worker.status == "HEALTHY",
                        Worker.last_heartbeat
                        > now - timedelta(seconds=svc.settings.worker_timeout),
                    )
                    .order_by(Worker.id)
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
            )
            usage = {
                p: (float(c), int(m), int(g))
                for p, c, m, g in s.execute(
                    select(
                        Job.project_id,
                        func.sum(Job.cpu_required),
                        func.sum(Job.memory_required_mb),
                        func.sum(Job.gpu_required),
                    )
                    .where(Job.status.in_(ACTIVE))
                    .group_by(Job.project_id)
                )
            }
            for group in groups:
                jobs = jobs_by_group[group.id]
                if len(jobs) != group.nodes or any(j.status not in WAITING for j in jobs):
                    continue
                project = projects.get(group.project_id) if group.project_id else None
                if group.project_id and project is None:
                    continue
                cpu, mem, gpu = (
                    sum(j.cpu_required for j in jobs),
                    sum(j.memory_required_mb for j in jobs),
                    sum(j.gpu_required for j in jobs),
                )
                used = usage.get(group.project_id, (0.0, 0, 0))
                if project and (
                    not fits(
                        cpu, mem, project.cpu_limit - used[0], project.memory_limit_mb - used[1]
                    )
                    or used[2] + gpu > project.gpu_limit
                ):
                    continue
                plan: list[tuple[Job, Worker, list[GPUDevice]]] = []
                selected: set[str] = set()
                for job in jobs:
                    for worker in workers:
                        overhead = execution_overhead(worker.capabilities)
                        if (
                            worker.id in selected
                            or worker.running_jobs >= svc.settings.worker_max_jobs
                            or not set(job.capabilities) <= set(worker.capabilities)
                            or not fits(
                                job.cpu_required + overhead[0],
                                job.memory_required_mb + overhead[1],
                                min(worker.cpu_available, worker.cpu_total - worker.cpu_reserved),
                                min(
                                    worker.memory_available_mb,
                                    worker.memory_total_mb - worker.memory_reserved_mb,
                                ),
                            )
                        ):
                            continue
                        devices = (
                            list(
                                s.scalars(
                                    select(GPUDevice)
                                    .where(
                                        GPUDevice.worker_id == worker.id,
                                        GPUDevice.enabled.is_(True),
                                        GPUDevice.allocated_to.is_(None),
                                        GPUDevice.memory_mb >= job.gpu_memory_mb,
                                    )
                                    .order_by(GPUDevice.id)
                                    .limit(job.gpu_required)
                                    .with_for_update(skip_locked=True)
                                )
                            )
                            if job.gpu_required
                            else []
                        )
                        if len(devices) != job.gpu_required:
                            continue
                        plan.append((job, worker, devices))
                        selected.add(worker.id)
                        break
                if len(plan) != group.nodes:
                    continue  # No partial attempts or reservations are written.
                for job, worker, devices in plan:
                    overhead = execution_overhead(worker.capabilities)
                    job.attempts_count += 1
                    attempt = Attempt(
                        id=identifier(),
                        job_id=job.id,
                        number=job.attempts_count,
                        worker_id=worker.id,
                        worker_session=worker.session_id,
                        lease_token=identifier(),
                        lease_expires_at=now + timedelta(seconds=svc.settings.lease_seconds),
                        status=JobStatus.SCHEDULED,
                        scheduled_at=now,
                        eligible_at=job.eligible_at,
                        logs="",
                        gpu_ids=[d.id for d in devices],
                        cpu_reserved=job.cpu_required + overhead[0],
                        memory_reserved_mb=job.memory_required_mb + overhead[1],
                    )
                    s.add(attempt)
                    s.flush()
                    for device in devices:
                        device.allocated_to = attempt.id
                    worker.cpu_reserved += attempt.cpu_reserved
                    worker.memory_reserved_mb += attempt.memory_reserved_mb
                    worker.running_jobs += 1
                    job.scheduled_at = now
                    svc.transition(s, job, JobStatus.SCHEDULED, now, attempt)
                    assigned += 1
                group.status = "ACTIVE"
                if project:
                    project.dispatch_count += group.nodes
                    usage[group.project_id] = (used[0] + cpu, used[1] + mem, used[2] + gpu)
        return assigned

    def create_session(
        self, body: SessionSubmit, key: str | None
    ) -> tuple[InteractiveSession, bool]:
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        digest, key = fingerprint(body.model_dump()), scoped_key(key)
        with self.svc.factory.begin() as s:
            s.execute(select(Admission).where(Admission.id == 1).with_for_update()).scalar_one()
            old = (
                s.scalar(
                    select(InteractiveSession).where(InteractiveSession.idempotency_key == key)
                )
                if key
                else None
            )
            if old:
                if old.request_hash != digest:
                    raise DomainError(409, "idempotency key has a different session request")
                return old, False
            self.svc.admission(s)
            now = self.svc.now(s)
            command = ["python", "-u", "/output/.strata/runtime.py", body.kind]
            request = body.job.model_copy(
                update={
                    "command": command,
                    "max_retries": 0,
                    "capabilities": sorted(
                        set(body.job.capabilities) | {"runtime-bridge", "python"}
                    ),
                }
            )
            job = self.svc.create_job(s, request, None, digest)
            row = InteractiveSession(
                id=identifier(),
                project_id=project_id(),
                created_by=actor_id(),
                job_id=job.id,
                kind=body.kind,
                specification=body.model_dump(),
                created_at=now,
                last_activity_at=now,
                idle_seconds=body.idle_seconds,
                idempotency_key=key,
                request_hash=digest,
                next_sequence=0,
            )
            s.add(row)
            s.flush()
            return row, True

    def session(self, session_id: str) -> dict[str, Any]:
        from control_plane.api import row_view

        with self.svc.factory() as s:
            row = s.get(InteractiveSession, session_id)
            if row is None:
                raise DomainError(404, "interactive session not found")
            result = row_view(row)
            result["job"] = row_view(self.svc.job(s, row.job_id))
            return result

    def submit_cell(self, session_id: str, body: CellSubmit | ProxySubmit, key: str) -> SessionCell:
        if not 1 <= len(key) <= 256:
            raise DomainError(422, "cell idempotency key must contain 1..256 characters")
        payload, digest = body.model_dump(), fingerprint(body.model_dump())
        with self.svc.factory.begin() as s:
            row = s.scalar(
                select(InteractiveSession)
                .where(InteractiveSession.id == session_id)
                .with_for_update()
            )
            if row is None:
                raise DomainError(404, "interactive session not found")
            old = s.scalar(
                select(SessionCell).where(
                    SessionCell.session_id == row.id, SessionCell.idempotency_key == key
                )
            )
            if old:
                if old.request_hash != digest:
                    raise DomainError(409, "cell idempotency key has different content")
                return old
            if (isinstance(body, CellSubmit)) != (row.kind == "python"):
                raise DomainError(422, "request does not match the session runtime")
            job = self.svc.job(s, row.job_id, lock=True)
            now = self.svc.now(s)
            if job.status in TERMINAL or job.status == JobStatus.CANCEL_REQUESTED:
                raise DomainError(409, "session is closed; create a fresh session")
            if row.next_sequence >= 1000:
                raise DomainError(429, "session has reached its 1000-message limit")
            if s.scalar(
                select(SessionCell.id).where(
                    SessionCell.session_id == row.id, SessionCell.status.in_(["QUEUED", "RUNNING"])
                )
            ):
                raise DomainError(409, "wait for the current cell or request to finish")
            cell = SessionCell(
                id=identifier(),
                project_id=row.project_id,
                session_id=row.id,
                sequence=row.next_sequence,
                payload=payload,
                status="QUEUED",
                result={},
                created_at=now,
                timeout_seconds=body.timeout_seconds,
                idempotency_key=key,
                request_hash=digest,
            )
            row.next_sequence += 1
            row.last_activity_at = now
            s.add(cell)
            s.flush()
            return cell

    def stop_session(self, session_id: str) -> dict[str, Any]:
        with self.svc.factory.begin() as s:
            row = s.scalar(
                select(InteractiveSession)
                .where(InteractiveSession.id == session_id)
                .with_for_update()
            )
            if row is None:
                raise DomainError(404, "interactive session not found")
            self.svc.cancel_single(row.job_id, s)
        return self.session(session_id)

    def tick(self) -> None:
        with self.svc.factory() as s:
            groups = list(
                s.scalars(
                    select(ComputeGroup.id).where(
                        ComputeGroup.status.in_(["ACTIVE", "CANCELLING", "QUEUED"])
                    )
                )
            )
            sessions = list(
                s.scalars(
                    select(InteractiveSession.id)
                    .join(Job, Job.id == InteractiveSession.job_id)
                    .where(
                        or_(
                            Job.status.not_in(TERMINAL),
                            InteractiveSession.id.in_(
                                select(SessionCell.session_id).where(
                                    SessionCell.status.in_(["QUEUED", "RUNNING"])
                                )
                            ),
                        )
                    )
                )
            )
        for group_id in groups:
            with self.svc.factory.begin() as s:
                group = s.scalar(
                    select(ComputeGroup).where(ComputeGroup.id == group_id).with_for_update()
                )
                assert group is not None
                jobs = list(
                    s.scalars(
                        select(Job)
                        .join(GroupMember, GroupMember.job_id == Job.id)
                        .where(GroupMember.group_id == group.id)
                    )
                )
                states = {j.status for j in jobs}
                if states == {JobStatus.SUCCEEDED}:
                    group.status = "SUCCEEDED"
                elif (
                    states
                    & {
                        JobStatus.FAILED,
                        JobStatus.TIMED_OUT,
                        JobStatus.CANCELLED,
                        JobStatus.CANCEL_REQUESTED,
                    }
                    or group.status == "CANCELLING"
                ):
                    for job in sorted(jobs, key=lambda j: j.id):
                        self.svc.cancel_single(job.id, s)
                    group.status = (
                        "FAILED"
                        if states & {JobStatus.FAILED, JobStatus.TIMED_OUT}
                        else "CANCELLING"
                    )
                    if all(j.status in TERMINAL for j in jobs):
                        group.status = (
                            "FAILED"
                            if states & {JobStatus.FAILED, JobStatus.TIMED_OUT}
                            else "CANCELLED"
                        )
        for session_id in sessions:
            with self.svc.factory.begin() as s:
                row = s.scalar(
                    select(InteractiveSession)
                    .where(InteractiveSession.id == session_id)
                    .with_for_update()
                )
                assert row is not None
                now = self.svc.now(s)
                cell = s.scalar(
                    select(SessionCell).where(
                        SessionCell.session_id == row.id, SessionCell.status == "RUNNING"
                    )
                )
                overdue = (
                    cell
                    and cell.started_at
                    and now >= cell.started_at + timedelta(seconds=cell.timeout_seconds)
                )
                closed = self.svc.job(s, row.job_id).status in TERMINAL | {
                    JobStatus.CANCEL_REQUESTED
                }
                if (
                    closed
                    or overdue
                    or now >= row.last_activity_at + timedelta(seconds=row.idle_seconds)
                ):
                    self.svc.cancel_single(row.job_id, s)
                    if cell:
                        cell.status, cell.result = "FAILED", {"error": "session deadline exceeded"}
                        cell.finished_at = now
                    for queued in s.scalars(
                        select(SessionCell).where(
                            SessionCell.session_id == row.id, SessionCell.status == "QUEUED"
                        )
                    ):
                        queued.status, queued.result, queued.finished_at = (
                            "FAILED",
                            {"error": "session closed"},
                            now,
                        )

    def exchange(self, attempt_id: str, session_id: str, token: str, raw: bytes) -> bytes:
        if len(raw) > MESSAGE_BYTES:
            raise DomainError(413, "runtime message exceeds 16 KiB")
        try:
            value = json.loads(raw)
            kind = value["kind"]
        except (ValueError, KeyError, TypeError) as exc:
            raise DomainError(422, "invalid runtime message") from exc
        with self.svc.factory.begin() as s:
            initial = s.get(Attempt, attempt_id)
            if initial is None:
                raise DomainError(404, "attempt not found")
            if kind == "collective":
                message = CollectiveMessage.model_validate(value)
                member = s.scalar(select(GroupMember).where(GroupMember.job_id == initial.job_id))
                if member is None:
                    raise DomainError(409, "attempt is not a collective participant")
                group = s.scalar(
                    select(ComputeGroup).where(ComputeGroup.id == member.group_id).with_for_update()
                )
                assert group is not None
                job, _, _, _ = self.svc.credentials(s, attempt_id, session_id, token)
                if group.status != "ACTIVE" or job.status != JobStatus.RUNNING:
                    raise DomainError(409, "compute group is not active")
                round_ = s.get(CollectiveRound, (group.id, message.sequence))
                if round_ is None:
                    if message.sequence != group.next_sequence:
                        raise DomainError(409, "collectives must be submitted in sequence")
                    round_ = CollectiveRound(
                        group_id=group.id,
                        sequence=message.sequence,
                        operation=message.operation,
                        contributions={},
                        result=None,
                    )
                    s.add(round_)
                if round_.operation != message.operation:
                    raise DomainError(409, "collective operations do not match")
                contributions = dict(round_.contributions)
                rank = str(member.rank)
                if rank in contributions and contributions[rank] != message.values:
                    raise DomainError(409, "rank already submitted different collective values")
                if contributions and len(next(iter(contributions.values()))) != len(message.values):
                    raise DomainError(409, "collective vector widths do not match")
                contributions[rank] = message.values
                round_.contributions = contributions
                if len(contributions) == group.nodes and round_.result is None:
                    vectors = [contributions[str(i)] for i in range(group.nodes)]
                    try:
                        reduction: list[float] = []
                        for column in zip(*vectors, strict=True):
                            reduction.append(
                                math.fsum(column)
                                if message.operation == "sum"
                                else min(column)
                                if message.operation == "min"
                                else max(column)
                            )
                    except OverflowError as exc:
                        raise DomainError(422, "collective result overflow") from exc
                    if not all(math.isfinite(v) for v in reduction):
                        raise DomainError(422, "collective result is not finite")
                    round_.result = reduction
                    group.next_sequence += 1
                response = (
                    {"sequence": message.sequence, "values": round_.result}
                    if round_.result is not None
                    else None
                )
            elif kind == "session":
                session_message = SessionMessage.model_validate(value)
                row = s.scalar(
                    select(InteractiveSession)
                    .where(InteractiveSession.job_id == initial.job_id)
                    .with_for_update()
                )
                if row is None:
                    raise DomainError(409, "attempt is not an interactive session")
                job, _, _, now = self.svc.credentials(s, attempt_id, session_id, token)
                if job.status != JobStatus.RUNNING:
                    raise DomainError(409, "session is not running")
                if session_message.sequence:
                    previous = s.scalar(
                        select(SessionCell).where(
                            SessionCell.session_id == row.id,
                            SessionCell.sequence == session_message.sequence - 1,
                        )
                    )
                    result = session_message.result
                    if previous is None or not result or result.get("cell_id") != previous.id:
                        raise DomainError(409, "session result is out of sequence")
                    if previous.status in {"SUCCEEDED", "FAILED"}:
                        if previous.result != result:
                            raise DomainError(409, "session result has different content")
                    elif previous.status == "RUNNING":
                        previous.status = "FAILED" if result.get("error") else "SUCCEEDED"
                        previous.result, previous.finished_at = result, now
                        row.last_activity_at = now
                    else:
                        raise DomainError(409, "session cell was not dispatched")
                elif session_message.result is not None:
                    raise DomainError(409, "initial kernel request cannot contain a result")
                cell = s.scalar(
                    select(SessionCell).where(
                        SessionCell.session_id == row.id,
                        SessionCell.sequence == session_message.sequence,
                    )
                )
                response = None
                if cell and cell.status in {"QUEUED", "RUNNING"}:
                    if cell.status == "QUEUED":
                        cell.status, cell.started_at = "RUNNING", now
                    response = {"sequence": cell.sequence, "cell_id": cell.id, **cell.payload}
            else:
                raise DomainError(422, "unknown runtime message kind")
            return (
                json.dumps(response, allow_nan=False, ensure_ascii=False).encode()
                if response is not None
                else b""
            )


def runtime_assignment(s: Any, job: Job) -> tuple[str, bytes]:
    member = s.scalar(select(GroupMember).where(GroupMember.job_id == job.id))
    row = s.scalar(select(InteractiveSession).where(InteractiveSession.job_id == job.id))
    context: dict[str, Any] = {}
    if member:
        group = s.get(ComputeGroup, member.group_id)
        context = {
            "kind": "collective",
            "group_id": member.group_id,
            "rank": member.rank,
            "size": group.nodes,
        }
    elif row:
        context = {
            "kind": row.kind,
            "port": row.specification["service_port"],
            "command": row.specification["job"]["command"],
        }
    if not context:
        return "", b""
    import strata_sdk.runtime

    code = Path(strata_sdk.runtime.__file__).read_bytes()
    return json.dumps(context), code
