import hashlib
import json
import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from opentelemetry.propagate import extract, inject
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from control_plane.access import actor_id, audit, project_id, scoped_key
from control_plane.config import Settings
from control_plane.domain import (
    ACTIVE,
    TERMINAL,
    WAITING,
    JobStatus,
    retry_delay,
    validate_transition,
)
from control_plane.errors import AdmissionPaused
from control_plane.errors import DomainError as DomainError
from control_plane.models import (
    Admission,
    Artifact,
    Attempt,
    GPUDevice,
    Job,
    JobEvent,
    ProvisionedWorker,
    Worker,
    WorkerHeartbeat,
    WorkerPool,
    identifier,
)
from control_plane.schemas import Completion, Heartbeat, JobSubmit, WorkerRegister
from control_plane.tracing import tracer

logger = logging.getLogger(__name__)


class EngineService:
    def __init__(
        self,
        factory: sessionmaker[Session],
        settings: Settings,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.factory = factory
        self.settings = settings
        self.clock = clock

    def now(self, session: Session) -> datetime:
        if self.clock:
            return self.clock()
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            value: datetime | None = session.scalar(select(func.clock_timestamp()))
            assert value is not None
            return value
        return datetime.now(UTC)

    def event(
        self,
        session: Session,
        job: Job,
        kind: str,
        now: datetime,
        attempt: Attempt | None = None,
        **details: str | int | float,
    ) -> None:
        session.add(
            JobEvent(
                job_id=job.id,
                attempt_id=attempt.id if attempt else None,
                kind=kind,
                created_at=now,
                details=details,
                event_uid=(event_uid := identifier()),
            )
        )
        if kind.startswith("JOB_") and kind.removeprefix("JOB_") in TERMINAL:
            from control_plane.webhooks import enqueue

            enqueue(self, session, job, kind, event_uid, now, attempt)
        if actor_id():
            audit(session, now, kind, job.id, job.project_id, **details)
        logger.info(
            kind.lower(),
            extra={
                "job_id": job.id,
                "attempt_id": attempt.id if attempt else None,
                "worker_id": attempt.worker_id if attempt else None,
            },
        )

    def transition(
        self,
        session: Session,
        job: Job,
        status: JobStatus,
        now: datetime,
        attempt: Attempt | None = None,
        **details: str | int | float,
    ) -> None:
        if not (
            job.execution_kind == "barrier"
            and job.status in WAITING
            and status == JobStatus.SUCCEEDED
        ):
            validate_transition(job.status, status)
        job.status = status
        self.event(session, job, f"JOB_{status}", now, attempt, **details)

    def admission(self, session: Session, count: int = 1) -> None:
        state = session.execute(
            select(Admission).where(Admission.id == 1).with_for_update()
        ).scalar_one()
        if not state.accepting_jobs:
            raise AdmissionPaused()
        depth = (
            session.scalar(
                select(func.count())
                .select_from(Job)
                .where(
                    Job.status.not_in([str(s) for s in TERMINAL]),
                )
                .execution_options(strata_unscoped=True)
            )
            or 0
        )
        if depth + count > self.settings.queue_limit:
            raise DomainError(429, "insufficient queue capacity: outstanding job limit reached")
        scope = project_id()
        if scope:
            from control_plane.models import Project

            project = session.scalar(select(Project).where(Project.id == scope).with_for_update())
            if project is None or not project.enabled:
                raise DomainError(404, "project not found")
            depth = (
                session.scalar(
                    select(func.count())
                    .select_from(Job)
                    .where(Job.project_id == scope, Job.status.not_in(TERMINAL))
                )
                or 0
            )
            if depth + count > project.queue_limit:
                raise DomainError(429, "project outstanding job quota reached")

    def submit(self, request: JobSubmit, key: str | None = None) -> tuple[Job, bool]:
        if request.image not in self.settings.allowed_images:
            raise DomainError(422, "image is not allowlisted")
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        key = scoped_key(key)
        specification = request.model_dump(exclude_none=True)
        if request.dependency_policy == "all_succeeded":
            specification.pop("dependency_policy")  # Preserve v2 idempotency hashes.
        for field in ("gpus", "gpu_memory_mb"):
            if specification["resources"].get(field) == 0:
                specification["resources"].pop(field)
        digest = hashlib.sha256(
            json.dumps(
                specification,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self.factory.begin() as session:
            # A single admission row makes both queue limits and concurrent duplicate POSTs atomic.
            session.execute(
                select(Admission).where(Admission.id == 1).with_for_update()
            ).scalar_one()
            if key is not None:
                existing = session.scalar(select(Job).where(Job.idempotency_key == key))
                if existing:
                    if existing.request_hash != digest:
                        raise DomainError(409, "idempotency key was used with a different request")
                    return existing, False
            self.admission(session)
            return self.create_job(session, request, key, digest), True

    def create_job(
        self,
        session: Session,
        request: JobSubmit,
        key: str | None,
        digest: str,
        campaign_id: str | None = None,
        parameters: dict[str, str | int | float | bool] | None = None,
    ) -> Job:
        from control_plane.models import DatasetVersion, JobDependency

        if request.image not in self.settings.allowed_images:
            raise DomainError(422, "image is not allowlisted")
        for item in request.inputs:
            version = session.get(DatasetVersion, item.version_id)
            if version is None or version.status != "SEALED":
                raise DomainError(422, "job inputs must refer to sealed dataset versions")
        parents = set(request.depends_on) | {
            item.job_id for item in request.artifact_inputs if not item.artifact_id
        }
        if request.dependency_policy == "any_failed" and not parents:
            raise DomainError(422, "any_failed requires at least one dependency")
        if request.dependency_policy != "all_succeeded" and any(
            not value.artifact_id for value in request.artifact_inputs
        ):
            raise DomainError(422, "conditional jobs must pin terminal artifacts explicitly")
        for artifact_input in request.artifact_inputs:
            if artifact_input.artifact_id:
                artifact = session.get(Artifact, artifact_input.artifact_id)
                attempt = session.get(Attempt, artifact.attempt_id) if artifact else None
                if (
                    artifact is None
                    or artifact.job_id != artifact_input.job_id
                    or artifact.name != artifact_input.name
                    or attempt is None
                    or attempt.status not in TERMINAL
                ):
                    raise DomainError(422, "pinned input must refer to a terminal attempt artifact")
        for parent_id in parents:
            parent = self.job(session, parent_id)
            if parent.execution_kind == "barrier" and any(
                item.job_id == parent_id and len(item.alias) > 24
                for item in request.artifact_inputs
            ):
                raise DomainError(
                    422, "expansion artifact aliases must contain at most 24 characters"
                )
        now = self.now(session)
        carrier: dict[str, str] = {}
        inject(carrier)
        job = Job(
            id=identifier(),
            project_id=project_id(),
            created_by=actor_id(),
            name=request.name,
            image=request.image,
            expected_image_digest=request.expected_image_digest,
            dependency_policy=request.dependency_policy,
            command=request.command,
            capabilities=sorted(
                set(request.capabilities)
                | ({"dataset-inputs"} if request.inputs or request.artifact_inputs else set())
                | ({"image-pinning"} if request.expected_image_digest else set())
                | ({"gpu-nvidia"} if request.resources.gpus else set())
            ),
            status=JobStatus.PENDING,
            priority=request.priority,
            cpu_required=request.resources.cpu,
            memory_required_mb=request.resources.memory_mb,
            gpu_required=request.resources.gpus,
            gpu_memory_mb=request.resources.gpu_memory_mb,
            max_retries=request.max_retries,
            timeout_seconds=request.timeout_seconds,
            idempotency_key=key,
            request_hash=digest,
            created_at=now,
            eligible_at=now,
            attempts_count=0,
            retry_count=0,
            traceparent=carrier.get("traceparent", ""),
            campaign_id=campaign_id,
            parameters=parameters or {},
            inputs=[item.model_dump(exclude_none=True) for item in request.inputs]
            + [item.model_dump(exclude_none=True) for item in request.artifact_inputs],
            depends_on=sorted(parents),
        )
        session.add(job)
        session.flush()
        for parent_id in job.depends_on:
            session.add(JobDependency(job_id=job.id, parent_id=parent_id))
        self.event(session, job, "JOB_CREATED", now)
        self.transition(session, job, JobStatus.QUEUED, now)
        return job

    def job(self, session: Session, job_id: str, lock: bool = False) -> Job:
        query = select(Job).where(Job.id == job_id)
        if lock:
            query = query.with_for_update()
        job = session.scalar(query)
        if job is None:
            raise DomainError(404, "job not found")
        return job

    def get_job(self, job_id: str) -> Job:
        with self.factory() as session:
            return self.job(session, job_id)

    def list_jobs(self, status: JobStatus | None, limit: int, offset: int) -> list[Job]:
        with self.factory() as session:
            query = select(Job).order_by(Job.created_at.desc(), Job.id).limit(limit).offset(offset)
            if status:
                query = query.where(Job.status == status)
            return list(session.scalars(query))

    def worker(self, session: Session, worker_id: str, lock: bool = False) -> Worker:
        query = select(Worker).where(Worker.id == worker_id)
        if lock:
            query = query.with_for_update()
        worker = session.scalar(query)
        if worker is None:
            raise DomainError(404, "worker not found")
        return worker

    def register(self, request: WorkerRegister) -> Worker:
        with self.factory.begin() as session:
            # Serialize creation, including two first registrations of the same worker ID.
            session.execute(
                select(Admission).where(Admission.id == 1).with_for_update()
            ).scalar_one()
            managed = session.get(ProvisionedWorker, request.worker_id)
            if managed is not None:
                if managed.phase == "REMOVED":
                    raise DomainError(409, "provisioned worker was retired")
                pool = session.get(WorkerPool, managed.pool_id)
                assert pool is not None
                if (
                    request.cpu_total != pool.cpu_per_worker
                    or request.memory_total_mb != pool.memory_per_worker_mb
                    or request.gpus
                    or "worker-" + pool.kind not in request.capabilities
                ):
                    raise DomainError(422, "worker registration does not match its pool allocation")
            worker = session.scalar(
                select(Worker)
                .where(
                    Worker.id == request.worker_id,
                )
                .with_for_update()
            )
            now = self.now(session)
            if (
                worker
                and worker.status in {"HEALTHY", "DRAINING"}
                and (now - worker.last_heartbeat).total_seconds() < self.settings.worker_timeout
            ):
                raise DomainError(409, "worker ID already has a live session")
            if worker is None:
                worker = Worker(
                    id=request.worker_id,
                    cpu_reserved=0,
                    memory_reserved_mb=0,
                    running_jobs=0,
                    heartbeats_count=0,
                    failures_count=0,
                )
                session.add(worker)
            worker.session_id = identifier()
            worker.status = "DRAINING" if managed and managed.phase == "DRAINING" else "HEALTHY"
            worker.cpu_total = request.cpu_total
            worker.memory_total_mb = request.memory_total_mb
            worker.cpu_available = request.cpu_total
            worker.memory_available_mb = request.memory_total_mb
            worker.capabilities = request.capabilities
            worker.last_heartbeat = now
            session.flush()
            for known_gpu in session.scalars(
                select(GPUDevice).where(GPUDevice.worker_id == worker.id).with_for_update()
            ):
                known_gpu.enabled = False
            for reported in request.gpus:
                device = session.scalar(
                    select(GPUDevice).where(GPUDevice.id == reported.id).with_for_update()
                )
                if device is not None and device.worker_id != worker.id:
                    owner = session.get(Worker, device.worker_id)
                    if device.allocated_to or (
                        owner
                        and owner.status in {"HEALTHY", "DRAINING"}
                        and (now - owner.last_heartbeat).total_seconds()
                        < self.settings.worker_timeout
                    ):
                        raise DomainError(409, "GPU is already owned by another live worker")
                if device is None:
                    device = GPUDevice(
                        id=reported.id,
                        worker_id=worker.id,
                        name=reported.name,
                        memory_mb=reported.memory_mb,
                        enabled=True,
                    )
                    session.add(device)
                else:
                    device.worker_id, device.name = worker.id, reported.name
                    device.memory_mb, device.enabled = reported.memory_mb, True
            if request.gpus and "gpu-nvidia" not in worker.capabilities:
                worker.capabilities = [*worker.capabilities, "gpu-nvidia"]
            return worker

    def credentials(
        self,
        session: Session,
        attempt_id: str,
        session_id: str,
        token: str,
    ) -> tuple[Job, Attempt, Worker, datetime]:
        initial = session.get(Attempt, attempt_id)
        if initial is None:
            raise DomainError(404, "attempt not found")
        job = self.job(session, initial.job_id, lock=True)
        attempt = session.scalar(
            select(Attempt)
            .where(
                Attempt.id == attempt_id,
            )
            .execution_options(populate_existing=True)
        )
        assert attempt is not None
        worker = self.worker(session, attempt.worker_id, lock=True)
        now = self.now(session)
        if (
            attempt.lease_token != token
            or attempt.worker_session != session_id
            or worker.session_id != session_id
            or worker.status not in {"HEALTHY", "DRAINING"}
            or (now - worker.last_heartbeat).total_seconds() >= self.settings.worker_timeout
            or attempt.status not in ACTIVE
            or job.status not in ACTIVE
            or attempt.lease_expires_at <= now
        ):
            raise DomainError(409, "stale or expired lease")
        return job, attempt, worker, now

    def heartbeat(self, worker_id: str, request: Heartbeat) -> dict[str, Any]:
        with self.factory.begin() as session:
            worker = self.worker(session, worker_id, lock=True)
            now = self.now(session)
            if (
                worker.session_id != request.session_id
                or worker.status not in {"HEALTHY", "DRAINING"}
                or (now - worker.last_heartbeat).total_seconds() >= self.settings.worker_timeout
            ):
                raise DomainError(409, "worker session expired; register again")
            if request.cpu_available > worker.cpu_total or (
                request.memory_available_mb > worker.memory_total_mb
            ):
                raise DomainError(422, "available resources exceed registered capacity")
            worker.cpu_available = request.cpu_available
            worker.memory_available_mb = request.memory_available_mb
            worker.last_heartbeat = now
            worker.heartbeats_count += 1
            session.add(
                WorkerHeartbeat(
                    worker_id=worker.id,
                    session_id=worker.session_id,
                    created_at=now,
                    cpu_available=request.cpu_available,
                    memory_available_mb=request.memory_available_mb,
                    running_jobs=len(request.leases),
                )
            )
        commands: list[dict[str, Any]] = []
        for lease in request.leases:
            try:
                with self.factory.begin() as session:
                    job, attempt, _, now = self.credentials(
                        session,
                        lease.attempt_id,
                        request.session_id,
                        lease.lease_token,
                    )
                    if attempt.worker_id != worker_id:
                        raise DomainError(409, "lease belongs to another worker")
                    attempt.lease_expires_at = now + timedelta(seconds=self.settings.lease_seconds)
                    commands.append(
                        {
                            "attempt_id": attempt.id,
                            "valid": True,
                            "cancel": job.status == JobStatus.CANCEL_REQUESTED,
                            "lease_seconds": self.settings.lease_seconds,
                        }
                    )
            except DomainError:
                commands.append({"attempt_id": lease.attempt_id, "valid": False, "cancel": True})
        return {"commands": commands}

    def assignments(self, worker_id: str, session_id: str) -> list[dict[str, Any]]:
        from control_plane.runtimes import runtime_assignment

        with self.factory() as session:
            worker = self.worker(session, worker_id)
            now = self.now(session)
            if (
                worker.session_id != session_id
                or worker.status not in {"HEALTHY", "DRAINING"}
                or (now - worker.last_heartbeat).total_seconds() >= self.settings.worker_timeout
            ):
                raise DomainError(409, "worker session expired")
            rows = session.execute(
                select(Attempt, Job)
                .join(Job, Job.id == Attempt.job_id)
                .where(
                    Attempt.worker_id == worker_id,
                    Attempt.worker_session == session_id,
                    Attempt.status == JobStatus.SCHEDULED,
                    Attempt.lease_expires_at > now,
                )
                .order_by(Attempt.scheduled_at)
                .limit(self.settings.scheduler_batch_size)
            )
            return [
                {
                    "attempt_id": a.id,
                    "lease_token": a.lease_token,
                    "job_id": j.id,
                    "image": j.image,
                    "expected_image_digest": j.expected_image_digest or "",
                    "gpu_ids": a.gpu_ids,
                    "command": j.command,
                    "cpu": j.cpu_required,
                    "memory_mb": j.memory_required_mb,
                    "timeout_seconds": j.timeout_seconds,
                    "lease_seconds": max(0, (a.lease_expires_at - now).total_seconds()),
                    "traceparent": j.traceparent,
                    "has_inputs": bool(j.inputs),
                    "runtime_context": (runtime := runtime_assignment(session, j))[0],
                    "runtime_code": runtime[1],
                }
                for a, j in rows
            ]

    def start(self, attempt_id: str, session_id: str, token: str, image_digest: str = "") -> None:
        if image_digest and not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest):
            raise DomainError(422, "invalid resolved image digest")
        with self.factory.begin() as session:
            job, attempt, _, now = self.credentials(session, attempt_id, session_id, token)
            if attempt.status == JobStatus.RUNNING:
                return  # Retrying a lost start response is safe.
            if job.status != JobStatus.SCHEDULED:
                raise DomainError(409, "attempt cannot start")
            if job.expected_image_digest and image_digest != job.expected_image_digest:
                raise DomainError(409, "resolved image does not match the pinned execution")
            from control_plane.inputs import resolved_manifest

            snapshot = attempt.provenance.get("inputs")
            if snapshot is None:
                snapshot = resolved_manifest(self, session, job)
            attempt.provenance = {
                "inputs": snapshot,
                "image_digest": image_digest or None,
                "gpus": attempt.gpu_ids,
            }
            attempt.status = JobStatus.RUNNING
            attempt.started_at = now
            job.started_at = now
            self.transition(session, job, JobStatus.RUNNING, now, attempt)

    def release(self, job: Job, worker: Worker, attempt: Attempt) -> None:
        worker.running_jobs = max(0, worker.running_jobs - 1)
        worker.cpu_reserved = max(
            0, worker.cpu_reserved - (attempt.cpu_reserved or job.cpu_required)
        )
        worker.memory_reserved_mb = max(
            0, worker.memory_reserved_mb - (attempt.memory_reserved_mb or job.memory_required_mb)
        )
        if not worker.running_jobs:
            # Repeated fractional CPU charges must not accumulate a phantom reservation.
            worker.cpu_reserved = 0
            worker.memory_reserved_mb = 0

    def finish(
        self,
        session: Session,
        job: Job,
        attempt: Attempt,
        worker: Worker,
        outcome: JobStatus,
        now: datetime,
        reason: str,
        exit_code: int | None = None,
    ) -> None:
        if job.status == JobStatus.CANCEL_REQUESTED:
            outcome = JobStatus.CANCELLED
            reason = "cancellation won before completion committed"
        attempt.status = outcome
        attempt.reason = reason
        attempt.exit_code = exit_code
        attempt.finished_at = now
        if attempt.started_at:
            span = tracer.start_span(
                "workload.execution",
                context=extract({"traceparent": job.traceparent}),
                start_time=int(attempt.started_at.timestamp() * 1e9),
                attributes={
                    "job.id": job.id,
                    "worker.id": worker.id,
                    "attempt.number": attempt.number,
                    "outcome": str(outcome),
                },
            )
            span.end(end_time=int(now.timestamp() * 1e9))
        self.release(job, worker, attempt)
        for device in session.scalars(
            select(GPUDevice)
            .where(GPUDevice.allocated_to == attempt.id)
            .order_by(GPUDevice.id)
            .with_for_update()
        ):
            device.allocated_to = None
        if outcome in {JobStatus.FAILED, JobStatus.TIMED_OUT} and job.retry_count < job.max_retries:
            job.retry_count += 1
            job.eligible_at = now + timedelta(
                seconds=retry_delay(
                    job.retry_count,
                    self.settings.retry_base_seconds,
                    self.settings.retry_max_seconds,
                    self.settings.retry_jitter_seconds,
                )
            )
            self.transition(session, job, JobStatus.RETRYING, now, attempt, reason=reason)
        else:
            job.finished_at = now
            self.transition(session, job, outcome, now, attempt, reason=reason)

    def complete(self, attempt_id: str, request: Completion) -> None:
        with self.factory.begin() as session:
            # Completed identical reports are idempotent even if their lease has since expired.
            initial = session.get(Attempt, attempt_id)
            if initial is not None:
                self.job(session, initial.job_id, lock=True)
                session.refresh(initial)
                if initial.status in TERMINAL:
                    if (
                        initial.lease_token == request.lease_token
                        and initial.worker_session == request.session_id
                        and initial.exit_code == request.exit_code
                        and initial.status == request.outcome
                    ):
                        return
                    raise DomainError(409, "attempt already finished")
            job, attempt, worker, now = self.credentials(
                session,
                attempt_id,
                request.session_id,
                request.lease_token,
            )
            if attempt.status != JobStatus.RUNNING and not (
                attempt.status == JobStatus.SCHEDULED and request.outcome == JobStatus.FAILED
            ):
                raise DomainError(409, "attempt has not started")
            if request.outcome == JobStatus.CANCELLED and job.status != JobStatus.CANCEL_REQUESTED:
                raise DomainError(409, "cancellation was not requested")
            self.finish(
                session,
                job,
                attempt,
                worker,
                request.outcome,
                now,
                request.reason or str(request.outcome),
                request.exit_code,
            )

    def cancel(self, job_id: str) -> Job:
        from control_plane.models import GroupMember
        from control_plane.runtimes import RuntimeService

        with self.factory() as session:
            self.job(session, job_id)
            member = session.scalar(select(GroupMember).where(GroupMember.job_id == job_id))
            group_id = member.group_id if member else None
        if group_id:
            RuntimeService(self).cancel_group(group_id)
            return self.get_job(job_id)
        with self.factory.begin() as session:
            return self.cancel_single(job_id, session)

    def cancel_single(self, job_id: str, session: Session) -> Job:
        job = self.job(session, job_id, lock=True)
        # Group reconciliation may have read this job before a concurrent completion.
        session.refresh(job)
        now = self.now(session)
        if job.status in TERMINAL or job.status == JobStatus.CANCEL_REQUESTED:
            return job
        if job.status in WAITING:
            job.finished_at = now
            self.transition(session, job, JobStatus.CANCELLED, now)
        elif job.status == JobStatus.SCHEDULED:
            attempt = session.scalars(
                select(Attempt).where(
                    Attempt.job_id == job.id, Attempt.status == JobStatus.SCHEDULED
                )
            ).one()
            worker = self.worker(session, attempt.worker_id, lock=True)
            self.finish(
                session, job, attempt, worker, JobStatus.CANCELLED, now, "cancelled before start"
            )
        else:
            self.transition(session, job, JobStatus.CANCEL_REQUESTED, now)
        return job

    def retry(self, job_id: str) -> Job:
        from control_plane.models import GroupMember, InteractiveSession

        with self.factory.begin() as session:
            self.admission(session)
            job = self.job(session, job_id, lock=True)
            if session.scalar(
                select(GroupMember.job_id).where(GroupMember.job_id == job.id)
            ) or session.scalar(
                select(InteractiveSession.id).where(InteractiveSession.job_id == job.id)
            ):
                raise DomainError(
                    409, "retry the whole compute group or create a fresh interactive session"
                )
            if job.status not in {JobStatus.FAILED, JobStatus.TIMED_OUT, JobStatus.CANCELLED}:
                raise DomainError(409, "only failed, timed out or cancelled jobs can be retried")
            now = self.now(session)
            job.retry_count = 0
            job.eligible_at = now
            job.started_at = job.scheduled_at = job.finished_at = None
            from control_plane.workflows import reset_barrier

            reset_barrier(session, job, now)
            self.transition(session, job, JobStatus.QUEUED, now, manual=1)
            return job

    def logs(self, attempt_id: str, session_id: str, token: str, content: str) -> None:
        if len(content.encode()) > self.settings.logs_max_bytes:
            raise DomainError(413, "log limit exceeded")
        with self.factory.begin() as session:
            _, attempt, _, _ = self.credentials(session, attempt_id, session_id, token)
            # Snapshot replacement makes retries idempotent; the worker keeps only a bounded tail.
            attempt.logs = content

    def artifact(
        self,
        attempt_id: str,
        session_id: str,
        token: str,
        name: str,
        content_type: str,
        content: bytes,
    ) -> Artifact:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", name):
            raise DomainError(422, "invalid artifact name")
        if len(content_type) > 128:
            raise DomainError(422, "content type is too long")
        if len(content) > self.settings.artifact_max_bytes:
            raise DomainError(413, "artifact limit exceeded")
        with self.factory.begin() as session:
            from control_plane.retention import storage_fence

            storage_fence(session)
            job, attempt, _, now = self.credentials(session, attempt_id, session_id, token)
            existing = session.scalar(
                select(Artifact).where(
                    Artifact.attempt_id == attempt_id,
                    Artifact.name == name,
                )
            )
            digest = hashlib.sha256(content).hexdigest()
            if existing:
                if existing.sha256 != digest:
                    raise DomainError(409, "artifact name already contains different bytes")
                return existing
            from control_plane.quotas import storage_admission

            storage_admission(session, job.project_id, len(content), now=now)
            artifact = Artifact(
                id=identifier(),
                project_id=job.project_id,
                job_id=job.id,
                attempt_id=attempt.id,
                name=name,
                sha256=digest,
                size=len(content),
                content_type=content_type,
                created_at=now,
            )
            from control_plane.storage import BlobStore

            BlobStore(self.settings).put_bytes(digest, content)
            session.add(artifact)
            self.event(session, job, "ARTIFACT_STORED", now, attempt, name=name, size=len(content))
            return artifact
