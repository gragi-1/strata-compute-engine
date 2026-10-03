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

from control_plane.config import Settings
from control_plane.domain import (
    ACTIVE,
    TERMINAL,
    WAITING,
    JobStatus,
    retry_delay,
    validate_transition,
)
from control_plane.models import (
    Admission,
    Artifact,
    Attempt,
    Job,
    JobEvent,
    Worker,
    WorkerHeartbeat,
    identifier,
)
from control_plane.schemas import Completion, Heartbeat, JobSubmit, WorkerRegister
from control_plane.tracing import tracer

logger = logging.getLogger(__name__)


class DomainError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        super().__init__(message)


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
            )
        )
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
        validate_transition(job.status, status)
        job.status = status
        self.event(session, job, f"JOB_{status}", now, attempt, **details)

    def admission(self, session: Session) -> None:
        session.execute(select(Admission).where(Admission.id == 1).with_for_update()).scalar_one()
        depth = (
            session.scalar(
                select(func.count())
                .select_from(Job)
                .where(
                    Job.status.not_in([str(s) for s in TERMINAL]),
                )
            )
            or 0
        )
        if depth >= self.settings.queue_limit:
            raise DomainError(429, "outstanding job limit reached")

    def submit(self, request: JobSubmit, key: str | None = None) -> tuple[Job, bool]:
        if request.image not in self.settings.allowed_images:
            raise DomainError(422, "image is not allowlisted")
        if key is not None and not 1 <= len(key) <= 256:
            raise DomainError(422, "idempotency key must contain 1..256 characters")
        digest = hashlib.sha256(
            json.dumps(
                request.model_dump(),
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
        parents = set(request.depends_on) | {item.job_id for item in request.artifact_inputs}
        for parent_id in parents:
            self.job(session, parent_id)
        now = self.now(session)
        carrier: dict[str, str] = {}
        inject(carrier)
        job = Job(
            id=identifier(),
            name=request.name,
            image=request.image,
            command=request.command,
            capabilities=sorted(
                set(request.capabilities)
                | ({"dataset-inputs"} if request.inputs or request.artifact_inputs else set())
            ),
            status=JobStatus.PENDING,
            priority=request.priority,
            cpu_required=request.resources.cpu,
            memory_required_mb=request.resources.memory_mb,
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
            inputs=[item.model_dump() for item in request.inputs]
            + [item.model_dump() for item in request.artifact_inputs],
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
            worker.status = "HEALTHY"
            worker.cpu_total = request.cpu_total
            worker.memory_total_mb = request.memory_total_mb
            worker.cpu_available = request.cpu_total
            worker.memory_available_mb = request.memory_total_mb
            worker.capabilities = request.capabilities
            worker.last_heartbeat = now
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
                    "command": j.command,
                    "cpu": j.cpu_required,
                    "memory_mb": j.memory_required_mb,
                    "timeout_seconds": j.timeout_seconds,
                    "lease_seconds": max(0, (a.lease_expires_at - now).total_seconds()),
                    "traceparent": j.traceparent,
                    "has_inputs": bool(j.inputs),
                }
                for a, j in rows
            ]

    def start(self, attempt_id: str, session_id: str, token: str) -> None:
        with self.factory.begin() as session:
            job, attempt, _, now = self.credentials(session, attempt_id, session_id, token)
            if attempt.status == JobStatus.RUNNING:
                return  # Retrying a lost start response is safe.
            if job.status != JobStatus.SCHEDULED:
                raise DomainError(409, "attempt cannot start")
            attempt.status = JobStatus.RUNNING
            attempt.started_at = now
            job.started_at = now
            self.transition(session, job, JobStatus.RUNNING, now, attempt)

    def release(self, job: Job, worker: Worker) -> None:
        worker.running_jobs = max(0, worker.running_jobs - 1)
        worker.cpu_reserved = max(0, worker.cpu_reserved - job.cpu_required)
        worker.memory_reserved_mb = max(0, worker.memory_reserved_mb - job.memory_required_mb)

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
        self.release(job, worker)
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
        with self.factory.begin() as session:
            job = self.job(session, job_id, lock=True)
            now = self.now(session)
            if job.status in TERMINAL or job.status == JobStatus.CANCEL_REQUESTED:
                return job
            if job.status in WAITING:
                job.finished_at = now
                self.transition(session, job, JobStatus.CANCELLED, now)
            elif job.status == JobStatus.SCHEDULED:
                attempt = session.scalars(
                    select(Attempt).where(
                        Attempt.job_id == job.id,
                        Attempt.status == JobStatus.SCHEDULED,
                    )
                ).one()
                worker = self.worker(session, attempt.worker_id, lock=True)
                self.finish(
                    session,
                    job,
                    attempt,
                    worker,
                    JobStatus.CANCELLED,
                    now,
                    "cancelled before start",
                )
            else:
                self.transition(session, job, JobStatus.CANCEL_REQUESTED, now)
            return job

    def retry(self, job_id: str) -> Job:
        with self.factory.begin() as session:
            self.admission(session)
            job = self.job(session, job_id, lock=True)
            if job.status not in {JobStatus.FAILED, JobStatus.TIMED_OUT, JobStatus.CANCELLED}:
                raise DomainError(409, "only failed, timed out or cancelled jobs can be retried")
            now = self.now(session)
            job.retry_count = 0
            job.eligible_at = now
            job.started_at = job.scheduled_at = job.finished_at = None
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
            artifact = Artifact(
                id=identifier(),
                job_id=job.id,
                attempt_id=attempt.id,
                name=name,
                sha256=digest,
                size=len(content),
                content_type=content_type,
                created_at=now,
            )
            root = self.settings.artifact_root
            root.mkdir(parents=True, exist_ok=True)
            # Content-addressed files: uncommitted uploads cannot corrupt a committed artifact.
            path = root / digest
            if not path.exists():
                temp = root / identifier()
                temp.write_bytes(content)
                temp.replace(path)
            session.add(artifact)
            self.event(session, job, "ARTIFACT_STORED", now, attempt, name=name, size=len(content))
            return artifact
