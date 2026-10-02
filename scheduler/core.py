from datetime import timedelta

from sqlalchemy import select

from control_plane.domain import ACTIVE, WAITING, JobStatus, fits
from control_plane.models import Attempt, Job, Worker, identifier
from control_plane.services import EngineService


class Scheduler:
    def __init__(self, service: EngineService) -> None:
        self.service = service

    def recover(self) -> int:
        svc = self.service
        with svc.factory.begin() as session:
            now = svc.now(session)
            workers = session.scalars(
                select(Worker)
                .where(
                    Worker.status == "HEALTHY",
                    Worker.last_heartbeat <= now - timedelta(seconds=svc.settings.worker_timeout),
                )
                .with_for_update(skip_locked=True)
            )
            for worker in workers:
                worker.status = "LOST"
                worker.failures_count += 1
        recovered = 0
        # Read candidates, then lock each job before its worker. All mutations use that order.
        with svc.factory() as session:
            ids = list(session.scalars(select(Attempt.id).where(Attempt.status.in_(ACTIVE))))
        for attempt_id in ids:
            with svc.factory.begin() as session:
                candidate = session.get(Attempt, attempt_id)
                if candidate is None:
                    continue
                job = session.scalar(
                    select(Job)
                    .where(Job.id == candidate.job_id)
                    .with_for_update(
                        skip_locked=True,
                    )
                )
                if job is None:
                    continue
                session.refresh(candidate)
                if candidate.status not in ACTIVE:
                    continue
                worker = svc.worker(session, candidate.worker_id, lock=True)
                now = svc.now(session)
                timed_out = (
                    candidate.started_at is not None
                    and now
                    >= candidate.started_at
                    + timedelta(
                        seconds=job.timeout_seconds + svc.settings.termination_grace_seconds,
                    )
                )
                lost = worker.status == "LOST" or worker.session_id != candidate.worker_session
                if not (timed_out or lost or candidate.lease_expires_at <= now):
                    continue
                reason = (
                    "execution deadline"
                    if timed_out
                    else "worker lost"
                    if lost
                    else "lease expired"
                )
                if lost:
                    svc.event(session, job, "WORKER_LOST", now, candidate)
                svc.finish(
                    session,
                    job,
                    candidate,
                    worker,
                    JobStatus.TIMED_OUT if timed_out else JobStatus.FAILED,
                    now,
                    reason,
                )
                recovered += 1
        return recovered

    def schedule(self) -> int:
        svc = self.service
        assigned = 0
        with svc.factory.begin() as session:
            now = svc.now(session)
            query = select(Job).where(Job.status.in_(WAITING), Job.eligible_at <= now)
            if svc.settings.scheduling_policy != "fifo":
                query = query.order_by(Job.priority.desc())
            jobs = list(
                session.scalars(
                    query.order_by(Job.created_at, Job.id)
                    .limit(
                        svc.settings.scheduler_batch_size,
                    )
                    .with_for_update(skip_locked=True)
                )
            )
            for job in jobs:
                workers = list(
                    session.scalars(
                        select(Worker)
                        .where(
                            Worker.status == "HEALTHY",
                            Worker.last_heartbeat
                            > now - timedelta(seconds=svc.settings.worker_timeout),
                        )
                        .order_by(Worker.id)
                        .with_for_update(skip_locked=True)
                    )
                )
                eligible = [
                    w
                    for w in workers
                    if fits(
                        job.cpu_required,
                        job.memory_required_mb,
                        min(w.cpu_available, w.cpu_total - w.cpu_reserved),
                        min(w.memory_available_mb, w.memory_total_mb - w.memory_reserved_mb),
                    )
                    and set(job.capabilities) <= set(w.capabilities)
                    and w.running_jobs < svc.settings.worker_max_jobs
                ]
                if not eligible:
                    continue

                def score(w: Worker) -> float:
                    return (w.cpu_total - w.cpu_reserved) / w.cpu_total + (
                        w.memory_total_mb - w.memory_reserved_mb
                    ) / w.memory_total_mb

                worker = sorted(
                    eligible,
                    key=score,
                    reverse=svc.settings.scheduling_policy != "best_fit",
                )[0]
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
                )
                session.add(attempt)
                session.flush()
                worker.cpu_reserved += job.cpu_required
                worker.memory_reserved_mb += job.memory_required_mb
                worker.running_jobs += 1
                job.scheduled_at = now
                svc.transition(session, job, JobStatus.SCHEDULED, now, attempt)
                assigned += 1
        return assigned

    def tick(self) -> tuple[int, int]:
        return self.recover(), self.schedule()
