from datetime import timedelta
from typing import Any

from sqlalchemy import Float, and_, cast, func, or_, select
from sqlalchemy.orm import aliased

from control_plane.domain import ACTIVE, TERMINAL, WAITING, JobStatus, execution_overhead, fits
from control_plane.models import (
    Admission,
    Attempt,
    GPUDevice,
    Job,
    JobDependency,
    Project,
    Worker,
    identifier,
)
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
                    Worker.status.in_(["HEALTHY", "DRAINING"]),
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
        from control_plane.runtimes import RuntimeService

        svc = self.service
        assigned = RuntimeService(svc).schedule_groups()
        with svc.factory.begin() as session:
            # Shared fence permits concurrent schedulers; an administrative pause waits for
            # existing assignments to commit and prevents later assignment transactions.
            state = session.execute(
                select(Admission).where(Admission.id == 1).with_for_update(read=True)
            ).scalar_one()
            if not state.scheduling_enabled:
                return assigned
            now = svc.now(session)
            # Reserve project budgets before job/worker locks. Completion never needs this lock.
            projects = {
                p.id: p
                for p in session.scalars(
                    select(Project)
                    .where(Project.enabled.is_(True))
                    .order_by(Project.id)
                    .with_for_update(skip_locked=True)
                )
            }
            usage = {
                key: [float(cpu), int(memory), int(gpus)]
                for key, cpu, memory, gpus in session.execute(
                    select(
                        Job.project_id,
                        func.sum(Job.cpu_required),
                        func.sum(Job.memory_required_mb),
                        func.sum(Job.gpu_required),
                    )
                    .where(Job.status.in_(ACTIVE), Job.project_id.in_(projects))
                    .group_by(Job.project_id)
                )
            }
            parent = aliased(Job)
            failed = (
                select(JobDependency.job_id)
                .join(parent, parent.id == JobDependency.parent_id)
                .where(
                    parent.status.in_([JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMED_OUT])
                )
            )
            unfinished = (
                select(JobDependency.job_id)
                .join(parent, parent.id == JobDependency.parent_id)
                .where(parent.status.not_in(TERMINAL))
            )
            unsuccessful = (
                select(JobDependency.job_id)
                .join(parent, parent.id == JobDependency.parent_id)
                .where(parent.status != JobStatus.SUCCEEDED)
            )
            blocked = select(Job.id).where(
                or_(
                    and_(Job.dependency_policy == "all_succeeded", Job.id.in_(unsuccessful)),
                    and_(Job.dependency_policy != "all_succeeded", Job.id.in_(unfinished)),
                    and_(Job.dependency_policy == "any_failed", Job.id.not_in(failed)),
                )
            )
            for job in session.scalars(
                select(Job)
                .where(
                    Job.status.in_(WAITING),
                    Job.id.in_(failed),
                    Job.dependency_policy == "all_succeeded",
                )
                .limit(svc.settings.scheduler_batch_size)
                .with_for_update(skip_locked=True)
            ):
                job.finished_at = now
                svc.transition(
                    session, job, JobStatus.FAILED, now, reason="dependency did not succeed"
                )
            for job in session.scalars(
                select(Job)
                .where(
                    Job.status.in_(WAITING),
                    Job.dependency_policy == "any_failed",
                    Job.id.not_in(unfinished),
                    Job.id.not_in(failed),
                )
                .limit(svc.settings.scheduler_batch_size)
                .with_for_update(skip_locked=True)
            ):
                job.finished_at = now
                svc.transition(
                    session,
                    job,
                    JobStatus.CANCELLED,
                    now,
                    reason="dependency condition evaluated false",
                )
            order: list[Any] = [Job.created_at, Job.id]
            if svc.settings.scheduling_policy != "fifo":
                age = (
                    func.extract("epoch", now - Job.eligible_at)
                    if session.bind is not None and session.bind.dialect.name == "postgresql"
                    else (func.julianday(now) - func.julianday(Job.eligible_at)) * 86400
                )
                order.insert(
                    0,
                    (Job.priority + cast(age, Float) / svc.settings.priority_aging_seconds).desc(),
                )
            active_usage = (
                select(
                    Job.project_id.label("project"),
                    func.sum(Job.cpu_required).label("cpu"),
                    func.sum(Job.memory_required_mb).label("memory"),
                    func.sum(Job.gpu_required).label("gpus"),
                )
                .where(Job.status.in_(ACTIVE))
                .group_by(Job.project_id)
                .subquery()
            )
            # Read a capacity snapshot before locking jobs; worker locks still follow job locks.
            available = list(
                session.scalars(
                    select(Worker).where(
                        Worker.status == "HEALTHY",
                        Worker.last_heartbeat
                        > now - timedelta(seconds=svc.settings.worker_timeout),
                        Worker.running_jobs < svc.settings.worker_max_jobs,
                    )
                )
            )
            if not available:
                return assigned
            worker_fits = []
            for w in available:
                overhead_cpu, overhead_memory = execution_overhead(w.capabilities)
                elements = (
                    func.json_array_elements_text(Job.capabilities).table_valued("value")
                    if session.bind is not None and session.bind.dialect.name == "postgresql"
                    else func.json_each(Job.capabilities).table_valued("key", "value")
                )
                unsupported = (
                    select(elements.c.value).where(elements.c.value.not_in(w.capabilities)).exists()
                )
                worker_fits.append(
                    and_(
                        Job.cpu_required + overhead_cpu
                        <= min(w.cpu_available, w.cpu_total - w.cpu_reserved) + 1e-9,
                        Job.memory_required_mb + overhead_memory
                        <= min(w.memory_available_mb, w.memory_total_mb - w.memory_reserved_mb),
                        ~unsupported,
                        Job.gpu_required
                        <= select(func.count())
                        .select_from(GPUDevice)
                        .where(
                            GPUDevice.worker_id == w.id,
                            GPUDevice.enabled.is_(True),
                            GPUDevice.allocated_to.is_(None),
                            GPUDevice.memory_mb >= Job.gpu_memory_mb,
                        )
                        .correlate(Job)
                        .scalar_subquery(),
                    )
                )
            candidates = (
                select(
                    Job.id.label("id"),
                    func.row_number()
                    .over(partition_by=Job.project_id, order_by=order)
                    .label("rank"),
                )
                .outerjoin(Project, Job.project_id == Project.id)
                .outerjoin(active_usage, active_usage.c.project == Job.project_id)
                .where(
                    Job.status.in_(WAITING),
                    Job.eligible_at <= now,
                    Job.execution_kind == "container",
                    Job.id.not_in(blocked),
                    or_(Job.project_id.is_(None), Job.project_id.in_(projects)),
                    or_(
                        Job.project_id.is_(None),
                        and_(
                            Job.cpu_required + func.coalesce(active_usage.c.cpu, 0)
                            <= Project.cpu_limit + 1e-9,
                            Job.memory_required_mb + func.coalesce(active_usage.c.memory, 0)
                            <= Project.memory_limit_mb,
                            Job.gpu_required + func.coalesce(active_usage.c.gpus, 0)
                            <= Project.gpu_limit,
                        ),
                    ),
                    or_(*worker_fits),
                )
                .subquery()
            )
            # Recheck on the locked relation, not only inside the ranked snapshot.
            # PostgreSQL can acquire a newer row after another scheduler commits while
            # the candidate subquery still describes the old QUEUED version.
            query = (
                select(Job)
                .join(candidates, candidates.c.id == Job.id)
                .outerjoin(Project)
                .where(Job.status.in_(WAITING), Job.eligible_at <= now)
            )
            jobs = list(
                session.scalars(
                    query.order_by(
                        candidates.c.rank, func.coalesce(Project.dispatch_count, 0), *order
                    )
                    .limit(
                        svc.settings.scheduler_batch_size,
                    )
                    .with_for_update(of=Job, skip_locked=True)
                )
            )
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
                    .execution_options(populate_existing=True)
                )
            )
            for job in jobs:
                if job.status not in WAITING or job.eligible_at > now:
                    continue
                project = projects.get(job.project_id) if job.project_id else None
                current = usage.setdefault(job.project_id, [0.0, 0, 0])
                if project and not fits(
                    job.cpu_required,
                    job.memory_required_mb,
                    project.cpu_limit - current[0],
                    project.memory_limit_mb - int(current[1]),
                ):
                    continue
                if project and job.gpu_required + current[2] > project.gpu_limit:
                    continue
                eligible = [
                    w
                    for w in workers
                    if fits(
                        job.cpu_required + execution_overhead(w.capabilities)[0],
                        job.memory_required_mb + execution_overhead(w.capabilities)[1],
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

                selected_gpus: list[GPUDevice] = []
                selected_worker = None
                for candidate in sorted(
                    eligible,
                    key=score,
                    reverse=svc.settings.scheduling_policy != "best_fit",
                ):
                    selected_gpus = (
                        list(
                            session.scalars(
                                select(GPUDevice)
                                .where(
                                    GPUDevice.worker_id == candidate.id,
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
                    if len(selected_gpus) == job.gpu_required:
                        selected_worker = candidate
                        break
                if selected_worker is None:
                    continue
                worker = selected_worker
                extra_cpu, extra_memory = execution_overhead(worker.capabilities)
                charged_cpu, charged_memory = (
                    job.cpu_required + extra_cpu,
                    job.memory_required_mb + extra_memory,
                )
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
                    gpu_ids=[gpu.id for gpu in selected_gpus],
                    cpu_reserved=charged_cpu,
                    memory_reserved_mb=charged_memory,
                )
                session.add(attempt)
                session.flush()
                for gpu in selected_gpus:
                    gpu.allocated_to = attempt.id
                worker.cpu_reserved += charged_cpu
                worker.memory_reserved_mb += charged_memory
                worker.running_jobs += 1
                job.scheduled_at = now
                svc.transition(session, job, JobStatus.SCHEDULED, now, attempt)
                if project:
                    project.dispatch_count += 1
                    current[0] += job.cpu_required
                    current[1] += job.memory_required_mb
                    current[2] += job.gpu_required
                assigned += 1
        return assigned

    def tick(self) -> tuple[int, int]:
        from control_plane.runtimes import RuntimeService

        recovered = self.recover()
        RuntimeService(self.service).tick()
        return recovered, self.schedule()
