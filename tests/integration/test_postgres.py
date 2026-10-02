from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from sqlalchemy import func, select

from control_plane.models import Attempt, Job, JobEvent, Worker
from control_plane.schemas import JobSubmit
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import assigned, complete, register, submit

pytestmark = pytest.mark.postgres


def test_concurrent_duplicate_submission_is_one_job(postgres_service):
    svc = postgres_service
    body = JobSubmit(name="compute", image="strata/python-workloads:local", command=["run"])
    with ThreadPoolExecutor(max_workers=12) as pool:
        ids = list(pool.map(lambda _: svc.submit(body, "same-key")[0].id, range(40)))
    assert len(set(ids)) == 1
    with svc.factory() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 1


def test_concurrent_admission_limit_is_not_exceeded(postgres_service):
    svc = postgres_service
    svc.settings.queue_limit = 5

    def send(_):
        try:
            return submit(svc).id
        except DomainError as exc:
            assert exc.code == 429
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(send, range(40)))
    assert len([r for r in results if r]) == 5


def test_two_schedulers_assign_100_jobs_once_and_reserve_atomically(postgres_service):
    svc = postgres_service
    svc.settings.scheduler_batch_size = 7
    svc.settings.worker_max_jobs = 128
    register(svc, "worker-a", cpu=32, memory=65536)
    register(svc, "worker-b", cpu=32, memory=65536)
    for _ in range(100):
        submit(svc, resources={"cpu": 0.5, "memory_mb": 32})
    barrier = Barrier(2)

    def run():
        barrier.wait()
        scheduler = Scheduler(svc)
        for _ in range(40):
            scheduler.schedule()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: run(), range(2)))
    with svc.factory() as session:
        attempts = list(session.scalars(select(Attempt)))
        assert len(attempts) == 100
        assert len({a.job_id for a in attempts}) == 100
        workers = list(session.scalars(select(Worker)))
        assert sum(w.cpu_reserved for w in workers) == 50
        assert all(w.cpu_reserved <= w.cpu_total for w in workers)


def test_cancel_completion_race_uses_commit_order(postgres_service):
    svc = postgres_service
    worker = register(svc)
    for _ in range(15):
        job, _, a = assigned(svc, worker=worker)
        barrier = Barrier(2)

        def finish(barrier=barrier, a=a):
            barrier.wait()
            complete(svc, worker, a)

        def cancel(barrier=barrier, job=job):
            barrier.wait()
            svc.cancel(job.id)

        with ThreadPoolExecutor(max_workers=2) as pool:
            a_future = pool.submit(finish)
            b_future = pool.submit(cancel)
            a_future.result()
            b_future.result()
        assert svc.get_job(job.id).status in {"SUCCEEDED", "CANCELLED"}
        with svc.factory() as session:
            assert session.get(Worker, worker.id).cpu_reserved == 0


def test_timeout_and_worker_loss_have_one_recovery(postgres_service):
    svc = postgres_service
    job, _, _ = assigned(svc, job=submit(svc, timeout_seconds=1, max_retries=0))
    svc.clock.advance(15)
    barrier = Barrier(2)

    def recover():
        barrier.wait()
        return Scheduler(svc).recover()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: recover(), range(2)))
    assert sum(results) == 1 and svc.get_job(job.id).status == "TIMED_OUT"
    with svc.factory() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(JobEvent)
                .where(JobEvent.job_id == job.id, JobEvent.kind == "JOB_TIMED_OUT")
            )
            == 1
        )


def test_server_uses_postgres_clock(postgres_service):
    postgres_service.clock = None
    with postgres_service.factory() as session:
        assert postgres_service.now(session).tzinfo is not None
