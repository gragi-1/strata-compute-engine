"""Helper capacity stays charged under concurrent dispatch and capability changes."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from sqlalchemy import select

from control_plane.models import Attempt, Worker
from scheduler.core import Scheduler
from tests.helpers import complete, register, submit


@pytest.mark.parametrize("fixture", ["service", "postgres_service"])
def test_helper_capacity_is_reserved_and_recovered_from_attempt_snapshot(request, fixture):
    svc = request.getfixturevalue(fixture)
    worker = register(svc, cpu=2, memory=160, capabilities=["python", "bounded-output"])
    for _ in range(8):
        submit(svc, resources={"cpu": 0.5, "memory_mb": 48}, max_retries=0)
    barrier = Barrier(2)

    def dispatch():
        barrier.wait(timeout=5)
        return Scheduler(svc).schedule()

    if fixture == "postgres_service":
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sum(pool.map(lambda _: dispatch(), range(2))) == 2
    else:
        assert Scheduler(svc).schedule() == 2
    with svc.factory() as session:
        attempts = list(session.scalars(select(Attempt)))
        assert len(attempts) == 2
        assert all(a.cpu_reserved == 0.51 and a.memory_reserved_mb == 80 for a in attempts)
        row = session.get(Worker, worker.id)
        assert row.cpu_reserved == pytest.approx(1.02) and row.memory_reserved_mb == 160
    assignment = svc.assignments(worker.id, worker.session_id)[0]
    svc.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    complete(svc, worker, assignment)
    assert Scheduler(svc).schedule() == 1
    # A replacement can advertise another capability set. Old charges remain exact.
    svc.clock.advance(svc.settings.worker_timeout + 1)
    register(svc, cpu=2, memory=160, capabilities=["python"])
    assert Scheduler(svc).recover() == 2
    with svc.factory() as session:
        row = session.get(Worker, worker.id)
        assert row.running_jobs == 0 and row.memory_reserved_mb == 0
        assert row.cpu_reserved == pytest.approx(0)


def test_helper_cpu_prevents_an_overcommitted_full_cpu_job(service):
    register(service, cpu=1, memory=512, capabilities=["python", "bounded-output"])
    job = submit(service, resources={"cpu": 1, "memory_mb": 128})
    assert Scheduler(service).schedule() == 0
    assert service.get_job(job.id).status == "QUEUED"
