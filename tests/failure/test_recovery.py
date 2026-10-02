import pytest
from sqlalchemy import select

from control_plane.models import Attempt, Worker
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import assigned, complete, heartbeat, register, submit


def test_worker_loss_reschedules_and_fences_old_generation(service):
    job, first, a = assigned(service)
    second = register(service, "worker-2")
    service.clock.advance(15)
    # A second worker still sends heartbeats; expiry is inclusive at 15 seconds.
    with service.factory.begin() as session:
        session.get(Worker, second.id).last_heartbeat = service.clock()
    assert Scheduler(service).recover() == 1
    assert service.get_job(job.id).status == "RETRYING"
    with service.factory() as session:
        assert session.get(Worker, first.id).status == "LOST"
    service.clock.advance(2)
    Scheduler(service).schedule()
    b = service.assignments(second.id, second.session_id)[0]
    assert b["lease_token"] != a["lease_token"]
    with pytest.raises(DomainError):
        complete(service, first, a)
    service.start(b["attempt_id"], second.session_id, b["lease_token"])
    complete(service, second, b)
    assert service.get_job(job.id).status == "SUCCEEDED"
    with service.factory() as session:
        attempts = list(session.scalars(select(Attempt).order_by(Attempt.number)))
        assert [a.status for a in attempts] == ["FAILED", "SUCCEEDED"]


def test_restart_rejects_old_session_and_keeps_reservation_until_recovery(service):
    job, old, a = assigned(service)
    service.clock.advance(15)
    fresh = register(service)
    assert fresh.session_id != old.session_id
    assert Scheduler(service).recover() == 1
    with pytest.raises(DomainError):
        heartbeat(service, old, a)
    assert service.get_job(job.id).status == "RETRYING"


def test_timeout_with_fresh_worker(service):
    job, worker, a = assigned(service, job=submit(service, timeout_seconds=1, max_retries=0))
    service.clock.advance(6)
    heartbeat(service, worker, a)
    assert Scheduler(service).recover() == 1
    assert Scheduler(service).recover() == 0
    assert service.get_job(job.id).status == "TIMED_OUT"


def test_cancelled_worker_loss_does_not_retry(service):
    job, _, _ = assigned(service)
    service.cancel(job.id)
    service.clock.advance(15)
    Scheduler(service).recover()
    assert service.get_job(job.id).status == "CANCELLED"
