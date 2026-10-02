import hashlib

import pytest
from sqlalchemy import select

from control_plane.models import Artifact, Attempt, JobEvent, Worker
from control_plane.schemas import Heartbeat, JobSubmit
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import assigned, complete, heartbeat, register, submit


def test_idempotency_and_conflict(service):
    request = JobSubmit(name="compute", image="strata/python-workloads:local", command=["run"])
    first, created = service.submit(request, "same")
    second, duplicate = service.submit(request, "same")
    assert created and not duplicate and first.id == second.id
    with pytest.raises(DomainError, match="different request"):
        service.submit(request.model_copy(update={"name": "different"}), "same")


def test_backpressure_preserves_duplicate(service):
    service.settings.queue_limit = 1
    request = JobSubmit(name="compute", image="strata/python-workloads:local", command=["run"])
    job, _ = service.submit(request, "key")
    assert service.submit(request, "key")[0].id == job.id
    with pytest.raises(DomainError) as exc:
        submit(service)
    assert exc.value.code == 429
    service.cancel(job.id)
    assert submit(service)


def test_image_and_key_validation(service):
    with pytest.raises(DomainError):
        submit(service, image="untrusted/image")
    with pytest.raises(DomainError):
        service.submit(
            JobSubmit(name="x", image="strata/python-workloads:local", command=["run"]), ""
        )


def test_priority_capability_and_capacity(service):
    worker = register(service, cpu=2, memory=512, capabilities=["python"])
    too_big = submit(service, resources={"cpu": 3, "memory_mb": 128}, priority=100)
    cpp = submit(service, capabilities=["cpp"], priority=99)
    low = submit(service, priority=1)
    high = submit(service, resources={"cpu": 2, "memory_mb": 512}, priority=10)
    assert Scheduler(service).schedule() == 1
    assert service.assignments(worker.id, worker.session_id)[0]["job_id"] == high.id
    assert all(service.get_job(j.id).status == "QUEUED" for j in [low, too_big, cpp])


@pytest.mark.parametrize("policy,expected", [("least_loaded", "big"), ("best_fit", "small")])
def test_placement_policies(service, policy, expected):
    service.settings.scheduling_policy = policy
    register(service, "big", cpu=8, memory=4096)
    small = register(service, "small", cpu=2, memory=1024)
    # Occupy the smaller worker so normalized headroom differs.
    with service.factory.begin() as session:
        w = session.get(Worker, small.id)
        w.cpu_reserved = 0.5
    job = submit(service)
    Scheduler(service).schedule()
    with service.factory() as session:
        assert session.scalar(select(Attempt).where(Attempt.job_id == job.id)).worker_id == expected


def test_fifo_ignores_priority(service):
    service.settings.scheduling_policy = "fifo"
    register(service, cpu=1)
    first = submit(service)
    service.clock.advance(1)
    submit(service, priority=99)
    Scheduler(service).schedule()
    assert service.get_job(first.id).status == "SCHEDULED"


def test_success_releases_resources_and_report_is_idempotent(service):
    job, worker, assignment = assigned(service)
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    complete(service, worker, assignment)
    complete(service, worker, assignment)
    assert service.get_job(job.id).status == "SUCCEEDED"
    with service.factory() as session:
        assert session.get(Worker, worker.id).cpu_reserved == 0
        assert [
            e.kind for e in session.scalars(select(JobEvent).where(JobEvent.job_id == job.id))
        ].count("JOB_SUCCEEDED") == 1
    with pytest.raises(DomainError):
        complete(service, worker, assignment, "FAILED", 1)


def test_retry_backoff_budget_and_manual_retry(service):
    job, worker, assignment = assigned(service, job=submit(service, max_retries=1))
    complete(service, worker, assignment, "FAILED", 1)
    assert service.get_job(job.id).status == "RETRYING"
    assert Scheduler(service).schedule() == 0
    service.clock.advance(2)
    Scheduler(service).schedule()
    next_attempt = service.assignments(worker.id, worker.session_id)[0]
    service.start(next_attempt["attempt_id"], worker.session_id, next_attempt["lease_token"])
    complete(service, worker, next_attempt, "FAILED", 1)
    assert service.get_job(job.id).status == "FAILED"
    assert service.retry(job.id).status == "QUEUED"
    Scheduler(service).schedule()
    assert service.get_job(job.id).attempts_count == 3


@pytest.mark.parametrize("start", [True, False])
def test_cancellation_wins_before_completion(service, start):
    job, worker, assignment = assigned(service, start=start)
    service.cancel(job.id)
    if start:
        assert heartbeat(service, worker, assignment)["commands"][0]["cancel"]
        complete(service, worker, assignment)
    assert service.get_job(job.id).status == "CANCELLED"
    assert service.cancel(job.id).status == "CANCELLED"


def test_queued_cancel_and_finished_success_wins(service):
    queued = submit(service)
    assert service.cancel(queued.id).status == "CANCELLED"
    job, worker, assignment = assigned(service)
    complete(service, worker, assignment)
    assert service.cancel(job.id).status == "SUCCEEDED"
    with pytest.raises(DomainError):
        service.retry(job.id)


def test_lease_renewal_cannot_resurrect_expired_attempt(service):
    job, worker, assignment = assigned(service)
    service.clock.advance(10)
    assert heartbeat(service, worker, assignment)["commands"][0]["valid"]
    # Keep worker fresh while omitting this attempt.
    for _ in range(3):
        service.clock.advance(10)
        heartbeat(service, worker)
    assert not heartbeat(service, worker, assignment)["commands"][0]["valid"]
    assert Scheduler(service).recover() == 1
    assert service.get_job(job.id).status == "RETRYING"
    with pytest.raises(DomainError):
        complete(service, worker, assignment)


def test_heartbeat_limits_and_live_worker_collision(service):
    worker = register(service)
    with pytest.raises(DomainError, match="live session"):
        register(service)
    with pytest.raises(DomainError):
        service.heartbeat(
            worker.id, Heartbeat(session_id="bad", cpu_available=1, memory_available_mb=1)
        )
    with pytest.raises(DomainError):
        service.heartbeat(
            worker.id,
            Heartbeat(session_id=worker.session_id, cpu_available=99, memory_available_mb=1),
        )


def test_logs_artifacts_hash_path_and_idempotent_upload(service):
    job, worker, a = assigned(service)
    service.logs(a["attempt_id"], worker.session_id, a["lease_token"], "hello")
    artifact = service.artifact(
        a["attempt_id"], worker.session_id, a["lease_token"], "result.csv", "text/csv", b"1,2"
    )
    assert artifact.sha256 == hashlib.sha256(b"1,2").hexdigest()
    assert (service.settings.artifact_root / artifact.sha256).read_bytes() == b"1,2"
    assert (
        service.artifact(
            a["attempt_id"], worker.session_id, a["lease_token"], "result.csv", "text/csv", b"1,2"
        ).id
        == artifact.id
    )
    with pytest.raises(DomainError):
        service.artifact(
            a["attempt_id"],
            worker.session_id,
            a["lease_token"],
            "result.csv",
            "text/csv",
            b"different",
        )
    with pytest.raises(DomainError):
        service.artifact(
            a["attempt_id"], worker.session_id, a["lease_token"], "../escape", "text/csv", b"x"
        )
    service.settings.logs_max_bytes = service.settings.artifact_max_bytes = 1
    with pytest.raises(DomainError):
        service.logs(a["attempt_id"], worker.session_id, a["lease_token"], "too big")
    with pytest.raises(DomainError):
        service.artifact(
            a["attempt_id"], worker.session_id, a["lease_token"], "large", "text/csv", b"xx"
        )
    with service.factory() as session:
        assert session.get(Attempt, a["attempt_id"]).logs == "hello"
        assert len(list(session.scalars(select(Artifact)))) == 1


def test_unknown_ids_and_invalid_reports(service):
    with pytest.raises(DomainError):
        service.get_job("missing")
    with pytest.raises(DomainError):
        service.assignments("missing", "missing")
    with pytest.raises(DomainError):
        service.start("missing", "missing", "missing")
    job, worker, a = assigned(service, start=False)
    with pytest.raises(DomainError, match="not started"):
        complete(service, worker, a)
    service.start(a["attempt_id"], worker.session_id, a["lease_token"])
    with pytest.raises(DomainError, match="not requested"):
        complete(service, worker, a, "CANCELLED", 137)


def test_heartbeat_rejects_lease_from_other_worker(service):
    _, first, assignment = assigned(service)
    other = register(service, "worker-2")
    # Even a known token cannot renew a lease through a different worker session.
    assert not heartbeat(service, other, assignment)["commands"][0]["valid"]
