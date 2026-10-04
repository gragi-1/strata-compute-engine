import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from control_plane.api import create_app
from control_plane.campaigns import request_digest
from control_plane.models import Job, JobSchedule, ScheduleFire, User
from control_plane.periodic import PeriodicService, ScheduleSubmit, next_occurrence
from control_plane.schemas import CampaignSubmit, JobSubmit, WorkflowSubmit
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import complete, register, submit
from tests.integration.test_identity import headers, prepare
from tests.integration.test_platform import spec


def template(**values):
    return ScheduleSubmit(
        name="Hourly experiment", cron="0 * * * *", job=JobSubmit(**spec()), **values
    )


def test_durable_restart_skip_and_bounded_catch_up(service):
    periodic = PeriodicService(service)
    skip = periodic.create(template())
    catch = periodic.create(template(catch_up=True))
    service.clock.advance(3 * 3600 + 1)
    assert PeriodicService(service).tick() == 2
    assert PeriodicService(service).tick() == 1
    assert PeriodicService(service).tick() == 1
    assert PeriodicService(service).tick() == 0
    with service.factory() as session:
        fires = list(session.scalars(select(ScheduleFire).order_by(ScheduleFire.scheduled_at)))
        assert [fire.scheduled_at.hour for fire in fires if fire.schedule_id == skip.id] == [3]
        assert [fire.scheduled_at.hour for fire in fires if fire.schedule_id == catch.id] == [
            1,
            2,
            3,
        ]
        assert session.get(JobSchedule, skip.id).next_run_at.hour == 4
        assert session.scalar(select(func.count()).select_from(Job)) == 4
        job = session.get(Job, fires[0].job_id)
        assert job.parameters["schedule_id"] == catch.id
    # Resume of an already enabled schedule does not discard pending occurrences.
    assert periodic.action(catch.id, True).next_run_at.hour == 4


def test_capacity_backoff_resume_limit_and_invalid_template_pause(service):
    service.settings.queue_limit = 1
    service.settings.schedule_max_active = 1
    periodic = PeriodicService(service)
    schedule = periodic.create(template(catch_up=True))
    occupied = submit(service)
    service.clock.advance(3600)
    assert periodic.tick() == 0
    with service.factory() as session:
        row = session.get(JobSchedule, schedule.id)
        assert row.enabled and row.next_run_at.hour == 1 and "capacity" in row.last_error
        assert row.next_check_at > service.clock()
    service.cancel(occupied.id)
    assert periodic.tick() == 0  # Durable backoff prevents a hot loop.
    service.clock.advance(30)
    assert periodic.tick() == 1
    periodic.action(schedule.id, False)
    periodic.create(template())
    with pytest.raises(DomainError, match="schedule capacity"):
        periodic.action(schedule.id, True)
    with service.factory.begin() as session:
        row = session.get(JobSchedule, schedule.id)
        row.enabled = True
        row.next_run_at = service.clock()
        row.specification = {"unknown": "invalid"}
    assert periodic.tick() == 0
    with service.factory() as session:
        row = session.get(JobSchedule, schedule.id)
        assert not row.enabled and row.last_error == "schedule template is invalid"


def test_schedule_api_project_boundaries_and_owner_revocation(service):
    client, _, admin, alpha, beta, users, tokens = prepare(service)
    alice, bob = headers(tokens["alice"], alpha), headers(tokens["bob"], beta)
    body = {"name": "Private recurring study", "cron": "* * * * *", "job": spec()}
    response = client.post("/schedules", json=body, headers=alice)
    assert response.status_code == 201, response.text
    schedule = response.json()
    assert client.get("/schedules", headers=bob).json() == []
    assert client.get(f"/schedules/{schedule['id']}/fires", headers=bob).status_code == 404
    assert client.post(f"/schedules/{schedule['id']}/pause", headers=bob).status_code == 403
    service.clock.advance(60)
    assert PeriodicService(service).tick() == 1
    with service.factory() as session:
        fire = session.scalar(select(ScheduleFire))
        assert session.get(Job, fire.job_id).project_id == alpha
    with service.factory.begin() as session:
        session.get(User, users["alice"]).enabled = False
    service.clock.advance(60)
    assert PeriodicService(service).tick() == 0
    with service.factory() as session:
        row = session.get(JobSchedule, schedule["id"])
        assert not row.enabled and "disabled" in row.last_error
        assert session.scalar(select(func.count()).select_from(ScheduleFire)) == 1


def test_cron_validation_timezone_and_dst(service):
    client = TestClient(create_app(service.settings, service))
    for cron, timezone in [
        ("invalid", "UTC"),
        ("* * * * * *", "UTC"),
        ("0 0 * * *", "Missing/Zone"),
    ]:
        assert (
            client.post(
                "/schedules",
                json={"name": "Invalid", "cron": cron, "timezone": timezone, "job": spec()},
            ).status_code
            == 422
        )
    assert next_occurrence(
        "0 9 * * *", "Europe/Madrid", datetime(2026, 3, 28, 9, tzinfo=UTC)
    ) == datetime(2026, 3, 29, 7, tzinfo=UTC)
    assert next_occurrence(
        "0 9 * * *", "Europe/Madrid", datetime(2026, 10, 24, 9, tzinfo=UTC)
    ) == datetime(2026, 10, 25, 8, tzinfo=UTC)
    assert (
        client.post(
            "/schedules",
            json={"name": "Invalid", "cron": "* * * * *", "job": spec(depends_on=["missing"])},
        ).status_code
        == 422
    )


@pytest.mark.postgres
def test_two_periodic_schedulers_create_one_occurrence(postgres_service):
    periodic = PeriodicService(postgres_service)
    schedule = periodic.create(template())
    postgres_service.clock.advance(3600)
    start = Barrier(2)

    def tick(_):
        start.wait(timeout=10)
        return PeriodicService(postgres_service).tick()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(tick, range(2))) == 1
    with postgres_service.factory() as session:
        assert session.scalar(select(func.count()).select_from(ScheduleFire)) == 1
        assert session.scalar(select(func.count()).select_from(Job)) == 1
        assert session.get(JobSchedule, schedule.id).next_run_at.hour == 2


def test_conditional_dependencies_cleanup_failure_handler_and_false_condition(service):
    parent = submit(service, max_retries=0)
    ordinary = submit(service, depends_on=[parent.id])
    cleanup = submit(service, depends_on=[parent.id], dependency_policy="all_terminal")
    handler = submit(service, depends_on=[parent.id], dependency_policy="any_failed")
    worker = register(service)
    scheduler = Scheduler(service)
    assert scheduler.schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    complete(service, worker, assignment, "FAILED", 1)
    assert scheduler.schedule() == 2
    assert service.get_job(ordinary.id).status == "FAILED"
    assert {value["job_id"] for value in service.assignments(worker.id, worker.session_id)} == {
        cleanup.id,
        handler.id,
    }
    success = submit(service)
    false_handler = submit(service, depends_on=[success.id], dependency_policy="any_failed")
    assert scheduler.schedule() == 1
    assignment = next(
        value
        for value in service.assignments(worker.id, worker.session_id)
        if value["job_id"] == success.id
    )
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    complete(service, worker, assignment)
    scheduler.schedule()
    assert service.get_job(false_handler.id).status == "CANCELLED"
    with pytest.raises(DomainError, match="at least one"):
        submit(service, dependency_policy="any_failed")


def test_v2_idempotency_hashes_survive_additive_fields(service):
    body = JobSubmit(**spec())
    # Frozen v2 serialization, independent of new defaults in the current models.
    old = {
        "name": "compute",
        "image": "strata/python-workloads:local",
        "command": ["true"],
        "resources": {"cpu": 1, "memory_mb": 256},
        "capabilities": [],
        "priority": 0,
        "max_retries": 0,
        "timeout_seconds": 600,
        "inputs": [],
        "depends_on": [],
        "artifact_inputs": [],
    }
    digest = hashlib.sha256(
        json.dumps(old, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    job, _ = service.submit(body, "v2-replay")
    assert job.request_hash == digest
    assert service.submit(body, "v2-replay")[1] is False
    for model in [
        CampaignSubmit(name="Study", template=body),
        WorkflowSubmit(name="Study", nodes={"run": body}),
    ]:
        legacy = {"name": "Study", "description": ""}
        if isinstance(model, CampaignSubmit):
            legacy.update(template=old, matrix={}, repeats=1)
        else:
            legacy.update(nodes={"run": old})
        assert (
            request_digest(model)
            == hashlib.sha256(json.dumps(legacy, separators=(",", ":")).encode()).hexdigest()
        )
