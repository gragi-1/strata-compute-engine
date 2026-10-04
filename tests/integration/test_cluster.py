import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from control_plane.api import create_app
from control_plane.campaigns import CampaignService
from control_plane.cluster import ClusterService
from control_plane.errors import AdmissionPaused
from control_plane.models import Attempt, AuditEvent, JobSchedule, ScheduleFire, WorkflowExpansion
from control_plane.periodic import PeriodicService
from control_plane.schemas import AdmissionUpdate, CampaignSubmit, JobSubmit, WorkflowSubmit
from control_plane.workflows import WorkflowService
from scheduler.core import Scheduler
from tests.helpers import complete, register, submit
from tests.integration.test_identity import headers, prepare
from tests.integration.test_periodic import template
from tests.integration.test_platform import spec
from tests.integration.test_workflows import generated


def update(service, accepting=True, scheduling=True):
    return ClusterService(service).update(
        AdmissionUpdate(
            accepting_jobs=accepting,
            scheduling_enabled=scheduling,
            reason="Maintenance drill",
        )
    )


def test_pause_preserves_running_work_replays_and_independent_queue_drain(service):
    client = TestClient(create_app(service.settings, service))
    existing = client.post(
        "/jobs", json=spec(priority=100), headers={"Idempotency-Key": "first"}
    ).json()
    waiting = submit(service)
    worker = register(service, cpu=1)
    scheduler = Scheduler(service)
    assert scheduler.schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    update(service, False, False)
    denied = client.post("/jobs", json=spec())
    assert denied.status_code == 503 and denied.headers["Retry-After"] == "30"
    replay = client.post("/jobs", json=spec(priority=100), headers={"Idempotency-Key": "first"})
    assert replay.status_code == 200 and replay.json()["id"] == existing["id"]
    assert scheduler.schedule() == 0
    # Cancellation and completion remain available, regardless of the maintenance state.
    complete(service, worker, assignment)
    assert scheduler.schedule() == 0
    update(service, False, True)
    assert scheduler.schedule() == 1
    assert service.get_job(waiting.id).status == "SCHEDULED"
    assert client.post(f"/jobs/{waiting.id}/cancel").status_code == 200
    update(service)
    assert client.post("/jobs", json=spec()).status_code == 201
    assert "strata_cluster_accepting_jobs 1.0" in client.get("/metrics").text


def test_pause_rejects_manual_retry_but_recovery_still_fences_expired_leases(service):
    job, worker = submit(service), register(service)
    scheduler = Scheduler(service)
    assert scheduler.schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    update(service, False, False)
    service.clock.advance(service.settings.worker_timeout + service.settings.lease_seconds + 1)
    assert scheduler.recover() == 1
    with pytest.raises(AdmissionPaused):
        service.retry(job.id)
    assert scheduler.schedule() == 0


def test_batch_replays_work_and_new_workflows_campaigns_are_rejected(service):
    campaigns = CampaignService(service)
    body = CampaignSubmit(name="Grid", template=JobSubmit(**spec()), matrix={"seed": [1, 2]})
    workflow = WorkflowSubmit(name="Pipeline", nodes={"run": JobSubmit(**spec())})
    first, _ = campaigns.create(body, "grid")
    graph, _ = campaigns.workflow(workflow, "graph")
    update(service, False)
    assert campaigns.create(body, "grid")[0].id == first.id
    assert campaigns.workflow(workflow, "graph")[0].id == graph.id
    with pytest.raises(AdmissionPaused):
        campaigns.create(body, "new-grid")
    with pytest.raises(AdmissionPaused):
        campaigns.workflow(workflow, "new-graph")


def test_pause_defers_periodic_occurrences_and_dynamic_expansion_without_disabling(service):
    schedule = PeriodicService(service).create(template(catch_up=True))
    generated(service, [{"seed": 1}])
    service.clock.advance(3601)
    update(service, False, False)
    assert PeriodicService(service).tick() == 0
    assert WorkflowService(service).tick() == 0
    with service.factory() as session:
        row = session.get(JobSchedule, schedule.id)
        assert row.enabled and row.next_run_at == schedule.next_run_at
        assert session.scalar(select(func.count()).select_from(ScheduleFire)) == 0
        assert session.scalar(select(WorkflowExpansion)).status == "WAITING"
    update(service)
    service.clock.advance(31)
    assert PeriodicService(service).tick() == 1
    assert WorkflowService(service).tick() == 1


def test_control_requires_platform_admin_and_is_idempotently_audited(service):
    client, root, token, alpha, _, _, tokens = prepare(service)
    body = {"accepting_jobs": False, "scheduling_enabled": False, "reason": "Upgrade"}
    for credential in (tokens["alice"], tokens["bob"]):
        auth = headers(credential, alpha)
        assert client.get("/cluster/admission", headers=auth).status_code == 403
        assert client.patch("/cluster/admission", headers=auth, json=body).status_code == 403
    admin = headers(token)
    assert client.get("/execution/config", headers=headers(tokens["alice"])).status_code == 400
    catalog = client.get("/execution/config", headers=headers(tokens["alice"], alpha))
    assert catalog.status_code == 200 and catalog.json() == {
        "approved_images": service.settings.allowed_images
    }
    assert "local-development-token" not in catalog.text
    assert client.get("/cluster/admission", headers=admin).json()["accepting_jobs"]
    assert (
        client.patch(
            "/cluster/admission", headers=admin, json={**body, "reason": "bad\nreason"}
        ).status_code
        == 422
    )
    for _ in range(2):
        result = client.patch("/cluster/admission", headers=admin, json=body)
        assert result.status_code == 200 and result.json()["changed_by"] == root.id
    scoped = client.post(
        "/auth/tokens",
        headers=admin,
        json={
            "name": "Scoped automation",
            "project_id": alpha,
            "role": "admin",
        },
    )
    assert scoped.status_code == 201, scoped.text
    assert (
        client.patch(
            "/cluster/admission", headers=headers(scoped.json()["access_token"]), json=body
        ).status_code
        == 403
    )
    with service.factory() as session:
        events = list(
            session.scalars(
                select(AuditEvent).where(AuditEvent.action == "CLUSTER_ADMISSION_UPDATED")
            )
        )
        assert len(events) == 1 and events[0].actor_id == root.id


@pytest.mark.postgres
def test_pause_waits_for_concurrent_schedulers_then_prevents_future_assignments(
    postgres_service, monkeypatch
):
    svc = postgres_service
    register(svc, cpu=20)
    for _ in range(20):
        submit(svc)
    # Both schedulers must hold the shared fence concurrently. An exclusive fence would
    # deadlock this barrier, revealing accidental serialization of scheduler replicas.
    entered, release, local = threading.Barrier(3), threading.Event(), threading.local()
    original = svc.now

    def clock(session):
        if getattr(local, "scheduler", False):
            local.scheduler = False
            entered.wait(10)
            assert release.wait(10)
        return original(session)

    monkeypatch.setattr(svc, "now", clock)

    def schedule():
        local.scheduler = True
        return Scheduler(svc).schedule()

    with ThreadPoolExecutor(3) as pool:
        schedulers = [pool.submit(schedule) for _ in range(2)]
        entered.wait(10)
        pause = pool.submit(update, svc, False, False)
        try:
            with pytest.raises(TimeoutError):
                pause.result(timeout=0.25)
        finally:
            release.set()
        assigned = sum(future.result(timeout=10) for future in schedulers)
        assert pause.result(timeout=10)["scheduling_enabled"] is False
    assert assigned > 0
    for _ in range(5):
        assert Scheduler(svc).schedule() == 0
    with svc.factory() as session:
        assert session.scalar(select(func.count()).select_from(Attempt)) == assigned
    with pytest.raises(AdmissionPaused):
        submit(svc)
