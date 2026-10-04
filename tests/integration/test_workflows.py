import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from sqlalchemy import func, select

from control_plane.campaigns import CampaignService
from control_plane.models import Job, JobDependency, WorkflowExpansion
from control_plane.rpc import engine_pb2 as pb
from control_plane.schemas import JobSubmit, WorkflowSubmit
from control_plane.services import DomainError
from control_plane.workflows import WorkflowService
from scheduler.core import Scheduler
from tests.helpers import complete, register, submit
from tests.integration.test_identity import headers, prepare
from tests.integration.test_platform import spec


def workflow():
    return WorkflowSubmit(
        name="Adaptive numerical study",
        nodes={
            "generate": JobSubmit(**spec()),
            "analyze": JobSubmit(
                **spec(
                    depends_on=["experiments"],
                    artifact_inputs=[
                        {"job_id": "experiments", "name": "result.json", "alias": "results"}
                    ],
                )
            ),
        },
        expansions={
            "experiments": {
                "source": "generate",
                "artifact": "parameters.json",
                "template": spec(command=["compute", "--seed", "${seed}"]),
                "max_jobs": 10,
            }
        },
    )


def generated(service, parameters):
    campaign, _ = CampaignService(service).workflow(workflow(), "adaptive")
    worker = register(service)
    scheduler = Scheduler(service)
    assert scheduler.schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    service.artifact(
        assignment["attempt_id"],
        worker.session_id,
        assignment["lease_token"],
        "parameters.json",
        "application/json",
        json.dumps(parameters).encode(),
    )
    complete(service, worker, assignment)
    return campaign, worker


def test_dynamic_children_join_resolved_artifacts_and_restart_idempotency(service):
    campaign, worker = generated(service, [{"seed": 41}, {"seed": 42}])
    assert WorkflowService(service).tick() == 1
    assert WorkflowService(service).tick() == 0
    assert Scheduler(service).schedule() == 2
    assignments = service.assignments(worker.id, worker.session_id)
    assert {tuple(value["command"]) for value in assignments} == {
        ("compute", "--seed", "41"),
        ("compute", "--seed", "42"),
    }
    for assignment in assignments:
        service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
        service.artifact(
            assignment["attempt_id"],
            worker.session_id,
            assignment["lease_token"],
            "result.json",
            "application/json",
            b'{"value":1}',
        )
        complete(service, worker, assignment)
    assert WorkflowService(service).complete_barriers() == 1
    assert Scheduler(service).schedule() == 1
    analysis = service.assignments(worker.id, worker.session_id)[0]
    from control_plane.inputs import manifest

    files = manifest(
        service,
        pb.AttemptRequest(
            attempt_id=analysis["attempt_id"],
            session_id=worker.session_id,
            lease_token=analysis["lease_token"],
        ),
    )
    assert {file["alias"] for file in files} == {"results-0", "results-1"}
    assert all(file["name"] == "result.json" for file in files)
    service.start(analysis["attempt_id"], worker.session_id, analysis["lease_token"])
    complete(service, worker, analysis)
    assert CampaignService(service).get(campaign.id)["status"] == "COMPLETED"
    with service.factory() as session:
        row = session.scalar(select(WorkflowExpansion))
        assert (
            row.status == "EXPANDED" and row.generated_count == 2 and len(row.manifest_sha256) == 64
        )
        assert session.scalar(select(func.count()).select_from(Job)) == 5
        gate = session.get(Job, row.gate_job_id)
        assert gate.attempts_count == 0 and gate.status == "SUCCEEDED"


def test_partial_rendering_failure_rolls_back_every_child(service):
    campaign, worker = generated(service, [{"seed": 1}, {"wrong": 2}])
    assert WorkflowService(service).tick() == 0
    with service.factory() as session:
        row = session.scalar(select(WorkflowExpansion))
        assert row.status == "FAILED" and "missing parameter" in row.last_error
        assert session.scalar(select(func.count()).select_from(Job)) == 3
        assert session.scalar(select(func.count()).select_from(JobDependency)) == 2
        assert session.get(Job, row.gate_job_id).status == "FAILED"
    Scheduler(service).schedule()
    assert CampaignService(service).get(campaign.id)["status"] == "COMPLETED"


def test_queue_backoff_then_atomic_expansion(service):
    service.settings.queue_limit = 4
    campaign, _ = generated(service, [{"seed": 1}, {"seed": 2}])
    occupied = submit(service)
    assert WorkflowService(service).tick() == 0
    with service.factory() as session:
        assert session.scalar(select(WorkflowExpansion)).status == "WAITING"
        assert session.scalar(select(func.count()).select_from(Job)) == 4
    service.cancel(occupied.id)
    assert WorkflowService(service).tick() == 0
    service.clock.advance(30)
    assert WorkflowService(service).tick() == 1


def test_failed_children_gate_and_campaign_retry_recover(service):
    campaign, worker = generated(service, [{"seed": 1}])
    WorkflowService(service).tick()
    Scheduler(service).schedule()
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    complete(service, worker, assignment, "FAILED", 1)
    WorkflowService(service).complete_barriers()
    Scheduler(service).schedule()
    assert CampaignService(service).retry(campaign.id) == 3
    assert Scheduler(service).schedule() == 1
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    service.artifact(
        assignment["attempt_id"],
        worker.session_id,
        assignment["lease_token"],
        "result.json",
        "application/json",
        b"{}",
    )
    complete(service, worker, assignment)
    assert WorkflowService(service).tick() == 0
    assert Scheduler(service).schedule() == 1


def test_empty_expansion_and_bounded_or_invalid_manifest(service):
    generated(service, [])
    assert WorkflowService(service).tick() == 1
    assert Scheduler(service).schedule() == 1
    service.settings.campaign_max_jobs = 2
    with pytest.raises(DomainError, match="total job limit"):
        CampaignService(service).workflow(workflow(), "too-large")


def test_private_expansion_api_and_cross_project_inputs(service):
    client, _, token, alpha, beta, _, tokens = prepare(service)
    alice, bob = headers(tokens["alice"], alpha), headers(tokens["bob"], beta)
    response = client.post("/workflows", json=workflow().model_dump(), headers=alice)
    assert response.status_code == 201, response.text
    campaign = response.json()
    assert len(client.get(f"/campaigns/{campaign['id']}/expansions", headers=alice).json()) == 1
    assert client.get(f"/campaigns/{campaign['id']}/expansions", headers=bob).status_code == 404
    invalid = workflow().model_dump()
    invalid["nodes"]["generate"]["depends_on"] = ["experiments"]
    assert client.post("/workflows", json=invalid, headers=alice).status_code == 422


@pytest.mark.postgres
def test_concurrent_dynamic_expansion_creates_one_set_of_children(postgres_service):
    campaign, _ = generated(postgres_service, [{"seed": 1}, {"seed": 2}])
    barrier = Barrier(2)

    def tick(_):
        barrier.wait(timeout=10)
        return WorkflowService(postgres_service).tick()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(tick, range(2))) == 1
    with postgres_service.factory() as session:
        assert session.scalar(select(func.count()).select_from(Job)) == 5
        assert session.scalar(select(WorkflowExpansion)).generated_count == 2
