from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from control_plane.api import create_app
from control_plane.experiments import ExperimentService, RunSubmit
from control_plane.models import ExperimentRun, Job
from control_plane.rpc import engine_pb2 as pb
from control_plane.schemas import NamedResource
from control_plane.services import DomainError
from tests.helpers import assigned, complete, register
from tests.integration.test_identity import headers, prepare
from tests.integration.test_platform import spec

IMAGE = "sha256:" + "1" * 64


def test_experiment_metrics_search_comparison_and_immutable_specification(service):
    client = TestClient(create_app(service.settings, service))
    experiment = client.post("/experiments", json={"name": "Numerical study"}).json()
    path = f"/experiments/{experiment['id']}/runs"
    body = {"job": spec(), "source_revision": "a" * 40, "metadata": {"seed": 42}}
    response = client.post(path, json=body, headers={"Idempotency-Key": "first"})
    assert response.status_code == 201
    run = response.json()
    assert (
        client.post(path, json=body, headers={"Idempotency-Key": "first"}).json()["id"] == run["id"]
    )
    assert (
        client.post(path, json={"job": spec()}, headers={"Idempotency-Key": "first"}).status_code
        == 409
    )
    metric_path = f"/experiment-runs/{run['id']}/metrics"
    assert client.put(metric_path, json={"values": {"error": 0.1}}).json() == {"error": 0.1}
    assert client.put(metric_path, json={"values": {"error": 0.2}}).status_code == 409
    assert client.put(metric_path, json={"values": {"escape/key": 2}}).status_code == 422
    assert (
        client.get(path, params={"metric": "error", "maximum": 0.15}).json()[0]["id"] == run["id"]
    )
    assert client.get(path, params={"metric": "error", "minimum": 0.15}).json() == []
    assert client.get(path, params={"maximum": 1}).status_code == 422
    assert (
        client.get("/experiment-runs/compare", params={"ids": run["id"]}).json()[0][
            "source_revision"
        ]
        == "a" * 40
    )
    assert (
        client.get(
            "/experiment-runs/compare", params={"ids": run["id"] + "," + run["id"]}
        ).status_code
        == 422
    )


def test_resolved_provenance_replay_image_guard_and_failed_attempt_checkpoint(service):
    client = TestClient(create_app(service.settings, service))
    experiment = client.post("/experiments", json={"name": "Checkpoint study"}).json()
    run = client.post(f"/experiments/{experiment['id']}/runs", json={"job": spec()}).json()
    worker = register(service, capabilities=["python", "cpp", "dataset-inputs", "image-pinning"])
    job, worker, assignment = assigned(
        service, job=service.get_job(run["job_id"]), worker=worker, start=False
    )
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"], IMAGE)
    artifact = service.artifact(
        assignment["attempt_id"],
        worker.session_id,
        assignment["lease_token"],
        "checkpoint.json",
        "application/json",
        b'{"step":42}',
    )
    complete(service, worker, assignment, outcome="FAILED", code=1)
    service.cancel(job.id)  # Stop retry waiting, retaining the failed attempt checkpoint.
    info = client.get(f"/experiment-runs/{run['id']}").json()
    assert info["attempts"][0]["provenance"]["image_digest"] == IMAGE
    assert info["artifacts"][0]["sha256"] == artifact.sha256
    checkpoint = {
        "job_id": job.id,
        "name": artifact.name,
        "alias": "checkpoint",
        "artifact_id": artifact.id,
    }
    body = {
        "checkpoints": [checkpoint],
        "command": ["run", "--resume", "/inputs/checkpoint/checkpoint.json"],
    }
    response = client.post(
        f"/experiment-runs/{run['id']}/replay", json=body, headers={"Idempotency-Key": "replay"}
    )
    assert response.status_code == 201, response.text
    replay = response.json()
    assert replay["specification"]["expected_image_digest"] == IMAGE
    assert replay["replay_of"] == run["id"] and replay["specification"]["depends_on"] == []
    assert (
        client.post(
            f"/experiment-runs/{run['id']}/replay", json=body, headers={"Idempotency-Key": "replay"}
        ).json()["id"]
        == replay["id"]
    )
    _, _, second = assigned(
        service, job=service.get_job(replay["job_id"]), worker=worker, start=False
    )
    with pytest.raises(DomainError, match="pinned"):
        service.start(
            second["attempt_id"], worker.session_id, second["lease_token"], "sha256:" + "2" * 64
        )
    from control_plane.inputs import manifest

    credentials = pb.AttemptRequest(
        attempt_id=second["attempt_id"],
        session_id=worker.session_id,
        lease_token=second["lease_token"],
    )
    assert manifest(service, credentials)[0]["sha256"] == artifact.sha256
    service.start(second["attempt_id"], worker.session_id, second["lease_token"], IMAGE)
    assert (
        client.post(
            f"/experiment-runs/{run['id']}/replay", json={"checkpoints": [checkpoint, checkpoint]}
        ).status_code
        == 422
    )


def test_unknown_image_and_cross_project_runs_fail_closed(service):
    client, _, admin, alpha, beta, _, tokens = prepare(service)
    alice = headers(tokens["alice"], alpha)
    bob = headers(tokens["bob"], beta)
    experiment = client.post("/experiments", headers=alice, json={"name": "Private study"}).json()
    run = client.post(
        f"/experiments/{experiment['id']}/runs", headers=alice, json={"job": spec()}
    ).json()
    assert client.get(f"/experiment-runs/{run['id']}", headers=bob).status_code == 404
    assert (
        client.get("/experiment-runs/compare", params={"ids": run["id"]}, headers=bob).status_code
        == 404
    )
    assert (
        client.put(
            f"/experiment-runs/{run['id']}/metrics", headers=bob, json={"values": {"x": 1}}
        ).status_code
        == 403
    )
    assert client.get(f"/experiments/{experiment['id']}/runs", headers=bob).status_code == 404
    _, worker, assignment = assigned(service, job=service.get_job(run["job_id"]))
    complete(service, worker, assignment)
    assert (
        client.post(f"/experiment-runs/{run['id']}/replay", headers=alice, json={}).status_code
        == 409
    )


@pytest.mark.postgres
def test_postgres_duplicate_runs_create_one_job_and_search_metrics(postgres_service):
    service = ExperimentService(postgres_service)
    experiment = service.create(NamedResource(name="Concurrent experiment"))
    body = RunSubmit(job=spec())
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(lambda _: service.run(experiment.id, body, "same")[0].id, range(24)))
    assert len(set(rows)) == 1
    with postgres_service.factory() as session:
        assert session.scalar(select(func.count()).select_from(ExperimentRun)) == 1
        assert session.scalar(select(func.count()).select_from(Job)) == 1
    client = TestClient(create_app(postgres_service.settings, postgres_service))
    client.put(f"/experiment-runs/{rows[0]}/metrics", json={"values": {"error": 0.1}})
    assert (
        len(
            client.get(
                f"/experiments/{experiment.id}/runs", params={"metric": "error", "maximum": 1}
            ).json()
        )
        == 1
    )
