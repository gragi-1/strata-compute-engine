from fastapi.testclient import TestClient

from control_plane.api import create_app
from tests.helpers import assigned, complete


def test_public_api_lifecycle_and_validation(service):
    with TestClient(create_app(service.settings, service)) as client:
        payload = {
            "name": "pi",
            "image": "strata/python-workloads:local",
            "command": ["python", "main.py"],
        }
        first = client.post("/jobs", json=payload, headers={"Idempotency-Key": "same"})
        assert first.status_code == 201
        job_id = first.json()["id"]
        duplicate = client.post("/jobs", json=payload, headers={"Idempotency-Key": "same"})
        assert duplicate.status_code == 200 and duplicate.json()["id"] == job_id
        assert client.get(f"/jobs/{job_id}").json()["status"] == "QUEUED"
        assert len(client.get("/jobs?status=QUEUED").json()) == 1
        assert client.get("/jobs?limit=0").status_code == 422
        assert client.get("/jobs/missing").status_code == 404
        assert client.post("/jobs", json={**payload, "extra": 1}).status_code == 422
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/ready").status_code == 200
        assert client.get(f"/jobs/{job_id}/logs").text == ""
        assert [e["kind"] for e in client.get(f"/jobs/{job_id}/events").json()] == [
            "JOB_CREATED",
            "JOB_QUEUED",
        ]
        assert client.post(f"/jobs/{job_id}/cancel").json()["status"] == "CANCELLED"
        assert client.post(f"/jobs/{job_id}/retry").json()["status"] == "QUEUED"


def test_api_artifact_attempt_and_metrics_privacy(service):
    job, worker, a = assigned(service)
    service.logs(a["attempt_id"], worker.session_id, a["lease_token"], "computed result")
    artifact = service.artifact(
        a["attempt_id"], worker.session_id, a["lease_token"], "result.csv", "text/csv", b"1,2"
    )
    service.clock.advance(1)
    complete(service, worker, a)
    with TestClient(create_app(service.settings, service)) as client:
        assert client.get(f"/jobs/{job.id}/logs?attempt=1").text == "computed result"
        attempts = client.get(f"/jobs/{job.id}/attempts").json()
        assert len(attempts) == 1 and "lease_token" not in attempts[0]
        assert "session_id" not in client.get(f"/workers/{worker.id}").json()
        assert len(client.get("/workers").json()) == 1
        assert client.get("/workers/missing").status_code == 404
        rows = client.get(f"/jobs/{job.id}/artifacts").json()
        assert rows[0]["uri"] == f"/artifacts/{artifact.id}"
        download = client.get(rows[0]["uri"])
        assert download.content == b"1,2" and artifact.sha256 in download.headers["etag"]
        assert client.get("/artifacts/missing").status_code == 404
        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert "jobs_submitted_total 1.0" in metrics.text
        assert "job_duration_seconds_count 1.0" in metrics.text
        assert "scheduler_latency_seconds" in metrics.text
        (service.settings.artifact_root / artifact.sha256).unlink()
        assert client.get(rows[0]["uri"]).status_code == 503


def test_scheduler_endpoint_auth_and_saturation(service):
    service.settings.queue_limit = 1
    with TestClient(create_app(service.settings, service)) as client:
        assert client.post("/internal/scheduler/tick").status_code == 401
        assert (
            client.post(
                "/internal/scheduler/tick",
                headers={"Authorization": f"Bearer {service.settings.worker_token}"},
            ).status_code
            == 200
        )
        payload = {"name": "pi", "image": "strata/python-workloads:local", "command": ["run"]}
        assert client.post("/jobs", json=payload).status_code == 201
        saturated = client.post("/jobs", json=payload)
        assert saturated.status_code == 429 and saturated.headers["retry-after"] == "2"
