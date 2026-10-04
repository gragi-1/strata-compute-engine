from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.schemas import Completion, WorkerRegister
from scheduler.core import Scheduler
from tests.helpers import submit


def test_log_revisions_follow_active_attempts_and_terminal_status_without_duplicate_payload(
    service,
):
    client = TestClient(create_app(service.settings, service))
    worker = service.register(
        WorkerRegister(worker_id="log-worker", cpu_total=2, memory_total_mb=512)
    )
    job = submit(service)
    initial = client.get(f"/jobs/{job.id}/log-snapshot").json()
    assert initial["attempt"] is None and initial["changed"] and initial["text"] == ""
    Scheduler(service).schedule()
    assignment = service.assignments(worker.id, worker.session_id)[0]
    service.start(assignment["attempt_id"], worker.session_id, assignment["lease_token"])
    service.logs(assignment["attempt_id"], worker.session_id, assignment["lease_token"], "step 1\n")
    snapshot = client.get(
        f"/jobs/{job.id}/log-snapshot", params={"cursor": initial["revision"]}
    ).json()
    assert snapshot["text"] == "step 1\n" and snapshot["status"] == "RUNNING"
    assert snapshot["attempt"] == 1
    assert snapshot["started_at"] is not None and snapshot["finished_at"] is None
    same = client.get(
        f"/jobs/{job.id}/log-snapshot", params={"cursor": snapshot["revision"]}
    ).json()
    assert not same["changed"] and same["text"] is None
    service.complete(
        assignment["attempt_id"],
        Completion(
            session_id=worker.session_id,
            lease_token=assignment["lease_token"],
            outcome="SUCCEEDED",
            exit_code=0,
        ),
    )
    final = client.get(
        f"/jobs/{job.id}/log-snapshot", params={"cursor": snapshot["revision"]}
    ).json()
    assert final["changed"] and final["status"] == "SUCCEEDED" and final["text"] == "step 1\n"
    assert final["finished_at"] is not None and final["retry_count"] == 0
    assert client.get("/jobs/missing/log-snapshot").status_code == 404
