import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import select

from control_plane.models import Attempt, GPUDevice
from control_plane.schemas import Completion, GPURegistration, WorkerRegister
from control_plane.services import DomainError
from scheduler.core import Scheduler
from tests.helpers import submit

GPU_ID = "GPU-12345678-1234-1234-1234-123456789abc"


def register_gpu(svc, worker="gpu-worker", memory=4096):
    return svc.register(
        WorkerRegister(
            worker_id=worker,
            cpu_total=16,
            memory_total_mb=8192,
            gpus=[GPURegistration(id=GPU_ID, name="Synthetic allocation device", memory_mb=memory)],
        )
    )


def test_gpu_reservation_is_exclusive_releases_and_filters_oversized_jobs(service):
    worker = register_gpu(service)
    oversized = submit(service, name="Oversized GPU", resources={"gpus": 1, "gpu_memory_mb": 8192})
    first = submit(service, name="First GPU", resources={"gpus": 1}, priority=10)
    second = submit(service, name="Second GPU", resources={"gpus": 1})
    cpu = submit(service, name="CPU alongside GPU")
    assert Scheduler(service).schedule() == 2
    assert service.get_job(oversized.id).status == "QUEUED"
    assignments = service.assignments(worker.id, worker.session_id)
    gpu = next(item for item in assignments if item["job_id"] == first.id)
    assert gpu["gpu_ids"] == [GPU_ID]
    assert next(item for item in assignments if item["job_id"] == cpu.id)["gpu_ids"] == []
    assert service.get_job(second.id).status == "QUEUED"
    service.start(gpu["attempt_id"], worker.session_id, gpu["lease_token"])
    with service.factory() as session:
        assert session.get(Attempt, gpu["attempt_id"]).provenance["gpus"] == [GPU_ID]
    service.complete(
        gpu["attempt_id"],
        Completion(
            session_id=worker.session_id,
            lease_token=gpu["lease_token"],
            outcome="SUCCEEDED",
            exit_code=0,
        ),
    )
    assert Scheduler(service).schedule() == 1
    with service.factory() as session:
        device = session.get(GPUDevice, GPU_ID)
        assert device.allocated_to != gpu["attempt_id"]


def test_live_gpu_cannot_be_registered_twice_and_recovery_allows_safe_transfer(service):
    worker = register_gpu(service)
    with pytest.raises(DomainError, match="another live worker"):
        register_gpu(service, "duplicate-worker")
    job = submit(service, resources={"gpus": 1})
    Scheduler(service).schedule()
    service.clock.advance(service.settings.worker_timeout + 1)
    with pytest.raises(DomainError, match="another live worker"):
        register_gpu(service, "new-owner")
    assert Scheduler(service).recover() == 1
    replacement = register_gpu(service, "new-owner")
    with service.factory() as session:
        device = session.get(GPUDevice, GPU_ID)
        assert device.worker_id == replacement.id and device.allocated_to is None
    assert service.get_job(job.id).status == "RETRYING"
    assert worker.id != replacement.id


@pytest.mark.postgres
def test_two_schedulers_cannot_allocate_one_gpu_twice(postgres_service):
    svc = postgres_service
    register_gpu(svc)
    for _ in range(10):
        submit(svc, resources={"gpus": 1})
    barrier = threading.Barrier(2)

    def schedule(_):
        barrier.wait(timeout=10)
        return Scheduler(svc).schedule()

    with ThreadPoolExecutor(2) as pool:
        assert sum(pool.map(schedule, range(2))) == 1
    with svc.factory() as session:
        attempts = list(session.scalars(select(Attempt)))
        assert len(attempts) == 1 and attempts[0].gpu_ids == [GPU_ID]
        assert session.get(GPUDevice, GPU_ID).allocated_to == attempts[0].id


def test_project_gpu_budget_and_private_log_access(service):
    from tests.integration.test_identity import headers, prepare

    client, root, token, alpha, beta, users, tokens = prepare(service)
    register_gpu(service)
    admin, operator = headers(token), headers(tokens["alice"], alpha)
    assert (
        client.patch(f"/projects/{alpha}", headers=admin, json={"gpu_limit": 0}).status_code == 200
    )
    specification = {
        "name": "Project GPU work",
        "image": "strata/python-workloads:local",
        "command": ["python", "main.py"],
        "resources": {"gpus": 1},
    }
    response = client.post("/jobs", headers=operator, json=specification)
    assert response.status_code == 201, response.text
    assert Scheduler(service).schedule() == 0
    assert (
        client.patch(f"/projects/{alpha}", headers=admin, json={"gpu_limit": 1}).status_code == 200
    )
    assert Scheduler(service).schedule() == 1
    job = response.json()
    assert (
        client.get(
            f"/jobs/{job['id']}/log-snapshot", headers=headers(tokens["bob"], beta)
        ).status_code
        == 404
    )
    assert client.get("/operations", headers=headers(tokens["alice"])).status_code == 403
    assert client.get("/operations", headers=admin).status_code == 200
    assert client.get("/workers/gpu-worker/gpus", headers=admin).json()[0]["allocated_to"]
