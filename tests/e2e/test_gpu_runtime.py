"""Actual discovered NVIDIA device reservation and CUDA execution on both workers."""

import json
import os
import platform
import socket
import threading
import time
from uuid import uuid4

import docker
import pytest
from sqlalchemy import select

from control_plane.models import Artifact, Attempt, GPUDevice, Job
from control_plane.rpc.server import make_server
from control_plane.schemas import JobSubmit
from scheduler.core import Scheduler
from worker.agent import Agent
from worker.executor import DockerExecutor
from worker.transport import Transport

pytestmark = [pytest.mark.docker, pytest.mark.gpu]


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_discovered_gpu_executes_cuda_and_releases_exclusive_device(service, tmp_path, kind):
    image = os.getenv("STRATA_TEST_GPU_IMAGE")
    if not image:
        pytest.skip(
            "set STRATA_TEST_GPU_IMAGE to the built workloads/gpu image on actual GPU hardware"
        )
    client = docker.from_env(timeout=3)
    service.settings.allowed_images = [image]
    service.settings.gpu_discovery_image = image
    service.settings.worker_cache_root = tmp_path / "cache"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    worker_id = f"gpu-{kind}-{uuid4().hex}"
    agent = thread = transport = container = None
    try:
        if kind == "python":
            transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
            agent = Agent(
                transport,
                DockerExecutor([image], client=client),
                worker_id,
                2,
                1024,
                service.settings,
            )
            thread = threading.Thread(target=agent.run, daemon=True)
            thread.start()
        else:
            container = client.containers.run(
                os.getenv("STRATA_TEST_CPP_IMAGE", "strata/worker-cpp:local"),
                detach=True,
                environment={
                    "STRATA_RPC_TARGET": f"127.0.0.1:{port}"
                    if platform.system() == "Linux"
                    else f"host.docker.internal:{port}",
                    "STRATA_WORKER_ID": worker_id,
                    "STRATA_WORKER_TOKEN": service.settings.worker_token,
                    "STRATA_ALLOWED_IMAGES": json.dumps([image]),
                    "STRATA_GPU_DISCOVERY_IMAGE": image,
                    "STRATA_WORKER_CPU": "2",
                    "STRATA_STORAGE_KEEPER_IMAGE": service.settings.storage_keeper_image,
                    "STRATA_WORKER_MEMORY_MB": "1024",
                },
                volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                labels={"strata.purpose": "product-gpu-runtime-test"},
                network_mode="host" if platform.system() == "Linux" else "default",
                mem_limit="256m",
                nano_cpus=1000000000,
            )
        jobs = [
            service.submit(
                JobSubmit(
                    name=f"CUDA evidence {index}",
                    image=image,
                    command=["1024", "2"],
                    resources={"cpu": 0.5, "memory_mb": 256, "gpus": 1, "gpu_memory_mb": 1024},
                    max_retries=0,
                    timeout_seconds=30,
                )
            )[0]
            for index in range(2)
        ]
        cpu_job = service.submit(
            JobSubmit(
                name="GPU access denied without reservation",
                image=image,
                command=["1024", "0"],
                resources={"cpu": 0.5, "memory_mb": 256},
                max_retries=0,
                timeout_seconds=30,
            )
        )[0]
        scheduler, concurrent, seen_running = Scheduler(service), False, False
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            scheduler.tick()
            with service.factory() as session:
                active = list(
                    session.scalars(
                        select(Attempt).where(Attempt.status.in_(["SCHEDULED", "RUNNING"]))
                    )
                )
                concurrent |= sum(bool(attempt.gpu_ids) for attempt in active) > 1
                seen_running |= any(attempt.status == "RUNNING" for attempt in active)
                states = [session.get(Job, job.id).status for job in [*jobs, cpu_job]]
            if all(state in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"} for state in states):
                assert states == ["SUCCEEDED", "SUCCEEDED", "FAILED"], states
                break
            time.sleep(0.2)
        else:
            logs = container.logs().decode(errors="replace") if container else "native Python agent"
            pytest.fail(f"GPU workloads did not finish: {states}; {logs}")
        assert seen_running and not concurrent
        with service.factory() as session:
            devices = list(
                session.scalars(select(GPUDevice).where(GPUDevice.worker_id == worker_id))
            )
            assert len(devices) >= 1 and all(device.allocated_to is None for device in devices)
            for job in jobs:
                attempt = session.scalar(select(Attempt).where(Attempt.job_id == job.id))
                assert len(attempt.gpu_ids) == 1
                assert attempt.provenance["gpus"] == attempt.gpu_ids
                assert "1024 values" in attempt.logs and "CUDA verified" in attempt.logs
                artifact = session.scalar(select(Artifact).where(Artifact.job_id == job.id))
                assert artifact.name == "cuda-result.json" and artifact.size > 0
            denied = session.scalar(select(Attempt).where(Attempt.job_id == cpu_job.id))
            assert denied.gpu_ids == [] and "CUDA verified" not in denied.logs
    finally:
        if agent:
            agent.stopped = True
            thread.join(timeout=15)
        if transport:
            transport.channel.close()
        if container:
            container.stop(timeout=5)
            container.remove(force=True)
        DockerExecutor([image], client=client).cleanup_orphans(worker_id)
        server.stop(0).wait()
