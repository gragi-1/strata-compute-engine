"""Actual kernel output limits, artifact retention and helper-loss failure on both agents."""

import json
import os
import platform
import socket
import threading
import time
from pathlib import Path
from uuid import uuid4

import docker
import pytest
from sqlalchemy import select

from control_plane.models import Artifact, Attempt, Worker
from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc.server import make_server
from control_plane.storage import BlobStore
from scheduler.core import Scheduler
from tests.helpers import submit
from worker.agent import Agent
from worker.executor import DockerExecutor
from worker.transport import Transport

pytestmark = pytest.mark.docker


def wait(predicate, diagnostic, seconds=35):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    pytest.fail(str(diagnostic()))


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_actual_output_quotas_collection_and_keeper_failure(service, tmp_path, kind):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for actual kernel output quotas")
    client = docker.from_env(timeout=3)
    image = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    settings = service.settings
    settings.allowed_images = [image]
    settings.worker_output_bytes, settings.worker_output_inodes = 32 * 1024**2, 64
    settings.worker_cache_root = tmp_path / "worker-cache"
    settings.heartbeat_interval = 1
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    rpc = make_server(service, f"127.0.0.1:{port}")
    rpc.start()
    worker_id = "quota-" + uuid4().hex
    executor = DockerExecutor([image], client=client, settings=settings)
    agent = transport = thread = cpp = None
    scheduler = Scheduler(service)

    def advance():
        scheduler.tick()

    def evidence(job):
        with service.factory() as session:
            rows = list(session.scalars(select(Attempt).where(Attempt.job_id == job.id)))
            return {
                "status": service.get_job(job.id).status,
                "attempts": [{"reason": a.reason, "logs": a.logs[-4096:]} for a in rows],
                "cpp": cpp.logs(tail=10).decode(errors="replace") if cpp else None,
            }

    def finished(job, status):
        advance()
        row = service.get_job(job.id)
        if row.status in {"FAILED", "SUCCEEDED", "CANCELLED", "TIMED_OUT"}:
            if row.status != status:
                pytest.fail(json.dumps(evidence(job), indent=2))
            return True
        return False

    try:
        if kind == "python":
            transport = Transport(f"127.0.0.1:{port}", settings.worker_token)
            agent = Agent(transport, executor, worker_id, 2, 1024, settings)
            thread = threading.Thread(target=agent.run)
            thread.start()
        else:
            cpp = client.containers.run(
                os.getenv("STRATA_TEST_CPP_IMAGE", "strata/worker-cpp:local"),
                detach=True,
                environment={
                    "STRATA_RPC_TARGET": f"127.0.0.1:{port}"
                    if platform.system() == "Linux"
                    else f"host.docker.internal:{port}",
                    "STRATA_WORKER_ID": worker_id,
                    "STRATA_WORKER_TOKEN": settings.worker_token,
                    "STRATA_ALLOWED_IMAGES": json.dumps([image]),
                    "STRATA_STORAGE_KEEPER_IMAGE": settings.storage_keeper_image,
                    "STRATA_WORKER_OUTPUT_BYTES": str(settings.worker_output_bytes),
                    "STRATA_WORKER_OUTPUT_INODES": str(settings.worker_output_inodes),
                    "STRATA_WORKER_CACHE_ROOT": "/tmp/strata-input-cache",
                },
                volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                network_mode="host" if platform.system() == "Linux" else "bridge",
                labels={"strata.purpose": "output-quota-test", "strata.test-worker": worker_id},
                mem_limit="256m",
                nano_cpus=250000000,
            )
        command = """import errno,json,os
f=open('/output/fill','wb',buffering=0); total=0
try:
    for _ in range(1024): total+=f.write(b'x'*65536)
except OSError as e: assert e.errno==errno.ENOSPC,e
else: raise AssertionError('byte quota absent')
f.close(); os.unlink('/output/fill'); count=0
try:
    for i in range(128):
        open('/output/empty-'+str(i),'wb').close(); count+=1
except OSError as e: assert e.errno==errno.ENOSPC,e
else: raise AssertionError('inode quota absent')
for i in range(count): os.unlink('/output/empty-'+str(i))
open('/output/result.json','w').write(json.dumps({'bytes':total,'files':count}))
"""
        job = submit(
            service,
            image=image,
            command=["python", "-c", command],
            resources={"cpu": 0.5, "memory_mb": 128},
            max_retries=0,
        )
        wait(lambda: finished(job, "SUCCEEDED"), lambda: evidence(job))
        with service.factory() as session:
            outputs = list(session.scalars(select(Artifact).where(Artifact.job_id == job.id)))
        assert len(outputs) == 1 and outputs[0].name == "result.json"
        result = json.loads(
            BlobStore(settings).get_path(outputs[0].sha256, outputs[0].size).read_bytes()
        )
        assert result == {"bytes": settings.worker_output_bytes, "files": 63}
        # Oversized transfer is an explicit failure, never an indefinitely renewing attempt.
        large = submit(
            service,
            image=image,
            command=["python", "-c", "open('/output/large.bin','wb').write(b'x'*(17*1024*1024))"],
            resources={"cpu": 0.5, "memory_mb": 128},
            max_retries=0,
        )
        wait(lambda: finished(large, "FAILED"), lambda: evidence(large))
        with service.factory() as session:
            attempt = session.scalar(select(Attempt).where(Attempt.job_id == large.id))
            assert attempt.reason == "output exceeds artifact transfer limit"
        killed = submit(
            service,
            image=image,
            command=[
                "python",
                "-u",
                "-c",
                "import time; print('retention active'); time.sleep(60)",
            ],
            resources={"cpu": 0.5, "memory_mb": 128},
            max_retries=0,
        )

        def running():
            advance()
            return service.get_job(killed.id).status == "RUNNING"

        wait(running, lambda: evidence(killed))
        with service.factory() as session:
            attempt = session.scalar(select(Attempt).where(Attempt.job_id == killed.id))
            keeper = client.containers.get("strata-keeper-" + attempt.id)
            assert keeper.labels["strata.worker"] == worker_id
            keeper.remove(force=True)
        wait(lambda: finished(killed, "FAILED"), lambda: evidence(killed))
        with service.factory() as session:
            row = session.get(Worker, worker_id)
            assert row.running_jobs == 0 and row.memory_reserved_mb == 0
            assert row.cpu_reserved == pytest.approx(0)
            assert all(
                session.scalar(select(Attempt).where(Attempt.job_id == item.id)).number == 1
                for item in [job, large, killed]
            )
            attempt = session.scalar(select(Attempt).where(Attempt.job_id == killed.id))
            assert attempt.reason == "bounded output storage lost"
        wait(
            lambda: (
                not client.api.containers(all=True, filters={"label": f"strata.worker={worker_id}"})
            ),
            lambda: "attempt containers were not removed",
        )
        assert client.volumes.list(filters={"label": f"strata.worker={worker_id}"}) == []
        if destination := os.getenv("STRATA_TEST_OUTPUT_EVIDENCE"):
            target = Path(destination)
            target.mkdir(parents=True, exist_ok=True)
            (target / f"output-quota-{kind}.json").write_text(
                json.dumps(
                    {
                        "passed": True,
                        "worker": kind,
                        "physical_hosts": 1,
                        "kernel_output_bytes": settings.worker_output_bytes,
                        "kernel_inodes": settings.worker_output_inodes,
                        "noswap": True,
                        "stopped_container_output_collected": True,
                        "oversized_transfer_failed": True,
                        "keeper_loss_failed": True,
                        "attempts_per_job": 1,
                        "reservations_released": True,
                        "keeper_image": client.images.get(settings.storage_keeper_image).id,
                        "agent_image": cpp.image.id if cpp else "native Python",
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
    finally:
        if agent:
            agent.stopped = True
            thread.join(timeout=10)
            assert not thread.is_alive()
        if cpp:
            assert cpp.labels["strata.test-worker"] == worker_id
            cpp.remove(force=True)
        executor.cleanup_orphans(worker_id)
        if transport:
            transport.channel.close()
        rpc.stop(0).wait()
        client.close()


def test_keeper_expires_without_renewal_and_name_collision_preserves_foreign_container(service):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for actual keeper expiry")
    client = docker.from_env(timeout=3)
    image = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    worker = "keeper-expiry-" + uuid4().hex
    service.settings.worker_output_bytes = 1024**2
    executor = DockerExecutor([image], grace=0, client=client, settings=service.settings)
    assignment = pb.Assignment(
        attempt_id=str(uuid4()),
        image=image,
        command=["python", "-c", "print('unused')"],
        cpu=0.25,
        memory_mb=128,
        lease_seconds=0.1,
    )
    workload = foreign = None
    try:
        workload = executor.create(assignment, worker)
        keeper = client.containers.get("strata-keeper-" + assignment.attempt_id)
        # An unstarted workload holds no active mount. Output expires independently.
        wait(
            lambda: not executor.storage_alive(assignment.attempt_id),
            lambda: keeper.logs().decode(errors="replace"),
            seconds=15,
        )
        executor.remove(workload)
        workload = None
        assignment.attempt_id = str(uuid4())
        foreign = client.containers.create(
            image,
            ["python", "-c", "print('foreign')"],
            name="strata-keeper-" + assignment.attempt_id,
            labels={"strata.purpose": "keeper-collision-test", "strata.foreign-test": worker},
        )
        with pytest.raises(docker.errors.APIError):
            executor.create(assignment, worker)
        foreign.reload()
        assert foreign.status == "created" and foreign.labels["strata.foreign-test"] == worker
        assert client.volumes.list(filters={"label": f"strata.worker={worker}"}) == []
    finally:
        if workload:
            executor.remove(workload)
        if foreign:
            assert foreign.labels["strata.foreign-test"] == worker
            foreign.remove()
        executor.cleanup_orphans(worker)
        client.close()
