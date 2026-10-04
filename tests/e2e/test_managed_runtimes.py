"""Real cooperating nodes and stateful notebook/service sessions through both agents."""

import base64
import json
import math
import os
import platform
import socket
import threading
import time
from datetime import UTC, datetime
from uuid import uuid4

import docker
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from control_plane.api import create_app
from control_plane.models import Artifact, Attempt, Worker
from control_plane.rpc.server import make_server
from control_plane.runtimes import GroupSubmit, RuntimeService, SessionSubmit
from control_plane.schemas import JobSubmit
from control_plane.storage import BlobStore
from scheduler.core import Scheduler
from worker.agent import Agent
from worker.executor import DockerExecutor
from worker.transport import Transport

pytestmark = pytest.mark.docker


@pytest.fixture
def runtime(service, tmp_path):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for actual managed runtimes")
    image = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    service.settings.allowed_images = [image]
    service.settings.worker_cache_root = tmp_path / "cache"
    service.settings.heartbeat_interval = 1
    client = docker.from_env(timeout=3)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    rpc = make_server(service, f"127.0.0.1:{port}")
    rpc.start()
    agents, cpp, workers = [], [], []

    def start(kind):
        worker_id = "runtime-" + uuid4().hex
        workers.append(worker_id)
        if kind == "python":
            transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
            executor = DockerExecutor([image], settings=service.settings)
            agent = Agent(transport, executor, worker_id, 2, 1024, service.settings)
            thread = threading.Thread(target=agent.run)
            agents.append((agent, thread, transport))
            thread.start()
        else:
            cpp.append(
                client.containers.run(
                    os.getenv("STRATA_TEST_CPP_IMAGE", "strata/worker-cpp:runtime-test"),
                    detach=True,
                    environment={
                        "STRATA_RPC_TARGET": f"127.0.0.1:{port}"
                        if platform.system() == "Linux"
                        else f"host.docker.internal:{port}",
                        "STRATA_WORKER_ID": worker_id,
                        "STRATA_WORKER_TOKEN": service.settings.worker_token,
                        "STRATA_ALLOWED_IMAGES": json.dumps([image]),
                        "STRATA_STORAGE_KEEPER_IMAGE": service.settings.storage_keeper_image,
                        "STRATA_WORKER_CPU": "2",
                        "STRATA_WORKER_MEMORY_MB": "1024",
                    },
                    volumes={
                        "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}
                    },
                    network_mode="host" if platform.system() == "Linux" else "bridge",
                    labels={
                        "strata.purpose": "managed-runtime-test",
                        "strata.test-worker": worker_id,
                    },
                    mem_limit="256m",
                    nano_cpus=250000000,
                )
            )
        return worker_id

    scheduler = Scheduler(service)

    def wait(predicate, seconds=70):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            scheduler.tick()
            if predicate():
                return
            time.sleep(0.2)
        with service.factory() as s:
            attempts = [
                {"job": a.job_id, "status": a.status, "reason": a.reason, "logs": a.logs[-4000:]}
                for a in s.scalars(select(Attempt))
            ]
        pytest.fail(
            str(
                {
                    "attempts": attempts,
                    "cpp": [c.logs(tail=10).decode(errors="replace") for c in cpp],
                }
            )
        )

    api = TestClient(create_app(service.settings, service))
    try:
        yield service, RuntimeService(service), api, client, image, start, wait
    finally:
        for agent, thread, transport in agents:
            agent.stopped = True
            thread.join(timeout=15)
            assert not thread.is_alive()
            transport.channel.close()
            agent.executor.client.close()
        for item in cpp:
            item.remove(force=True)
        # Only resources owned by these UUID worker identities belong to this fixture.
        for worker in workers:
            for c in client.containers.list(all=True, filters={"label": f"strata.worker={worker}"}):
                c.remove(force=True)
            for v in client.volumes.list(filters={"label": f"strata.worker={worker}"}):
                v.remove()
        rpc.stop(0).wait()
        api.close()
        client.close()


def job(image, command):
    return JobSubmit(
        name="Managed runtime",
        image=image,
        command=command,
        resources={"cpu": 0.5, "memory_mb": 128},
        max_retries=0,
        timeout_seconds=120,
    )


def test_python_and_cpp_cooperate_in_distributed_integral_and_barrier(runtime):
    svc, manager, _, client, image, start, wait = runtime
    workers = [start("python"), start("cpp")]
    code = """import sys,json,math
sys.path.insert(0,'/output/.strata')
from runtime import Collective
c=Collective(); n=100000
local=sum(4/(1+((i+.5)/n)**2)/n for i in range(c.rank,n,c.size))
total=c.reduce([local])[0]; c.barrier()
assert abs(total-math.pi)<1e-8
open('/output/integral.json','w').write(json.dumps({'rank':c.rank,'pi':total}))
"""
    group, _ = manager.create_group(
        GroupSubmit(name="Distributed integral", job=job(image, ["python", "-c", code])), None
    )
    wait(lambda: manager.group(group.id)["status"] in {"SUCCEEDED", "FAILED"})
    assert manager.group(group.id)["status"] == "SUCCEEDED"
    with svc.factory() as s:
        attempts = list(s.scalars(select(Attempt)))
        assert len(attempts) == 2 and {a.worker_id for a in attempts} == set(workers)
        assert all(a.number == 1 for a in attempts)
        outputs = list(s.scalars(select(Artifact)))
        assert len(outputs) == 2
        results = [
            json.loads(BlobStore(svc.settings).get_path(o.sha256, o.size).read_bytes())
            for o in outputs
        ]
        assert {r["rank"] for r in results} == {0, 1}
        assert all(abs(r["pi"] - math.pi) < 1e-8 for r in results)
        assert all(w.running_jobs == 0 for w in s.scalars(select(Worker)))
    wait(
        lambda: (
            not client.api.containers(all=True, filters={"label": f"strata.worker={workers[0]}"})
        )
    )


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_stateful_notebook_errors_output_export_and_idle_shutdown(runtime, kind):
    svc, manager, api, client, image, start, wait = runtime
    worker = start(kind)
    session, _ = manager.create_session(
        SessionSubmit(job=job(image, ["python", "-c", "pass"]), idle_seconds=10), None
    )
    wait(lambda: svc.get_job(session.job_id).status == "RUNNING")

    def execute(code):
        response = api.post(
            f"/interactive-sessions/{session.id}/cells",
            headers={"Idempotency-Key": uuid4().hex},
            json={"code": code},
        )
        assert response.status_code == 201, response.text
        cell = response.json()
        wait(
            lambda: (
                api.get(f"/interactive-sessions/{session.id}/cells/{cell['id']}").json()["status"]
                in {"SUCCEEDED", "FAILED"}
            )
        )
        return api.get(f"/interactive-sessions/{session.id}/cells/{cell['id']}").json()

    assert execute("x=21; print(x)")["result"]["stdout"] == "21\n"
    assert (
        execute("print(x*2); open('/output/result.txt','w').write(str(x*2))")["result"]["stdout"]
        == "42\n"
    )
    assert execute("raise ValueError('controlled error')")["status"] == "FAILED"
    bounded = execute("print('ñ'*10000)")["result"]["stdout"]
    assert len(bounded.encode()) == 4096
    notebook = api.get(f"/interactive-sessions/{session.id}/notebook").json()
    assert len(notebook["cells"]) == 4 and notebook["cells"][1]["outputs"][0]["text"] == "42\n"
    svc.clock.advance(11)
    wait(lambda: svc.get_job(session.job_id).status == "CANCELLED")
    with svc.factory() as s:
        assert s.get(Worker, worker).running_jobs == 0
        output = s.scalar(select(Artifact).where(Artifact.job_id == session.job_id))
        assert output.name == "result.txt"
        assert BlobStore(svc.settings).get_path(output.sha256, output.size).read_bytes() == b"42"
    wait(lambda: not client.api.containers(all=True, filters={"label": f"strata.worker={worker}"}))


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_container_local_http_proxy_has_authorized_lifecycle_and_no_published_port(runtime, kind):
    svc, manager, api, client, image, start, wait = runtime
    worker = start(kind)
    session, _ = manager.create_session(
        SessionSubmit(
            kind="service",
            idle_seconds=10,
            job=job(
                image,
                [
                    "python",
                    "-m",
                    "http.server",
                    "8080",
                    "--bind",
                    "127.0.0.1",
                    "--directory",
                    "/output",
                ],
            ),
        ),
        None,
    )
    wait(lambda: svc.get_job(session.job_id).status == "RUNNING")
    # RUNNING acknowledges the wrapper; wait for its actual server before asking once.
    time.sleep(2)
    response = api.post(
        f"/interactive-sessions/{session.id}/proxy",
        headers={"Idempotency-Key": "request"},
        json={"path": "/"},
    )
    assert response.status_code == 201, response.text
    cell = response.json()
    wait(
        lambda: (
            api.get(f"/interactive-sessions/{session.id}/cells/{cell['id']}").json()["status"]
            in {"SUCCEEDED", "FAILED"}
        )
    )
    result = api.get(f"/interactive-sessions/{session.id}/cells/{cell['id']}").json()["result"]
    assert result["status"] == 200 and b"Directory listing" in base64.b64decode(
        result["body_base64"]
    )
    items = client.containers.list(filters={"label": [f"strata.worker={worker}", "strata.attempt"]})
    assert all(
        i.attrs["HostConfig"]["NetworkMode"] == "none" or i.attrs["Config"].get("NetworkDisabled")
        for i in items
    )
    assert all(not i.attrs["HostConfig"].get("PortBindings") for i in items)
    manager.stop_session(session.id)
    wait(lambda: svc.get_job(session.job_id).status == "CANCELLED")


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_slow_batch_launch_renews_pending_and_running_leases(runtime, kind):
    svc, _, _, _, image, start, wait = runtime
    svc.clock = lambda: datetime.now(UTC)
    svc.settings.lease_seconds = 6
    svc.settings.worker_timeout = 8
    svc.settings.scheduler_batch_size = 6
    worker = start(kind)
    from tests.helpers import submit

    jobs = [
        submit(
            svc,
            image=image,
            command=["python", "-c", "open('/output/burst.txt','w').write('retained')"],
            resources={"cpu": 0.1, "memory_mb": 64},
            max_retries=0,
        )
        for _ in range(6)
    ]
    wait(
        lambda: all(svc.get_job(j.id).status in {"SUCCEEDED", "FAILED"} for j in jobs), seconds=100
    )
    assert all(svc.get_job(j.id).status == "SUCCEEDED" for j in jobs)
    with svc.factory() as s:
        attempts = list(s.scalars(select(Attempt)))
        assert len(attempts) == 6 and all(a.number == 1 for a in attempts)
        outputs = list(s.scalars(select(Artifact)))
        assert len(outputs) == 6
        assert all(
            BlobStore(svc.settings).get_path(o.sha256, o.size).read_bytes() == b"retained"
            for o in outputs
        )
        assert s.get(Worker, worker).running_jobs == 0
        assert s.get(Worker, worker).cpu_reserved == 0
