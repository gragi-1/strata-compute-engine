"""Actual Docker host cleanup without registering or restarting the workload agent."""

import os
import socket
from contextlib import suppress
from uuid import uuid4

import docker
import grpc
import pytest

from control_plane.rpc import engine_pb2 as pb
from control_plane.rpc.server import make_server
from scheduler.core import Scheduler
from tests.helpers import submit
from worker.executor import DockerExecutor
from worker.reaper import HostReaper
from worker.transport import Transport

pytestmark = pytest.mark.docker


def test_independent_reaper_preserves_live_and_other_clusters_and_removes_expired(service):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for actual host cleanup")
    client = docker.from_env(timeout=3)
    image = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    service.settings.allowed_images = [image]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
    executor = DockerExecutor(service.settings.allowed_images, client=client)
    container = unrelated = assignment = None
    worker = "reaper-test-" + uuid4().hex
    try:
        reply = transport.call(
            "Register", pb.RegisterRequest(worker_id=worker, cpu_total=2, memory_total_mb=512)
        )
        transport.session_id, executor.cluster_id = reply.session_id, reply.cluster_id
        job = submit(service, image=image, command=["python", "-c", "import time; time.sleep(60)"])
        Scheduler(service).schedule()
        assignment = transport.call(
            "Poll", pb.PollRequest(worker_id=worker, session_id=reply.session_id)
        ).assignments[0]
        container = executor.create(assignment, worker)
        transport.call("Start", transport.credentials(assignment))
        container.start()
        unrelated = client.containers.run(
            image,
            ["python", "-c", "import time; time.sleep(60)"],
            detach=True,
            network_disabled=True,
            mem_limit="128m",
            nano_cpus=100000000,
            labels={
                "strata.cluster": str(uuid4()),
                "strata.worker": worker,
                "strata.attempt": str(uuid4()),
                "strata.purpose": "product-reaper-test",
            },
        )
        reaper = HostReaper(transport, client)
        assert reaper.tick(apply=True)["container_candidates"] == []
        container.reload()
        assert container.status == "running"
        service.clock.advance(service.settings.lease_seconds + 1)
        preview = reaper.tick()
        keeper = client.containers.get("strata-keeper-" + assignment.attempt_id)
        assert set(preview["container_candidates"]) == {container.id, keeper.id}
        container.reload()
        assert container.status == "running"
        report = reaper.tick(apply=True)
        assert set(report["removed_containers"]) == {container.id, keeper.id}
        assert report["errors"] == 0
        with pytest.raises(docker.errors.NotFound):
            client.containers.get(container.id)
        unrelated.reload()
        assert unrelated.status == "running"
        Scheduler(service).recover()
        assert service.get_job(job.id).status == "RETRYING"
        server.stop(0).wait()
        with pytest.raises(grpc.RpcError):
            reaper.tick(apply=True)
        unrelated.reload()
        assert unrelated.status == "running"
    finally:
        for item in (container, unrelated):
            if item:
                with suppress(docker.errors.NotFound):
                    item.remove(force=True)
        if assignment:
            with suppress(docker.errors.NotFound):
                client.containers.get("strata-keeper-" + assignment.attempt_id).remove(force=True)
            for prefix in ("strata-output-", "strata-input-"):
                with suppress(docker.errors.NotFound):
                    client.volumes.get(prefix + assignment.attempt_id).remove(force=True)
        transport.channel.close()
        server.stop(0).wait()
