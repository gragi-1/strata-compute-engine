"""Real bounded host provisioning, actual CPU work and crash-safe intent adoption."""

import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from uuid import uuid4

import docker
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from control_plane.cluster import ClusterService
from control_plane.models import Attempt, Job, ProvisionedWorker, Worker
from control_plane.provisioning import (
    DockerPoolAdapter,
    PoolController,
    PoolService,
    PoolSettings,
    PoolUpdate,
)
from control_plane.rpc.server import make_server
from control_plane.schemas import AdmissionUpdate
from scheduler.core import Scheduler
from tests.helpers import submit

pytestmark = [pytest.mark.docker, pytest.mark.postgres]


@pytest.fixture
def pool_runtime(postgres_service, request):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for actual Docker provisioning")
    svc = postgres_service
    client = docker.from_env(timeout=10)
    image = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    svc.settings.allowed_images = [image]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(svc, f"127.0.0.1:{port}")
    server.start()
    kind = getattr(request, "param", "python")
    agent_image = client.images.get(
        os.getenv(
            "STRATA_TEST_CPP_IMAGE" if kind == "cpp" else "STRATA_TEST_CONTROL_IMAGE",
            "strata/worker-cpp:local" if kind == "cpp" else "strata/control-plane:local",
        )
    ).id
    config = PoolSettings(
        id="runtime-" + uuid4().hex,
        image=agent_image,
        kind=kind,
        rpc_target=f"127.0.0.1:{port}"
        if platform.system() == "Linux"
        else f"host.docker.internal:{port}",
        network="host" if platform.system() == "Linux" else "bridge",
        cpu_per_worker=0.5,
        memory_per_worker_mb=256,
        host_cpu_budget=2,
        host_memory_budget_mb=1024,
        maximum_workers=2,
        idle_seconds=1,
    )
    adapter = DockerPoolAdapter(svc, config, client)
    controller = PoolController(svc, config, adapter)
    controller.tick()
    try:
        yield svc, client, config, adapter, controller, image
    finally:
        with svc.factory() as session:
            rows = list(
                session.scalars(
                    select(ProvisionedWorker).where(ProvisionedWorker.pool_id == config.id)
                )
            )
            attempts = list(
                session.scalars(
                    select(Attempt).where(Attempt.worker_id.in_([row.worker_id for row in rows]))
                )
            )
        # Fixture cleanup owns only UUID-named, cluster-labelled test agents/workloads.
        for row in rows:
            with suppress(docker.errors.NotFound):
                adapter.remove(row.worker_id)
        for attempt in attempts:
            with suppress(docker.errors.NotFound):
                keeper = client.containers.get("strata-keeper-" + attempt.id)
                assert keeper.labels.get("strata.cluster") == adapter.cluster_id
                keeper.remove(force=True)
            with suppress(docker.errors.NotFound):
                container = client.containers.get("strata-" + attempt.id)
                assert container.labels.get("strata.cluster") == adapter.cluster_id
                container.remove(force=True)
            for prefix in ("strata-output-", "strata-input-"):
                with suppress(docker.errors.NotFound):
                    volume = client.volumes.get(prefix + attempt.id)
                    assert volume.attrs["Labels"]["strata.cluster"] == adapter.cluster_id
                    volume.remove()
        server.stop(0).wait()
        client.close()


def wait_for(predicate, evidence, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    pytest.fail(evidence())


@pytest.mark.parametrize("pool_runtime", ["python", "cpp"], indirect=True)
def test_actual_scale_up_runs_jobs_and_disable_drains_before_scale_down(pool_runtime):
    svc, client, config, adapter, controller, image = pool_runtime
    pools = PoolService(svc)
    jobs = [
        submit(
            svc,
            image=image,
            command=[
                "python",
                "-u",
                "-c",
                "import time; print('elastic execution'); time.sleep(20)",
            ],
            # Two workloads plus their .01 CPU / 32 MiB keepers fit each .5 / 256 node.
            resources={"cpu": 0.2, "memory_mb": 96},
            max_retries=0,
        )
        for _ in range(4)
    ]
    assert controller.tick()["desired"] == 0
    pools.update(config.id, PoolUpdate(enabled=True, minimum=0, maximum=2))
    ClusterService(svc).update(AdmissionUpdate(accepting_jobs=True, scheduling_enabled=False))
    assert controller.tick()["desired"] == 2
    assert pools.workers(config.id) == []
    ClusterService(svc).update(AdmissionUpdate(accepting_jobs=True, scheduling_enabled=True))
    assert controller.tick() == {"leader": True, "desired": 2, "errors": 0}
    records = pools.workers(config.id)
    assert len(records) == 2

    def registered():
        controller.tick()
        with svc.factory() as session:
            return len(list(session.scalars(select(Worker)))) == 2

    wait_for(registered, lambda: str(pools.workers(config.id)))
    for row in pools.workers(config.id):
        item = adapter.container(row["worker_id"])
        assert item.attrs["HostConfig"]["NanoCpus"] == 250000000
        assert item.attrs["HostConfig"]["Memory"] == 256 * 1024**2
        assert "strata.worker" not in item.labels
    scheduler = Scheduler(svc)
    assert scheduler.schedule() == 4

    def started():
        return all(svc.get_job(job.id).status == "RUNNING" for job in jobs)

    wait_for(started, lambda: str([svc.get_job(job.id).status for job in jobs]))
    pools.update(config.id, PoolUpdate(enabled=False, minimum=0, maximum=2))
    assert controller.tick()["desired"] == 0
    # An occupied agent is drained but remains alive until every attempt finishes.
    for row in pools.workers(config.id):
        assert row["phase"] == "DRAINING"
        assert adapter.container(row["worker_id"]).status == "running"
    wait_for(
        lambda: all(svc.get_job(job.id).status == "SUCCEEDED" for job in jobs),
        lambda: str([svc.get_job(job.id).status for job in jobs]),
    )
    assert controller.tick()["errors"] == 0
    assert all(row["phase"] == "REMOVED" for row in pools.workers(config.id))
    assert all(adapter.container(row["worker_id"]) is None for row in records)
    with svc.factory() as session:
        assert all(worker.status == "LOST" for worker in session.scalars(select(Worker)))
        assert all(job.attempts_count == 1 for job in session.scalars(select(Job)))


def test_create_before_commit_is_adopted_and_removal_failure_keeps_drain_durable(
    pool_runtime, monkeypatch
):
    svc, _, config, adapter, controller, _ = pool_runtime
    pools = PoolService(svc)
    pools.update(config.id, PoolUpdate(enabled=True, minimum=1, maximum=1))
    original = adapter.ensure

    def interrupted(worker_id):
        original(worker_id)
        raise RuntimeError("synthetic interrupted create containing a secret")

    monkeypatch.setattr(adapter, "ensure", interrupted)
    assert controller.tick()["errors"] == 1
    row = pools.workers(config.id)[0]
    assert row["phase"] == "REQUESTED" and row["container_id"] is None
    existing = adapter.container(row["worker_id"])
    assert existing is not None
    monkeypatch.setattr(adapter, "ensure", original)
    svc.clock.advance(31)
    # The deterministic name and labels recover the original external create, without
    # starting a second agent or releasing its charged slot.
    assert controller.tick()["errors"] == 0
    adopted = pools.workers(config.id)
    assert len(adopted) == 1 and adopted[0]["container_id"] == existing.id
    pools.update(config.id, PoolUpdate(enabled=False, minimum=0, maximum=1))
    remove = adapter.remove

    def failed_remove(worker_id):
        raise ValueError("synthetic failed removal containing a secret")

    monkeypatch.setattr(adapter, "remove", failed_remove)
    assert controller.tick()["errors"] == 1
    assert pools.workers(config.id)[0]["phase"] == "DRAINING"
    assert "secret" not in str(pools.list_pools()) + str(pools.workers(config.id))
    existing.reload()
    assert existing.status == "running"
    monkeypatch.setattr(adapter, "remove", remove)
    svc.clock.advance(31)
    assert controller.tick()["errors"] == 0
    assert pools.workers(config.id)[0]["phase"] == "REMOVED"


def test_process_exit_after_docker_create_recovers_original_intent(pool_runtime):
    svc, _, config, adapter, controller, _ = pool_runtime
    pools = PoolService(svc)
    pools.update(config.id, PoolUpdate(enabled=True, minimum=1, maximum=1))
    with svc.factory() as session:
        schema = session.scalar(text("SELECT current_schema()"))
    settings = svc.settings.model_dump(mode="json")
    settings["database_url"] = (
        make_url(svc.settings.database_url)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    environment = dict(
        os.environ,
        STRATA_CRASH_SETTINGS=json.dumps(settings),
        STRATA_CRASH_POOL=config.model_dump_json(),
        STRATA_CRASH_CLOCK=svc.clock().isoformat(),
    )
    code = """
import json, os
from datetime import datetime
from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.services import EngineService
from control_plane.provisioning import PoolSettings, DockerPoolAdapter, PoolController
settings = Settings(**json.loads(os.environ['STRATA_CRASH_SETTINGS']))
config = PoolSettings(**json.loads(os.environ['STRATA_CRASH_POOL']))
svc = EngineService(sessions(make_engine(settings.database_url)), settings,
                    lambda: datetime.fromisoformat(os.environ['STRATA_CRASH_CLOCK']))
adapter = DockerPoolAdapter(svc, config)
original = adapter.ensure
def terminate(worker_id):
    original(worker_id)
    os._exit(23)
adapter.ensure = terminate
PoolController(svc, config, adapter).tick()
"""
    child = subprocess.run(
        [sys.executable, "-c", code], env=environment, capture_output=True, timeout=30
    )
    assert child.returncode == 23, child.stderr.decode(errors="replace")
    record = pools.workers(config.id)[0]
    assert record["phase"] == "REQUESTED" and record["container_id"] is None
    created = adapter.container(record["worker_id"])
    assert created is not None
    assert controller.tick()["errors"] == 0
    recovered = pools.workers(config.id)
    assert len(recovered) == 1 and recovered[0]["container_id"] == created.id


def test_concurrent_controller_does_not_create_extra_agents_and_foreign_containers_survive(
    pool_runtime, monkeypatch
):
    svc, client, config, adapter, controller, image = pool_runtime
    pools = PoolService(svc)
    pools.update(config.id, PoolUpdate(enabled=True, minimum=1, maximum=1))
    entered, release = threading.Event(), threading.Event()
    original = adapter.ensure

    def blocked(worker_id):
        entered.set()
        assert release.wait(10)
        return original(worker_id)

    monkeypatch.setattr(adapter, "ensure", blocked)
    unrelated = client.containers.run(
        image,
        ["python", "-c", "import time; time.sleep(60)"],
        detach=True,
        network_disabled=True,
        mem_limit="128m",
        nano_cpus=100000000,
        labels={
            "strata.cluster": str(uuid4()),
            "strata.pool": config.id,
            "strata.provisioned-worker": "foreign-" + uuid4().hex,
        },
    )
    try:
        with ThreadPoolExecutor(2) as executor:
            owner = executor.submit(controller.tick)
            assert entered.wait(10)
            try:
                assert executor.submit(controller.tick).result(timeout=5) == {"leader": False}
            finally:
                release.set()
            assert owner.result(timeout=20)["errors"] == 0
        assert len(pools.workers(config.id)) == 1
        pools.update(config.id, PoolUpdate(enabled=False, minimum=0, maximum=1))
        assert controller.tick()["errors"] == 0
        unrelated.reload()
        assert unrelated.status == "running"
    finally:
        unrelated.remove(force=True)
