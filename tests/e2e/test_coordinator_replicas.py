"""Actual API/RPC replica loss, shared S3 bytes and live execution through HAProxy."""

import hashlib
import json
import os
import platform
import socket
import threading
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import docker
import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from control_plane.models import Admission, Attempt, Worker
from control_plane.storage import BlobStore
from strata_sdk import Client
from tests.integration.test_tls import certificates
from worker.agent import Agent
from worker.executor import DockerExecutor
from worker.transport import Transport

pytestmark = [pytest.mark.docker, pytest.mark.postgres]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(predicate, diagnostics, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except httpx.TransportError:
            pass  # Startup and failover can reset the explicitly owned proxy connection.
        time.sleep(0.2)
    pytest.fail(diagnostics())


@pytest.fixture
def replicas(postgres_service, tmp_path, monkeypatch):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for real replica loss tests")
    endpoint = os.getenv("STRATA_TEST_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("set STRATA_TEST_S3_ENDPOINT for shared real S3 storage")
    svc = postgres_service
    host_network = platform.system() == "Linux"
    svc.clock = None
    cert, key = certificates(tmp_path)
    monkeypatch.setenv("STRATA_RPC_CA", str(cert))
    svc.settings.worker_token = "synthetic-replica-worker-token-32-characters"
    api_key = "synthetic-replica-api-key-32-characters"
    svc.settings.api_keys = {api_key: "admin"}
    workload = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    svc.settings.allowed_images = [workload]
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.getenv("STRATA_TEST_S3_KEY", "strata-test"))
    monkeypatch.setenv(
        "AWS_SECRET_ACCESS_KEY", os.getenv("STRATA_TEST_S3_SECRET", "strata-test-secret")
    )
    svc.settings.storage_backend, svc.settings.s3_endpoint_url = "s3", endpoint
    svc.settings.s3_bucket = "strata-replicas-" + uuid4().hex
    store = BlobStore(svc.settings)
    store.client.create_bucket(Bucket=svc.settings.s3_bucket)
    client = docker.from_env(timeout=10)
    prefix = "strata-replicas-" + uuid4().hex
    labels = {"strata.purpose": "replica-integration-test", "strata.test": prefix}
    network = client.networks.create(prefix, labels=labels)
    containers, volumes = [], []
    with svc.factory() as session:
        schema = session.scalar(text("SELECT current_schema()"))
        admission_id = session.get(Admission, 1).cluster_id
    url = make_url(svc.settings.database_url).update_query_dict(
        {"options": f"-csearch_path={schema}", "connect_timeout": "2"}
    )
    # Linux CI containers reach the native fixture via the host gateway as on Docker Desktop.
    if not host_network and url.host in {"localhost", "127.0.0.1"}:
        url = url.set(host="host.docker.internal")
    s3_url = (
        endpoint
        if host_network
        else str(httpx.URL(endpoint).copy_with(host="host.docker.internal"))
    )
    environment = {
        "STRATA_DATABASE_URL": url.render_as_string(hide_password=False),
        "STRATA_WORKER_TOKEN": svc.settings.worker_token,
        "STRATA_API_KEYS": json.dumps(svc.settings.api_keys),
        "STRATA_ALLOWED_IMAGES": json.dumps([workload]),
        "STRATA_API_BIND": "0.0.0.0",
        "STRATA_RPC_BIND": "0.0.0.0:50051",
        "STRATA_STORAGE_BACKEND": "s3",
        "STRATA_S3_ENDPOINT_URL": s3_url,
        "STRATA_S3_BUCKET": svc.settings.s3_bucket,
        "STRATA_ARTIFACT_ROOT": "/app/data/artifacts",
        "STRATA_HEARTBEAT_INTERVAL": "1",
        "STRATA_TLS_CERT": "/run/strata/rpc.pem",
        "STRATA_TLS_KEY": "/run/strata/rpc.key",
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    }
    image = os.getenv("STRATA_TEST_CONTROL_IMAGE", "strata/control-plane:local")
    try:
        nodes = []
        config_text = (
            Path(__file__).resolve().parents[2] / "deploy/haproxy-replicas.cfg"
        ).read_text()
        api_port, rpc_port = free_port(), free_port()
        for replica in ("node-a", "node-b"):
            bind_host = "127.0.0.1" if host_network else "0.0.0.0"
            node_api_port, node_rpc_port = (
                (free_port(), free_port()) if host_network else (8000, 50051)
            )
            if host_network:
                config_text = config_text.replace(
                    f"server {replica} {replica}:8000",
                    f"server {replica} 127.0.0.1:{node_api_port}",
                ).replace(
                    f"server {replica} {replica}:50051 check port 8000",
                    f"server {replica} 127.0.0.1:{node_rpc_port} check port {node_api_port}",
                )
            volume = client.volumes.create(prefix + "-" + replica, labels=labels)
            volumes.append(volume)
            node = client.containers.run(
                image,
                command=["strata-coordinator"],
                name=prefix + "-" + replica,
                hostname=replica,
                detach=True,
                environment=environment
                | {
                    "STRATA_REPLICA_ID": replica,
                    "STRATA_API_BIND": "127.0.0.1" if host_network else "0.0.0.0",
                    "STRATA_API_PORT": str(node_api_port),
                    "STRATA_RPC_BIND": f"{bind_host}:{node_rpc_port}",
                },
                labels=labels,
                volumes={
                    volume.name: {"bind": "/app/data/artifacts", "mode": "rw"},
                    str(cert): {"bind": "/run/strata/rpc.pem", "mode": "ro"},
                    str(key): {"bind": "/run/strata/rpc.key", "mode": "ro"},
                },
                extra_hosts={"host.docker.internal": "host-gateway"},
                network="host" if host_network else network.name,
                mem_limit=512 * 1024**2,
                nano_cpus=500000000,
                cap_drop=["ALL"],
                security_opt=["no-new-privileges"],
            )
            if not host_network:
                network.disconnect(node)
                network.connect(node, aliases=[replica])
            nodes.append(node)
            containers.append(node)
        if host_network:
            config_text = config_text.replace("bind :8080", f"bind 127.0.0.1:{api_port}").replace(
                "bind :50051", f"bind 127.0.0.1:{rpc_port}"
            )
        config = tmp_path / "haproxy.cfg"
        config.write_text(config_text)
        gateway_image = client.images.get(
            os.getenv("STRATA_TEST_HA_GATEWAY_IMAGE", "strata/ha-gateway:local")
        )  # Preloaded reviewed image, pinned to its immutable ID before execution.
        gateway = client.containers.run(
            gateway_image.id,
            name=prefix + "-gateway",
            detach=True,
            labels=labels,
            network="host" if host_network else network.name,
            ports={}
            if host_network
            else {"8080/tcp": ("127.0.0.1", None), "50051/tcp": ("127.0.0.1", None)},
            volumes={str(config): {"bind": "/usr/local/etc/haproxy/haproxy.cfg", "mode": "ro"}},
            mem_limit=128 * 1024**2,
            nano_cpus=250000000,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
        )
        containers.append(gateway)
        if not host_network:
            network.disconnect(gateway)
            network.connect(gateway, aliases=["tls-rpc"])
        gateway.reload()
        ports = gateway.attrs["NetworkSettings"]["Ports"]
        api = "http://127.0.0.1:" + str(
            api_port if host_network else ports["8080/tcp"][0]["HostPort"]
        )
        rpc_target = "127.0.0.1:" + str(
            rpc_port if host_network else ports["50051/tcp"][0]["HostPort"]
        )
        http = httpx.Client(base_url=api, headers={"Authorization": f"Bearer {api_key}"}, timeout=5)

        def diagnostics():
            return "\n".join(
                item.name + "\n" + item.logs(tail=50).decode(errors="replace")
                for item in containers
            )

        wait_until(lambda: http.get("/ready").status_code == 200, diagnostics)
        yield svc, client, network, nodes, http, rpc_target, workload, diagnostics
    finally:
        with svc.factory() as session:
            attempts = list(session.scalars(select(Attempt)))
        for container in reversed(containers):
            with suppress(docker.errors.NotFound):
                container.reload()
                assert container.labels.get("strata.test") == prefix
                container.remove(force=True)
        for attempt in attempts:
            with suppress(docker.errors.NotFound):
                item = client.containers.get("strata-" + attempt.id)
                assert item.labels.get("strata.cluster") == admission_id
                item.remove(force=True)
            for name in ("strata-output-", "strata-input-"):
                with suppress(docker.errors.NotFound):
                    volume = client.volumes.get(name + attempt.id)
                    assert volume.attrs["Labels"]["strata.cluster"] == admission_id
                    volume.remove()
        for volume in volumes:
            assert volume.attrs["Labels"]["strata.test"] == prefix
            volume.remove()
        network.remove()
        client.close()
        for page in store.client.get_paginator("list_objects_v2").paginate(
            Bucket=svc.settings.s3_bucket
        ):
            for row in page.get("Contents", []):
                store.client.delete_object(Bucket=svc.settings.s3_bucket, Key=row["Key"])
        store.client.delete_bucket(Bucket=svc.settings.s3_bucket)


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_replica_crash_preserves_running_attempt_shared_artifacts_and_idempotency(replicas, kind):
    svc, client, network, nodes, http, target, image, diagnostics = replicas
    ready_replicas = set()

    def both_ready():
        response = http.get("/ready", headers={"Connection": "close"})
        if response.status_code == 200:
            ready_replicas.add(response.headers["X-Strata-Replica"])
        return ready_replicas == {"node-a", "node-b"}

    wait_until(both_ready, diagnostics)
    worker_id = "replica-worker-" + uuid4().hex
    transport, agent, thread, cpp = None, None, None, None
    if kind == "python":
        transport = Transport(target, svc.settings.worker_token)
        agent = Agent(
            transport, DockerExecutor([image], client=client), worker_id, 1, 512, svc.settings
        )
        thread = threading.Thread(target=agent.run)
        thread.start()
    else:
        # The worker is in the same private network; no host port or arbitrary input endpoint.
        cpp = client.containers.run(
            os.getenv("STRATA_TEST_CPP_IMAGE", "strata/worker-cpp:local"),
            name=worker_id,
            detach=True,
            labels={"strata.purpose": "replica-integration-test", "strata.worker-test": worker_id},
            network="host" if platform.system() == "Linux" else network.name,
            environment={
                "STRATA_RPC_TARGET": target if platform.system() == "Linux" else "tls-rpc:50051",
                "STRATA_WORKER_ID": worker_id,
                "STRATA_WORKER_TOKEN": svc.settings.worker_token,
                "STRATA_ALLOWED_IMAGES": json.dumps([image]),
                "STRATA_WORKER_CPU": "1",
                "STRATA_STORAGE_KEEPER_IMAGE": svc.settings.storage_keeper_image,
                "STRATA_WORKER_MEMORY_MB": "512",
                "STRATA_WORKER_CACHE_BYTES": "1048576",
                "STRATA_RPC_CA": "/run/strata/rpc-ca.pem",
            },
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                os.environ["STRATA_RPC_CA"]: {"bind": "/run/strata/rpc-ca.pem", "mode": "ro"},
            },
            user="root",
            mem_limit=256 * 1024**2,
            nano_cpus=250000000,
        )
    try:
        node_diagnostics = diagnostics

        def diagnostics():
            details = node_diagnostics()
            if cpp is not None:
                details += "\n" + cpp.logs(tail=60).decode(errors="replace")
            return details

        def registered():
            with svc.factory() as session:
                return session.get(Worker, worker_id) is not None

        wait_until(registered, diagnostics)
        body = {
            "name": "Replica loss during actual execution",
            "image": image,
            "command": [
                "python",
                "-u",
                "-c",
                "import time; from pathlib import Path; "
                "print('running across coordinator loss', flush=True); time.sleep(12); "
                "Path('/output/result.txt').write_text('replica recovery verified')",
            ],
            "resources": {"cpu": 0.25, "memory_mb": 128},
            "max_retries": 0,
            "timeout_seconds": 60,
        }
        key = "replica-loss-" + uuid4().hex
        response = http.post("/jobs", json=body, headers={"Idempotency-Key": key})
        assert response.status_code == 201, response.text
        job_id = response.json()["id"]
        wait_until(lambda: svc.get_job(job_id).status == "RUNNING", diagnostics)
        crash_started = time.monotonic()
        nodes[0].kill()  # Abrupt process/container loss, no graceful lease cleanup.

        def survivor_ready():
            response = http.get("/ready")
            return response.status_code == 200 and response.headers["X-Strata-Replica"] == "node-b"

        wait_until(survivor_ready, diagnostics, seconds=12)
        failover_seconds = time.monotonic() - crash_started
        # Client retries preserve the same key across a backend's ambiguous disconnect.
        with Client(url=str(http.base_url), api_key=next(iter(svc.settings.api_keys))) as sdk:
            repeated = sdk.submit(body, key)
        assert repeated["id"] == job_id

        def finished():
            state = svc.get_job(job_id).status
            assert state not in {"FAILED", "TIMED_OUT", "CANCELLED"}, diagnostics()
            return state == "SUCCEEDED"

        wait_until(finished, diagnostics)
        with svc.factory() as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 1
            worker = session.get(Worker, worker_id)
            assert worker.running_jobs == 0 and worker.cpu_reserved == 0
        artifacts = http.get(f"/jobs/{job_id}/artifacts").json()
        assert len(artifacts) == 1 and artifacts[0]["name"] == "result.txt"
        expected = b"replica recovery verified"
        assert artifacts[0]["sha256"] == hashlib.sha256(expected).hexdigest()
        assert http.get("/artifacts/" + artifacts[0]["id"]).content == expected
        # Restart the same replica with its own empty S3 cache and read the same output.
        nodes[0].start()
        observed = set()

        def both_read_output():
            reply = http.get("/artifacts/" + artifacts[0]["id"])
            if reply.status_code == 200:
                assert reply.content == expected
                observed.add(reply.headers["X-Strata-Replica"])
            return observed == {"node-a", "node-b"}

        wait_until(both_read_output, diagnostics)
        assert failover_seconds < 12
        output = os.getenv("STRATA_TEST_HA_EVIDENCE")
        if output:
            Path(output).mkdir(parents=True, exist_ok=True)
            (Path(output) / f"coordinator-{kind}-failover.json").write_text(
                json.dumps(
                    {
                        "passed": True,
                        "physical_hosts": 1,
                        "replicas": 2,
                        "verified_rpc_tls": True,
                        "shared_s3": True,
                        "attempt_count": 1,
                        "worker": kind,
                        "api_recovery_seconds": failover_seconds,
                    },
                    indent=2,
                )
                + "\n"
            )
    finally:
        if agent is not None:
            agent.stopped = True
            thread.join(timeout=12)
            assert not thread.is_alive()
            transport.channel.close()
        if cpp is not None:
            cpp.reload()
            assert cpp.labels.get("strata.worker-test") == worker_id
            cpp.remove(force=True)
        http.close()
