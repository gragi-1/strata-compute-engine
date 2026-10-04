"""Private real PostgreSQL/Patroni quorum loss and primary crash, with verified TLS."""

import json
import os
import ssl
import subprocess
import threading
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import docker
import httpx
import psycopg
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.engine import URL

from control_plane.config import Settings
from control_plane.coordinator import Coordinator, ReplicaSettings
from control_plane.models import Attempt, Job, Worker
from control_plane.upgrade import upgrade
from scripts.ha_lab_credentials import generate
from strata_sdk import Client
from tests.e2e.test_coordinator_replicas import free_port, wait_until
from tests.integration.test_tls import certificates
from worker.agent import Agent
from worker.executor import DockerExecutor
from worker.transport import Transport

pytestmark = pytest.mark.docker


@pytest.fixture
def database_cluster(tmp_path):
    if os.getenv("STRATA_TEST_POSTGRES_HA") != "1":
        pytest.skip("set STRATA_TEST_POSTGRES_HA=1 for the real private quorum failure drill")
    client = docker.from_env(timeout=10)
    prefix = "strata-pg-ha-" + uuid4().hex
    root = Path(__file__).resolve().parents[2]
    credentials = tmp_path / "credentials"
    generate(credentials)
    image = client.images.get(os.getenv("STRATA_TEST_PG_HA_IMAGE", "strata/postgres-ha:local"))
    environment = os.environ | {
        "STRATA_HA_CREDENTIAL_ROOT": str(credentials.resolve()),
        "STRATA_POSTGRES_HA_IMAGE": image.id,
        "STRATA_HA_GATEWAY_IMAGE": client.images.get(
            os.getenv("STRATA_TEST_HA_GATEWAY_IMAGE", "strata/ha-gateway:local")
        ).id,
        "STRATA_PG_WRITER_PORT": str(free_port()),
    }
    config = json.loads(
        subprocess.check_output(
            [
                os.getenv("STRATA_TEST_DOCKER_CLI", "docker"),
                "compose",
                "-p",
                prefix,
                "-f",
                str(root / "deploy/ha/postgres.compose.yml"),
                "config",
                "--format",
                "json",
            ],
            env=environment,
        )
    )
    labels = {"strata.purpose": "postgres-ha-integration-test", "strata.test": prefix}
    network = client.networks.create(prefix, labels=labels)
    volumes, nodes = {}, {}
    secret_values = json.loads((credentials / "secrets.json").read_text())
    ca = credentials / "tls/gateway/ca.pem"
    tls = ssl.create_default_context(cafile=str(ca))
    tls.load_cert_chain(
        str(credentials / "tls/pg-a/client.pem"), str(credentials / "tls/pg-a/client.key")
    )
    health = httpx.Client(verify=tls, timeout=3, trust_env=False)
    addresses, writer_url = {}, None

    def diagnostics():
        return "\n".join(
            name + "\n" + node.logs(tail=60).decode(errors="replace")
            for name, node in nodes.items()
        )

    try:
        for name in config["volumes"]:
            volumes[name] = client.volumes.create(prefix + "-" + name, labels=labels)
        # Preserve the shipped template settings; only add private ephemeral observation ports.
        for name, options in config["services"].items():
            mounts = {}
            for mount in options["volumes"]:
                source = (
                    volumes[mount["source"]].name if mount["type"] == "volume" else mount["source"]
                )
                mounts[source] = {
                    "bind": mount["target"],
                    "mode": "ro" if mount.get("read_only") else "rw",
                }
            ports = {}
            if name.startswith("pg-"):
                ports = {"8008/tcp": ("127.0.0.1", None), "5432/tcp": ("127.0.0.1", None)}
            elif name == "postgres-writer":
                ports = {"5432/tcp": ("127.0.0.1", None)}
            else:
                ports = {"2379/tcp": ("127.0.0.1", None)}
            node = client.containers.run(
                options["image"],
                name=prefix + "-" + name,
                hostname=name,
                detach=True,
                environment=options.get("environment", {}),
                volumes=mounts,
                labels=labels,
                network=network.name,
                ports=ports,
                read_only=options.get("read_only", False),
                cap_drop=options.get("cap_drop", []),
                security_opt=options.get("security_opt", []),
                mem_limit=options.get("mem_limit"),
                nano_cpus=int(float(options["cpus"]) * 1e9),
                pids_limit=options["pids_limit"],
                tmpfs=dict(item.split(":", 1) for item in options.get("tmpfs", [])),
            )
            nodes[name] = node
            network.disconnect(node)
            network.connect(node, aliases=[name])
            node.reload()
            addresses[name] = {
                int(port.split("/")[0]): int(mapping[0]["HostPort"])
                for port, mapping in node.attrs["NetworkSettings"]["Ports"].items()
                if mapping
            }
        writer_url = URL.create(
            "postgresql+psycopg",
            username="strata",
            password=secret_values["application"],
            host="127.0.0.1",
            port=addresses["postgres-writer"][5432],
            database="strata",
            query={
                "sslmode": "verify-full",
                "sslrootcert": str(ca.resolve()),
                "connect_timeout": "2",
                "options": "-cstatement_timeout=3000",
            },
        )

        def cluster_ready():
            results = [
                health.get(f"https://127.0.0.1:{addresses[name][8008]}/patroni")
                for name in ("pg-a", "pg-b", "pg-c")
            ]
            return (
                all(
                    reply.status_code == 200 and reply.json()["state"] == "running"
                    for reply in results
                )
                and sum(reply.json()["role"] in {"primary", "master"} for reply in results) == 1
            )

        wait_until(cluster_ready, diagnostics, seconds=120)

        def writer_ready():
            try:
                with psycopg.connect(
                    writer_url.render_as_string(hide_password=False).replace(
                        "postgresql+psycopg://", "postgresql://"
                    )
                ) as conn:
                    return conn.execute("SELECT 1").fetchone() == (1,)
            except psycopg.OperationalError:
                return False

        wait_until(writer_ready, diagnostics)
        yield nodes, addresses, writer_url, health, diagnostics
    finally:
        health.close()
        for node in reversed(list(nodes.values())):
            with suppress(docker.errors.NotFound):
                node.reload()
                assert node.labels.get("strata.test") == prefix
                node.remove(force=True)
        for volume in volumes.values():
            assert volume.attrs["Labels"].get("strata.test") == prefix
            volume.remove()
        network.remove()
        client.close()


def test_primary_and_quorum_loss_preserve_jobs_and_fence_writes(
    database_cluster, tmp_path, monkeypatch
):
    nodes, addresses, url, health, diagnostics = database_cluster
    cert, key = certificates(tmp_path)
    settings = Settings(
        database_url=url.render_as_string(hide_password=False),
        artifact_root=tmp_path / "artifacts",
        allowed_images=[os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")],
        tls_cert=cert,
        tls_key=key,
        heartbeat_interval=1,
        worker_timeout=60,
        lease_seconds=90,
        worker_token="synthetic-database-failover-worker-token-32",
        api_keys={"synthetic-database-failover-api-key-32": "admin"},
    )
    monkeypatch.setenv("STRATA_DATABASE_URL", settings.database_url)
    upgrade()
    upgrade(check=True)
    node = Coordinator(
        settings, ReplicaSettings(api_port=free_port(), rpc_bind=f"127.0.0.1:{free_port()}")
    )
    thread = threading.Thread(target=node.run)
    thread.start()
    monkeypatch.setenv("STRATA_RPC_CA", str(cert))
    transport = Transport(node.replica.rpc_bind, settings.worker_token)
    executor = DockerExecutor(settings.allowed_images)
    agent = Agent(transport, executor, "ha-worker-" + uuid4().hex, 1, 512, settings)
    worker_thread = threading.Thread(target=agent.run)
    sdk = Client(
        url=f"http://127.0.0.1:{node.replica.api_port}",
        api_key=next(iter(settings.api_keys)),
    )
    timings = {}

    def primary():
        names = []
        for name in ("pg-a", "pg-b", "pg-c"):
            if nodes[name].status != "running":
                continue
            reply = health.get(f"https://127.0.0.1:{addresses[name][8008]}/primary")
            if reply.status_code == 200:
                names.append(name)
        assert len(names) <= 1, "more than one writable primary"
        return names[0] if names else None

    def restart(name):
        nodes[name].start()
        nodes[name].reload()
        # Docker can allocate another ephemeral host port when a stopped container restarts.
        addresses[name] = {
            int(port.split("/")[0]): int(mapping[0]["HostPort"])
            for port, mapping in nodes[name].attrs["NetworkSettings"]["Ports"].items()
            if mapping
        }

    try:
        wait_until(lambda: node.ready(), diagnostics)
        # No plaintext database session, no invalid CA, and no superuser application role.
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(
                host="127.0.0.1",
                port=addresses["postgres-writer"][5432],
                user="strata",
                password=url.password,
                dbname="strata",
                sslmode="disable",
                connect_timeout=2,
            )
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(
                url.update_query_dict({"sslrootcert": str(cert.resolve())})
                .render_as_string(hide_password=False)
                .replace("postgresql+psycopg://", "postgresql://")
            )
        ca_only = ssl.create_default_context(cafile=url.query["sslrootcert"])
        with httpx.Client(verify=ca_only, timeout=3, trust_env=False) as unauthorized:
            for name, port, path in (("etcd-a", 2379, "/version"), ("pg-a", 8008, "/patroni")):
                with pytest.raises(httpx.TransportError):
                    unauthorized.get(f"https://127.0.0.1:{addresses[name][port]}{path}")
        with node.svc.factory() as session:
            role = session.execute(
                text(
                    "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication "
                    "FROM pg_roles WHERE rolname=current_user"
                )
            ).one()
            assert role == (False, False, False, False)
            assert session.scalar(text("SHOW synchronous_commit")) == "on"
            assert session.scalar(text("SHOW synchronous_standby_names"))
        worker_thread.start()
        wait_until(lambda: bool(sdk.request("GET", "/workers")), diagnostics)
        body = {
            "name": "Database failover during real container execution",
            "image": settings.allowed_images[0],
            "command": [
                "python",
                "-u",
                "-c",
                "import time; from pathlib import Path; "
                "print('database failover workload', flush=True); time.sleep(40); "
                "Path('/output/result.txt').write_text('database failover verified')",
            ],
            "resources": {"cpu": 0.25, "memory_mb": 128},
            "max_retries": 0,
            "timeout_seconds": 100,
        }
        job = sdk.submit(body, "primary-loss-job")
        wait_until(lambda: node.svc.get_job(job["id"]).status == "RUNNING", diagnostics)
        original = primary()
        assert original
        started = time.monotonic()
        nodes[original].kill()
        nodes[original].reload()

        def writer_ready():
            winner = primary()
            if not winner:
                return False
            try:
                return sdk.http.get("/ready").status_code == 200
            except httpx.TransportError:
                return False

        wait_until(lambda: primary() != original and writer_ready(), diagnostics, seconds=45)
        timings["primary_recovery_seconds"] = time.monotonic() - started
        assert sdk.submit(body, "primary-loss-job")["id"] == job["id"]
        restart(original)
        wait_until(
            lambda: (
                health.get(f"https://127.0.0.1:{addresses[original][8008]}/replica").status_code
                == 200
            ),
            diagnostics,
        )

        def completed():
            state = node.svc.get_job(job["id"]).status
            assert state not in {"FAILED", "CANCELLED", "TIMED_OUT"}, diagnostics()
            return state == "SUCCEEDED"

        wait_until(completed, diagnostics)
        with node.svc.factory() as session:
            assert session.scalar(select(func.count()).select_from(Attempt)) == 1
            worker = session.get(Worker, agent.worker_id)
            assert worker.cpu_reserved == 0 and worker.running_jobs == 0
        artifacts = sdk.request("GET", "/jobs/" + job["id"] + "/artifacts")
        assert sdk.request_response("GET", "/artifacts/" + artifacts[0]["id"]).content == (
            b"database failover verified"
        )
        # Losing two DCS members removes quorum: the cluster must stop acknowledging writes.
        for name in ("etcd-a", "etcd-b"):
            nodes[name].kill()
            nodes[name].reload()
        started = time.monotonic()
        wait_until(lambda: primary() is None, diagnostics, seconds=45)
        timings["quorum_write_fence_seconds"] = time.monotonic() - started
        reply = sdk.http.post("/jobs", json=body, headers={"Idempotency-Key": "during-quorum-loss"})
        assert reply.status_code == 503
        for name in ("etcd-a", "etcd-b"):
            restart(name)
        wait_until(writer_ready, diagnostics, seconds=60)
        recovered = sdk.submit(
            body | {"name": "After quorum recovery", "command": ["true"]}, "after-quorum-recovery"
        )
        wait_until(lambda: node.svc.get_job(recovered["id"]).status == "SUCCEEDED", diagnostics)
        with node.svc.factory() as session:
            assert session.scalar(select(func.count()).select_from(Job)) == 2
        output = os.getenv("STRATA_TEST_HA_EVIDENCE")
        if output:
            Path(output).mkdir(parents=True, exist_ok=True)
            (Path(output) / "postgres-failover.json").write_text(
                json.dumps(
                    {
                        "passed": True,
                        "physical_hosts": 1,
                        "database_nodes": 3,
                        "dcs_nodes": 3,
                        "verified_tls": True,
                        "dcs_mtls": True,
                        "attempts_for_original_job": 1,
                        "primary_before": original,
                        "timings": timings,
                    },
                    indent=2,
                )
                + "\n"
            )
    finally:
        agent.stopped = True
        if worker_thread.ident is not None:
            worker_thread.join(timeout=12)
            assert not worker_thread.is_alive()
        transport.channel.close()
        executor.client.close()
        node.stopped.set()
        thread.join(timeout=15)
        assert not thread.is_alive()
        sdk.http.close()
