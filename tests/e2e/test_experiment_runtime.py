"""Actual Docker execution, input cache reuse and pinned replay on both agents."""

import json
import os
import platform
import socket
import threading
import time
from uuid import uuid4

import docker
import pytest

from control_plane.campaigns import CampaignService
from control_plane.datasets import DatasetService
from control_plane.experiments import ExperimentService, Replay, RunSubmit
from control_plane.rpc.server import make_server
from control_plane.schemas import JobSubmit, NamedResource, WorkflowSubmit
from control_plane.workflows import WorkflowService
from scheduler.core import Scheduler
from worker.agent import Agent
from worker.executor import DockerExecutor
from worker.transport import Transport

pytestmark = pytest.mark.docker


@pytest.mark.parametrize("kind", ["python", "cpp"])
def test_actual_worker_experiment_provenance_cache_and_replay(service, tmp_path, kind):
    if os.getenv("STRATA_TEST_DOCKER_RUNTIME") != "1":
        pytest.skip("set STRATA_TEST_DOCKER_RUNTIME=1 for actual Docker worker execution")
    cpp_image = os.getenv("STRATA_TEST_CPP_IMAGE", "strata/worker-cpp:local")
    image = os.getenv("STRATA_TEST_WORKLOAD_IMAGE", "strata/python-workloads:local")
    service.settings.allowed_images = [image]
    client = docker.from_env(timeout=3)
    service.settings.worker_cache_root = tmp_path / "worker-cache"
    datasets = DatasetService(service)
    dataset = datasets.create(NamedResource(name="Runtime measurements"))
    version = datasets.version(dataset.id, "v1")
    file = datasets.upload(version.id, "data.csv", [b"x,y\n1,2\n3,4\n"])
    datasets.seal(version.id)
    registry = ExperimentService(service)
    experiment = registry.create(NamedResource(name="Actual worker study"))
    run, _ = registry.run(
        experiment.id,
        RunSubmit(
            job={
                "name": "Profile measurements",
                "image": image,
                "command": ["python", "main.py", "profile", "--input", "/inputs/samples/data.csv"],
                "inputs": [{"version_id": version.id, "alias": "samples"}],
                "resources": {"cpu": 1, "memory_mb": 128},
                "max_retries": 0,
            }
        ),
        "initial",
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = make_server(service, f"127.0.0.1:{port}")
    server.start()
    worker_id = f"product-{kind}-{uuid4().hex}"
    thread = agent = transport = container = None
    try:
        if kind == "python":
            transport = Transport(f"127.0.0.1:{port}", service.settings.worker_token)
            agent = Agent(
                transport,
                DockerExecutor(service.settings.allowed_images, client=client),
                worker_id,
                2,
                1024,
                service.settings,
            )
            thread = threading.Thread(target=agent.run, daemon=True)
            thread.start()
        else:
            container = client.containers.run(
                cpp_image,
                detach=True,
                environment={
                    "STRATA_RPC_TARGET": f"127.0.0.1:{port}"
                    if platform.system() == "Linux"
                    else f"host.docker.internal:{port}",
                    "STRATA_WORKER_ID": worker_id,
                    "STRATA_WORKER_TOKEN": service.settings.worker_token,
                    "STRATA_WORKER_CPU": "2",
                    "STRATA_WORKER_MEMORY_MB": "1024",
                    "STRATA_ALLOWED_IMAGES": json.dumps(service.settings.allowed_images),
                    "STRATA_WORKER_CACHE_ROOT": "/var/lib/strata/input-cache",
                    "STRATA_STORAGE_KEEPER_IMAGE": service.settings.storage_keeper_image,
                },
                volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                labels={"strata.purpose": "product-runtime-test"},
                network_mode="host" if platform.system() == "Linux" else "default",
                nano_cpus=1000000000,
                mem_limit="256m",
            )
        scheduler = Scheduler(service)

        def wait(run_id):
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                scheduler.tick()
                info = registry.get(run_id)
                if info["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}:
                    assert info["status"] == "SUCCEEDED", info
                    return info
                time.sleep(0.2)
            logs = (
                container.logs().decode(errors="replace") if container else "native Python worker"
            )
            pytest.fail(f"worker did not finish: {registry.get(run_id)}; {logs}")

        original = wait(run.id)
        live_run, _ = registry.run(
            experiment.id,
            RunSubmit(
                job={
                    "name": "Live output evidence",
                    "image": image,
                    "command": [
                        "python",
                        "-u",
                        "-c",
                        "import os,time; print('visible while running',flush=True); "
                        "os.write(1,b'invalid:\\xff\\n'); time.sleep(4); "
                        "print('finished',flush=True)",
                    ],
                    "resources": {"cpu": 1, "memory_mb": 128},
                    "max_retries": 0,
                }
            ),
            "live-output",
        )
        seen_live = False
        deadline = time.monotonic() + 30
        from sqlalchemy import select

        from control_plane.models import Attempt

        while time.monotonic() < deadline:
            scheduler.tick()
            info = registry.get(live_run.id)
            with service.factory() as session:
                log_text = (
                    session.scalar(select(Attempt.logs).where(Attempt.job_id == info["job_id"]))
                    or ""
                )
            if info["status"] == "RUNNING" and "visible while running" in log_text:
                seen_live = True
            if info["status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}:
                assert info["status"] == "SUCCEEDED" and "finished" in log_text
                assert "invalid:" in log_text and "\ufffd" in log_text
                break
            time.sleep(0.2)
        else:
            pytest.fail("live-output workload did not finish")
        assert seen_live, f"{kind} only published output after completion"
        assert original["attempts"][0]["provenance"]["inputs"][0]["sha256"] == file.sha256
        image_digest = original["attempts"][0]["provenance"]["image_digest"]
        assert image_digest.startswith("sha256:") and len(image_digest) == 71
        replay, _ = registry.replay(run.id, Replay(), "replay")
        repeated = wait(replay.id)
        assert repeated["specification"]["expected_image_digest"] == image_digest
        assert repeated["attempts"][0]["provenance"]["image_digest"] == image_digest
        if agent:
            assert (agent.cache.store.root / file.sha256).read_bytes() == b"x,y\n1,2\n3,4\n"
        else:
            assert container.exec_run(
                ["find", "/var/lib/strata/input-cache", "-name", file.sha256]
            ).output.strip()
        campaigns = CampaignService(service)
        job = {
            "name": "Adaptive compute",
            "image": image,
            "resources": {"cpu": 1, "memory_mb": 128},
            "max_retries": 0,
        }
        campaign, _ = campaigns.workflow(
            WorkflowSubmit(
                name="Actual dynamic runtime",
                nodes={
                    "generate": JobSubmit(
                        **job,
                        command=[
                            "python",
                            "-c",
                            "from pathlib import Path; "
                            "Path('/output/parameters.json').write_text('[{\"seed\":41},{\"seed\":42}]')",
                        ],
                    ),
                    "analyze": JobSubmit(
                        **job,
                        command=[
                            "python",
                            "-c",
                            "import glob,json; from pathlib import Path; "
                            "files=glob.glob('/inputs/results-*/*.json'); assert len(files)==2; "
                            "Path('/output/result.json').write_text(json.dumps({'results':len(files)}))",
                        ],
                        artifact_inputs=[
                            {"job_id": "experiments", "name": "result.json", "alias": "results"}
                        ],
                    ),
                },
                expansions={
                    "experiments": {
                        "source": "generate",
                        "artifact": "parameters.json",
                        "max_jobs": 2,
                        "template": job
                        | {
                            "command": [
                                "python",
                                "main.py",
                                "monte-carlo",
                                "--samples",
                                "1000",
                                "--seed",
                                "${seed}",
                            ]
                        },
                    }
                },
            ),
            "runtime-fanout",
        )
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            scheduler.tick()
            WorkflowService(service).tick()
            status = campaigns.get(campaign.id)
            if status["status"] == "COMPLETED":
                assert status["counts"] == {"SUCCEEDED": 5}, campaigns.results(campaign.id)
                assert any(row.get("result.results") == 2 for row in campaigns.results(campaign.id))
                break
            time.sleep(0.2)
        else:
            pytest.fail(f"dynamic workflow did not finish: {campaigns.results(campaign.id)}")
    finally:
        if agent:
            agent.stopped = True
        if thread:
            thread.join(timeout=15)
        if transport:
            transport.channel.close()
        if container:
            container.stop(timeout=10)
            container.remove(force=True)
        # Only this randomly named test worker's containers and volumes are touched.
        DockerExecutor(service.settings.allowed_images, client=client).cleanup_orphans(worker_id)
        server.stop(0).wait()
        client.close()
