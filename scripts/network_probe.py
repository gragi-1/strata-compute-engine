"""Validate TLS and independent Docker daemons on one physical host."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx

from scripts.e2e_live import wait_job
from tests.integration.test_tls import certificates


def main():
    docker = (
        shutil.which("docker")
        or r"C:\Users\grage\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe"
    )
    nonce = uuid4().hex[:10]
    prefix = f"strata-probe-{nonce}"
    output = Path("build/network-validation") / nonce
    output.mkdir(parents=True)
    cert, key = certificates(output)
    containers, volumes = [], []
    control_image = os.getenv("STRATA_TEST_CONTROL_IMAGE", "strata/control-plane:local")
    cpp_image = os.getenv("STRATA_TEST_CPP_IMAGE", "strata/worker-cpp:local")
    keeper_image = os.getenv("STRATA_STORAGE_KEEPER_IMAGE", control_image)
    parent_network = os.getenv("STRATA_PROBE_NETWORK", "strata_default")
    artifact_volume = os.getenv("STRATA_PROBE_ARTIFACT_VOLUME", "strata_artifacts")
    database_url = os.getenv(
        "STRATA_DATABASE_URL", "postgresql+psycopg://strata:strata@postgres:5432/strata"
    )

    def command(*args, input_file=None):
        result = subprocess.run([docker, *args], stdin=input_file, capture_output=True, check=True)
        return result.stdout.decode().strip()

    token = os.getenv("STRATA_WORKER_TOKEN", "local-development-token")
    socket_volumes = []
    try:
        image_tar = output / "workload.tar"
        # Independent daemons need both the workload and v3's output-retention image.
        command(
            "image",
            "save",
            "--output",
            str(image_tar),
            "strata/python-workloads:local",
            keeper_image,
        )
        tls_volume = prefix + "-tls"
        command("volume", "create", "--label", "strata.validation=network", tls_volume)
        volumes.append(tls_volume)
        seed = command(
            "create",
            "--label",
            "strata.validation=network",
            "--volume",
            f"{tls_volume}:/run/tls",
            control_image,
            "true",
        )
        containers.append(seed)
        command("cp", str(cert), f"{seed}:/run/tls/certificate.pem")
        command("cp", str(key), f"{seed}:/run/tls/key.pem")
        rpc_name = prefix + "-rpc"
        containers.append(
            command(
                "run",
                "-d",
                "--name",
                rpc_name,
                "--network",
                parent_network,
                "--network-alias",
                f"tls-rpc-{nonce}",
                "--label",
                "strata.validation=network",
                "--env",
                f"STRATA_DATABASE_URL={database_url}",
                "--env",
                f"STRATA_WORKER_TOKEN={token}",
                "--env",
                "STRATA_ARTIFACT_ROOT=/app/data/artifacts",
                "--env",
                "STRATA_TLS_CERT=/run/tls/certificate.pem",
                "--env",
                "STRATA_TLS_KEY=/run/tls/key.pem",
                "--volume",
                f"{tls_volume}:/run/tls:ro",
                "--volume",
                f"{artifact_volume}:/app/data/artifacts",
                control_image,
                "strata-rpc",
            )
        )
        # Use the shared 'tls-rpc' SAN through an alias specific to this disposable network bridge.
        network = command("network", "create", prefix + "-network")
        command("network", "connect", "--alias", "tls-rpc", network, rpc_name)
        for kind in ["python", "cpp"]:
            daemon_name = prefix + "-daemon-" + kind
            socket_volume = prefix + "-socket-" + kind
            data_volume = prefix + "-data-" + kind
            for volume in [socket_volume, data_volume]:
                command("volume", "create", "--label", "strata.validation=network", volume)
                volumes.append(volume)
            socket_volumes.append(socket_volume)
            daemon = command(
                "run",
                "-d",
                "--privileged",
                "--name",
                daemon_name,
                "--network",
                network,
                "--label",
                "strata.validation=network",
                "--env",
                "DOCKER_TLS_CERTDIR=",
                "--volume",
                f"{socket_volume}:/var/run",
                "--volume",
                f"{data_volume}:/var/lib/docker",
                "docker:29-dind",
                "dockerd",
                "--host=unix:///var/run/docker.sock",
            )
            containers.append(daemon)
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    command("exec", daemon, "docker", "info")
                    break
                except subprocess.CalledProcessError:
                    time.sleep(1)
            else:
                raise TimeoutError("isolated Docker daemon did not start")
            with image_tar.open("rb") as stream:
                command("exec", "-i", daemon, "docker", "load", input_file=stream)
            worker_id = prefix + "-" + kind
            args = [
                "run",
                "-d",
                "--name",
                worker_id,
                "--network",
                network,
                "--label",
                "strata.validation=network",
                "--user",
                "root",
                "--env",
                "STRATA_RPC_TARGET=tls-rpc:50051",
                "--env",
                f"STRATA_WORKER_ID={worker_id}",
                "--env",
                f"STRATA_WORKER_TOKEN={token}",
                "--env",
                "STRATA_RPC_CA=/run/tls/certificate.pem",
                "--env",
                "STRATA_WORKER_CPU=1",
                "--env",
                "STRATA_WORKER_MEMORY_MB=512",
                "--env",
                f"STRATA_STORAGE_KEEPER_IMAGE={keeper_image}",
                "--env",
                "DOCKER_HOST=unix:///run/strata-docker/docker.sock",
                "--env",
                "STRATA_DOCKER_SOCKET=/run/strata-docker/docker.sock",
                "--volume",
                f"{socket_volume}:/run/strata-docker",
                "--volume",
                f"{tls_volume}:/run/tls:ro",
            ]
            args.extend([control_image, "strata-worker"] if kind == "python" else [cpp_image])
            containers.append(command(*args))
        with httpx.Client(
            base_url=os.getenv("STRATA_API_URL", "http://localhost:8000"),
            timeout=60,
            headers={"Authorization": f"Bearer {os.environ['STRATA_API_KEY']}"}
            if os.getenv("STRATA_API_KEY")
            else {},
        ) as client:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                live = {w["id"] for w in client.get("/workers").json() if w["status"] == "HEALTHY"}
                if all(prefix + "-" + kind in live for kind in ["python", "cpp"]):
                    break
                time.sleep(1)
            else:
                raise TimeoutError("TLS workers did not register")
            dataset = client.post(
                "/datasets", json={"name": "Network validation observations"}
            ).json()
            version = client.post(
                f"/datasets/{dataset['id']}/versions", json={"label": "v1"}
            ).json()
            client.put(
                f"/dataset-versions/{version['id']}/files/observations.csv",
                content=b"x,y\n1,2\n3,4\n5,6\n",
            ).raise_for_status()
            client.post(f"/dataset-versions/{version['id']}/seal").raise_for_status()
            results = {}
            for kind in ["python", "cpp"]:
                response = client.post(
                    "/jobs",
                    json={
                        "name": f"Independent daemon profile {kind}",
                        "image": "strata/python-workloads:local",
                        "command": [
                            "python",
                            "/app/main.py",
                            "profile",
                            "--input",
                            "/inputs/data/observations.csv",
                        ],
                        "capabilities": [f"node:{prefix}-{kind}"],
                        "inputs": [{"version_id": version["id"], "alias": "data"}],
                        # Leave capacity for the separately charged output keeper.
                        "resources": {"cpu": 0.5, "memory_mb": 256},
                        "max_retries": 0,
                        "timeout_seconds": 120,
                    },
                )
                response.raise_for_status()
                job = response.json()
                final = wait_job(client, job["id"], 180)
                assert final["status"] == "SUCCEEDED", final
                artifact = client.get(f"/jobs/{job['id']}/artifacts").json()[0]
                result = client.get(artifact["uri"]).json()
                assert result["rows"] == 3 and result["statistics"]["x"]["mean"] == 3, result
                results[kind] = {"job_id": job["id"], "result": result}
            evidence = {
                "environment": "one physical host, two independent Docker daemons",
                "rpc": "TLS verified by both worker implementations",
                "results": results,
            }
            (output / "evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
            print(json.dumps(evidence))
    except subprocess.CalledProcessError as exc:
        print(exc.stderr.decode(errors="replace"))
        raise
    finally:
        for container in reversed(containers):
            logs = subprocess.run([docker, "logs", container], capture_output=True)
            (output / f"container-{container[:12]}.log").write_bytes(logs.stdout + logs.stderr)
            subprocess.run([docker, "rm", "-f", container], capture_output=True)
        for volume in volumes:
            subprocess.run([docker, "volume", "rm", volume], capture_output=True)
        subprocess.run([docker, "network", "rm", prefix + "-network"], capture_output=True)
        image_tar = output / "workload.tar"
        if image_tar.is_file():
            image_tar.unlink()


if __name__ == "__main__":
    main()
