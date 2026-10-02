"""Exercise both real worker implementations against Docker Compose."""

import hashlib
import os
import time

import httpx


def wait_ready(client, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            ready = client.get("/ready")
            workers = client.get("/workers")
            if ready.status_code == 200 and workers.status_code == 200:
                healthy = {w["id"] for w in workers.json() if w["status"] == "HEALTHY"}
                if {"worker-1", "worker-2", "worker-cpp"} <= healthy:
                    return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise TimeoutError("deployment did not become ready with all three workers")


def wait_job(client, job_id, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/jobs/{job_id}")
        response.raise_for_status()
        job = response.json()
        if job["status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}:
            return job
        time.sleep(0.2)
    raise TimeoutError(f"job {job_id} did not finish")


def main():
    with httpx.Client(
        base_url=os.getenv("STRATA_API_URL", "http://localhost:8000"), timeout=10
    ) as client:
        wait_ready(client)
        for implementation in ["python", "cpp"]:
            response = client.post(
                "/jobs",
                json={
                    "name": f"e2e-{implementation}",
                    "image": "strata/python-workloads:local",
                    "command": ["python", "/app/main.py", "monte-carlo", "--samples", "100000"],
                    "capabilities": [f"worker-{implementation}"],
                    "resources": {"cpu": 1, "memory_mb": 128},
                    "max_retries": 0,
                    "timeout_seconds": 60,
                },
            )
            response.raise_for_status()
            job_id = response.json()["id"]
            job = wait_job(client, job_id)
            assert job["status"] == "SUCCEEDED", job
            rows = client.get(f"/jobs/{job_id}/artifacts").json()
            assert rows, f"{implementation} did not upload its output"
            for artifact in rows:
                data = client.get(artifact["uri"]).content
                assert hashlib.sha256(data).hexdigest() == artifact["sha256"]
            assert "pi" in client.get(f"/jobs/{job_id}/logs").text
            print(f"{implementation}: real container, logs and verified artifact succeeded")
        wave = client.post(
            "/jobs",
            json={
                "name": "e2e-wave",
                "image": "strata/wave-solver:local",
                "command": [
                    "/solver",
                    "--nx",
                    "256",
                    "--steps",
                    "500",
                    "--output",
                    "/output/wave.csv",
                ],
                "max_retries": 0,
                "timeout_seconds": 60,
            },
        ).json()
        assert wait_job(client, wave["id"])["status"] == "SUCCEEDED"
        print("C++ wave workload: real container succeeded")
        for implementation in ["python", "cpp"]:
            long_job = {
                "name": f"timeout-{implementation}",
                "image": "strata/python-workloads:local",
                "command": ["python", "/app/main.py", "monte-carlo", "--samples", "1000000000"],
                "capabilities": [f"worker-{implementation}"],
                "max_retries": 0,
                "timeout_seconds": 1,
            }
            response = client.post("/jobs", json=long_job)
            response.raise_for_status()
            timed = wait_job(client, response.json()["id"], timeout=30)
            assert timed["status"] == "TIMED_OUT", timed
            response = client.post(
                "/jobs",
                json=long_job | {"name": f"cancel-{implementation}", "timeout_seconds": 120},
            )
            response.raise_for_status()
            job_id = response.json()["id"]
            deadline = time.monotonic() + 30
            while client.get(f"/jobs/{job_id}").json()["status"] != "RUNNING":
                if time.monotonic() >= deadline:
                    raise TimeoutError("cancel test job did not start")
                time.sleep(0.1)
            client.post(f"/jobs/{job_id}/cancel").raise_for_status()
            cancelled = wait_job(client, job_id, timeout=30)
            assert cancelled["status"] == "CANCELLED", cancelled
            print(f"{implementation}: real timeout and running cancellation succeeded")


if __name__ == "__main__":
    main()
