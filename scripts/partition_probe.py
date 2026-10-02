"""Verify both local watchdogs when worker-control replies cannot arrive."""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

import httpx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--api", default=os.getenv("STRATA_API_URL", "http://localhost:8000"))
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=38,
        help="Allow the default 30s lease plus 5s TERM grace and transport margin",
    )
    parser.add_argument("--output", type=Path, default=Path("docs/demo/partition-probe.json"))
    args = parser.parse_args()
    jobs = []
    paused = False
    with httpx.Client(base_url=args.api, timeout=10) as client:
        try:
            for node in ["worker-1", "worker-cpp"]:
                response = client.post(
                    "/jobs",
                    json={
                        "name": f"partition-{node}",
                        "image": "strata/python-workloads:local",
                        "command": [
                            "python",
                            "/app/main.py",
                            "monte-carlo",
                            "--samples",
                            "1000000000",
                        ],
                        "capabilities": [f"node:{node}"],
                        "max_retries": 1,
                        "timeout_seconds": 120,
                    },
                )
                response.raise_for_status()
                jobs.append(response.json()["id"])
            deadline = time.monotonic() + 30
            while not all(client.get(f"/jobs/{j}").json()["status"] == "RUNNING" for j in jobs):
                if time.monotonic() >= deadline:
                    raise TimeoutError("partition workloads did not start")
                time.sleep(0.1)
            attempts = [client.get(f"/jobs/{j}/attempts").json()[-1] for j in jobs]
            subprocess.run(
                [args.docker, "compose", "pause", "rpc"], check=True, capture_output=True
            )
            paused = True
            started = time.monotonic()
            time.sleep(args.wait_seconds)
            evidence = []
            for job_id, attempt in zip(jobs, attempts, strict=True):
                inspected = subprocess.run(
                    [args.docker, "inspect", f"strata-{attempt['id']}"],
                    capture_output=True,
                    text=True,
                )
                inspected.check_returncode()
                state = json.loads(inspected.stdout)[0]["State"]
                running = state["Running"]
                job = client.get(f"/jobs/{job_id}").json()
                assert not running, f"{attempt['worker_id']} did not stop at local lease expiry"
                assert job["status"] == "RETRYING", job
                evidence.append(
                    {
                        "job_id": job_id,
                        "worker_id": attempt["worker_id"],
                        "container_running": running,
                        "container_state": state["Status"],
                        "job_status": job["status"],
                    }
                )
                client.post(f"/jobs/{job_id}/cancel").raise_for_status()
            result = {
                "kind": "live RPC reply blackhole",
                "elapsed_seconds": time.monotonic() - started,
                "watchdogs": evidence,
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2))
            print(json.dumps(result, indent=2))
        finally:
            if paused:
                subprocess.run(
                    [args.docker, "compose", "unpause", "rpc"], check=True, capture_output=True
                )
            for job_id in jobs:
                client.post(f"/jobs/{job_id}/cancel")


if __name__ == "__main__":
    main()
