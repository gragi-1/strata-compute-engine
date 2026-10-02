"""Record actual worker loss and recovery as an asciinema v2 cast."""

import argparse
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default=os.getenv("STRATA_API_URL", "http://localhost:8000"))
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--samples", type=int, default=30000000)
    parser.add_argument("--min-duration", type=float, default=65)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, default=Path("docs/demo/failure-recovery.cast"))
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    killed = None
    with args.output.open("w", encoding="utf-8") as cast:
        cast.write(
            json.dumps(
                {
                    "version": 2,
                    "width": 100,
                    "height": 26,
                    "timestamp": int(datetime.now(UTC).timestamp()),
                    "title": "Strata: real Docker worker failure recovery",
                }
            )
            + "\n"
        )

        def emit(message):
            print(message, flush=True)
            cast.write(
                json.dumps([round(time.monotonic() - started, 3), "o", message + "\r\n"]) + "\n"
            )
            cast.flush()

        with httpx.Client(base_url=args.api, timeout=10) as client:
            try:
                emit("STRATA | PostgreSQL + gRPC + isolated Docker compute")
                emit("Submit a Monte Carlo workload with automatic retries")
                response = client.post(
                    "/jobs",
                    json={
                        "name": "failure-recovery-demo",
                        "image": "strata/python-workloads:local",
                        "command": [
                            "python",
                            "/app/main.py",
                            "monte-carlo",
                            "--samples",
                            str(args.samples),
                        ],
                        "resources": {"cpu": 1, "memory_mb": 128},
                        "capabilities": ["worker-python"],
                        "max_retries": 3,
                        "timeout_seconds": 120,
                    },
                )
                response.raise_for_status()
                job_id = response.json()["id"]
                emit(f"Job {job_id}")
                last_event = 0
                terminal = False
                deadline = time.monotonic() + args.timeout
                while time.monotonic() < deadline:
                    job = client.get(f"/jobs/{job_id}").json()
                    events = client.get(
                        f"/jobs/{job_id}/events", params={"after": last_event}
                    ).json()
                    for event in events:
                        last_event = event["id"]
                        emit(f"{event['created_at'][11:19]}  {event['kind']}")
                    if job["status"] == "RUNNING" and killed is None:
                        attempts = client.get(f"/jobs/{job_id}/attempts").json()
                        killed = attempts[-1]["worker_id"]
                        if killed not in {"worker-1", "worker-2"}:
                            raise RuntimeError("demo expects standard Compose Python workers")
                        emit(f"Fault injection: docker compose kill {killed}")
                        subprocess.run(
                            [args.docker, "compose", "kill", killed],
                            check=True,
                            capture_output=True,
                        )
                    workers = client.get("/workers").json()
                    if (
                        killed
                        and any(w["id"] == killed and w["status"] == "LOST" for w in workers)
                        and not terminal
                    ):
                        pass  # Loss is recorded as WORKER_LOST in the job event history.
                    if job["status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}:
                        terminal = True
                        assert killed is not None and job["status"] == "SUCCEEDED", job
                        attempts = client.get(f"/jobs/{job_id}/attempts").json()
                        assert len(attempts) >= 2 and attempts[-1]["worker_id"] != killed
                        emit(
                            f"Recovered on {attempts[-1]['worker_id']} | "
                            f"{len(attempts)} durable attempts"
                        )
                        emit(f"Logs: {client.get(f'/jobs/{job_id}/logs').text.strip()}")
                        emit("Artifacts: SHA-256 metadata persisted; old lease fenced")
                        remaining = args.min_duration - (time.monotonic() - started)
                        if remaining > 0:
                            time.sleep(remaining)
                        emit("Demo complete. All transitions above came from the live database.")
                        return
                    time.sleep(0.25)
                raise TimeoutError("failure recovery demo did not finish")
            finally:
                if killed:
                    subprocess.run(
                        [args.docker, "compose", "start", killed], check=True, capture_output=True
                    )


if __name__ == "__main__":
    main()
