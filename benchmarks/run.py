"""Measure a live deployment; every reported number comes from this run."""

import argparse
import concurrent.futures
import json
import math
import os
import platform
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def percentile(values, fraction):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(fraction * len(values)) - 1)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default=os.getenv("STRATA_API_URL", "http://localhost:8000"))
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--samples", type=int, default=100000)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--implementation", choices=["python", "cpp", "mixed"], default="mixed")
    parser.add_argument(
        "--worker-id", help="Target one registered node for equal-budget comparisons"
    )
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/latest.json"))
    args = parser.parse_args()
    if args.jobs < 1 or args.concurrency < 1:
        parser.error("jobs and concurrency must be positive")
    started = time.perf_counter()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    with httpx.Client(base_url=args.api, timeout=30) as client:
        workers_response = client.get("/workers")
        workers_response.raise_for_status()
        workers = workers_response.json()
        required = {"python"}
        if args.implementation != "mixed":
            required.add(f"worker-{args.implementation}")
        if args.worker_id:
            required.add(f"node:{args.worker_id}")
        eligible = [
            w for w in workers if w["status"] == "HEALTHY" and required <= set(w["capabilities"])
        ]
        if not eligible:
            raise RuntimeError("no healthy worker matches the benchmark capabilities")

        def submit(index):
            before = time.perf_counter()
            capabilities = sorted(required)
            response = client.post(
                "/jobs",
                json={
                    "name": f"benchmark-{run_id}-{index}",
                    "image": "strata/python-workloads:local",
                    "command": [
                        "python",
                        "/app/main.py",
                        "monte-carlo",
                        "--samples",
                        str(args.samples),
                    ],
                    "resources": {"cpu": 1, "memory_mb": 128},
                    "capabilities": capabilities,
                    "max_retries": args.max_retries,
                    "timeout_seconds": 120,
                },
                headers={"Idempotency-Key": f"benchmark-{run_id}-{index}"},
            )
            response.raise_for_status()
            return response.json()["id"], time.perf_counter() - before

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            submitted = list(pool.map(submit, range(args.jobs)))
        pending = {job_id for job_id, _ in submitted}
        jobs = []
        deadline = time.monotonic() + args.timeout
        while pending:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"{len(pending)} jobs did not finish before the benchmark deadline"
                )
            for job_id in list(pending):
                response = client.get(f"/jobs/{job_id}")
                response.raise_for_status()
                job = response.json()
                if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}:
                    jobs.append(job)
                    pending.remove(job_id)
            if pending:
                time.sleep(0.2)
        elapsed = time.perf_counter() - started
        workers_used = set()
        for job in jobs:
            attempts = client.get(f"/jobs/{job['id']}/attempts")
            attempts.raise_for_status()
            workers_used.update(a["worker_id"] for a in attempts.json())
        scheduling = [
            (
                datetime.fromisoformat(j["scheduled_at"]) - datetime.fromisoformat(j["created_at"])
            ).total_seconds()
            for j in jobs
            if j["scheduled_at"]
        ]
        latencies = [latency for _, latency in submitted]
        try:
            docker_info = json.loads(
                subprocess.check_output(["docker", "info", "--format", "{{json .}}"], text=True)
            )
            daemon = {
                k: docker_info.get(k)
                for k in ["NCPU", "MemTotal", "OperatingSystem", "ServerVersion"]
            }
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
            daemon = None
        result = {
            "measured_at": datetime.now(UTC).isoformat(),
            "kind": "live-container-deployment",
            "client_environment": {
                "os": platform.platform(),
                "python": platform.python_version(),
                "cpu": platform.processor(),
                "logical_cpus": os.cpu_count(),
            },
            "local_docker_daemon": daemon,
            "workers": workers,
            "eligible_workers": [w["id"] for w in eligible],
            "workers_used": sorted(workers_used),
            "parameters": vars(args) | {"output": str(args.output)},
            "jobs": len(jobs),
            "succeeded": sum(j["status"] == "SUCCEEDED" for j in jobs),
            "attempts_total": sum(j["attempts_count"] for j in jobs),
            "jobs_retried": sum(j["attempts_count"] > 1 for j in jobs),
            "elapsed_seconds": elapsed,
            "successful_jobs_per_second": sum(j["status"] == "SUCCEEDED" for j in jobs) / elapsed,
            "submit_latency_seconds": {
                f"p{p}": percentile(latencies, p / 100) for p in [50, 95, 99]
            },
            "scheduling_latency_seconds": {
                f"p{p}": percentile(scheduling, p / 100) for p in [50, 95, 99]
            },
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
        if result["succeeded"] != args.jobs:
            raise SystemExit("benchmark contains failed jobs; retain report and investigate")


if __name__ == "__main__":
    main()
