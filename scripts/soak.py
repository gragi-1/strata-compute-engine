"""Run bounded real workloads and retain reproducible load evidence for an isolated cluster."""

import argparse
import hashlib
import json
import math
import os
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from strata_sdk import Client

TERMINAL = {"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"}


def percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else 0


def soak(
    client: Client,
    image: str,
    output: Path,
    seconds: int,
    concurrency: int,
    kinds: list[str],
    drain_seconds: int,
    require_idle_workers: bool,
) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    run_id = uuid4().hex
    started = time.monotonic()
    stop_submission = started + seconds
    pending, completed, workers = {}, [], set()
    evidence = {
        "run_id": run_id,
        "started_at": datetime.now(UTC).isoformat(),
        "requested_seconds": seconds,
        "concurrency": concurrency,
        "image": image,
        "worker_kinds": kinds,
        "passed": False,
    }
    sequence = 0
    try:
        while time.monotonic() < stop_submission or pending:
            if time.monotonic() > stop_submission + drain_seconds:
                raise TimeoutError("soak workloads did not drain within the configured deadline")
            while time.monotonic() < stop_submission and len(pending) < concurrency:
                kind = kinds[sequence % len(kinds)]
                payload = f"strata-soak-{run_id}-{sequence}"
                digest = hashlib.sha256(payload.encode()).hexdigest()
                code = (
                    "import hashlib,json; "
                    f"payload={payload!r}; "
                    "data={'sha256':hashlib.sha256(payload.encode()).hexdigest()}; "
                    "open('/output/result.json','w').write(json.dumps(data)); "
                    "print(data['sha256'],flush=True)"
                )
                specification = {
                    "name": f"Soak workload {sequence}",
                    "image": image,
                    "command": ["python", "-c", code],
                    "resources": {"cpu": 0.1, "memory_mb": 64},
                    "capabilities": [] if kind == "any" else [f"worker-{kind}"],
                    "max_retries": 0,
                    "timeout_seconds": 60,
                }
                key = f"soak-{run_id}-{sequence}"
                before = time.monotonic()
                job = client.submit(specification, key)
                pending[job["id"]] = {
                    "submitted": before,
                    "digest": digest,
                    "kind": kind,
                    "sequence": sequence,
                }
                replay = client.submit(specification, key)
                if replay["id"] != job["id"]:
                    raise AssertionError("idempotent submission created a second job")
                sequence += 1
            for job_id, expected in list(pending.items()):
                row = client.request_object("GET", f"/jobs/{job_id}")
                if row["status"] not in TERMINAL:
                    continue
                if row["status"] != "SUCCEEDED":
                    raise AssertionError(f"workload {job_id} ended as {row['status']}")
                attempts = client.request("GET", f"/jobs/{job_id}/attempts")
                if len(attempts) != 1 or attempts[0]["status"] != "SUCCEEDED":
                    raise AssertionError("workload did not complete with exactly one attempt")
                destination = output / "artifacts" / job_id
                artifacts = client.artifacts(job_id, destination)
                if len(artifacts) != 1 or artifacts[0]["name"] != "result.json":
                    raise AssertionError("workload did not deliver its expected output")
                actual = json.loads((destination / "result.json").read_text(encoding="utf-8"))
                if actual != {"sha256": expected["digest"]}:
                    raise AssertionError("workload returned incorrect computation bytes")
                workers.add(attempts[0]["worker_id"])
                completed.append(
                    {
                        "job_id": job_id,
                        "worker_id": attempts[0]["worker_id"],
                        "kind": expected["kind"],
                        "latency_seconds": time.monotonic() - expected["submitted"],
                        "artifact_sha256": artifacts[0]["sha256"],
                        "one_attempt": True,
                    }
                )
                del pending[job_id]
            time.sleep(0.5)
        if set(kinds) - {item["kind"] for item in completed}:
            raise AssertionError("the run did not exercise every requested worker kind")
        rows = [client.request_object("GET", f"/workers/{worker}") for worker in sorted(workers)]
        evidence["final_workers"] = rows
        if require_idle_workers and any(
            row["running_jobs"] or row["cpu_reserved"] or row["memory_reserved_mb"] for row in rows
        ):
            raise AssertionError("isolated workers retained reservations after workloads drained")
        if not completed:
            raise AssertionError("the run completed no workloads")
        evidence["passed"] = True
    except BaseException as exc:
        evidence["error_type"] = type(exc).__name__
        evidence["unfinished_jobs"] = list(pending)
        # Only cancel jobs admitted by this run, never unrelated cluster workloads.
        for job_id in pending:
            with suppress(Exception):
                client.request("POST", f"/jobs/{job_id}/cancel")
        raise
    finally:
        elapsed = time.monotonic() - started
        latencies = [item["latency_seconds"] for item in completed]
        evidence.update(
            finished_at=datetime.now(UTC).isoformat(),
            elapsed_seconds=elapsed,
            submitted=sequence,
            succeeded=len(completed),
            jobs_per_second=len(completed) / max(1, elapsed),
            latency_p50_seconds=percentile(latencies, 0.5),
            latency_p95_seconds=percentile(latencies, 0.95),
            jobs=completed,
        )
        (output / "evidence.json").write_text(
            json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
        )
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--worker-kinds", default="any", help="any, or comma-separated python,cpp")
    parser.add_argument("--drain-seconds", type=int, default=120)
    parser.add_argument("--require-idle-workers", action="store_true")
    args = parser.parse_args()
    kinds = list(dict.fromkeys(args.worker_kinds.split(",")))
    if (
        not 10 <= args.seconds <= 86400
        or not 1 <= args.concurrency <= 64
        or not 10 <= args.drain_seconds <= 3600
        or not kinds
        or set(kinds) - {"any", "python", "cpp"}
    ):
        parser.error("invalid bounded load settings")
    client = Client(args.url, os.getenv("STRATA_API_KEY"))
    try:
        result = soak(
            client,
            args.image,
            args.output,
            args.seconds,
            args.concurrency,
            kinds,
            args.drain_seconds,
            args.require_idle_workers,
        )
        print(json.dumps({key: value for key, value in result.items() if key != "jobs"}, indent=2))
    finally:
        client.http.close()
