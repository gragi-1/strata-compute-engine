# Benchmarks

Actual measurements from **2026-10-02**, using real Docker execution, PostgreSQL scheduling, gRPC agents and artifact reporting. Every number below is backed by a linked raw report.

## Environment

| Component | Measured configuration |
|---|---|
| Host CPU | Intel Core i7-11800H @ 2.30 GHz; 8 cores / 16 logical processors |
| Client | Windows 11 build 26200; Python 3.13.5 |
| Docker daemon | Linux containers on Docker Desktop; Engine 29.8.1 |
| Docker VM resources | 16 logical CPUs; 8,173,215,744 bytes RAM (7.612 GiB) |
| Database | PostgreSQL 17.11, Debian x86_64 container |
| Default workers | Two Python agents and one C++ agent, on the same daemon |
| Per-worker advertised budget | 2 CPU / 1024 MiB; each job requests 1 CPU / 128 MiB |
| Workload | Seeded Python Monte Carlo, 100,000 samples per job, seed 42 |
| Submission concurrency | 16 threads |
| Recovery settings | Heartbeat 5 s; worker timeout 15 s; lease 30 s; retry base 2 s + jitter |

All three agents were healthy at each run's start. Comparisons explicitly target one node; the other agents remain idle. API, RPC, scheduler, database and monitoring share the host with workloads. Advertised agent budgets are logical reservations, not three separate physical machines.

## Execution throughput and placement

| Run | Active workers | Jobs succeeded | Attempts | Retried jobs | Total wall time (s) | Successful jobs/s | Placement p95 (s) |
|---|---:|---:|---:|---:|---:|---:|---:|
| [Mixed, final](benchmark-results/mixed-1000-retries.json) | 3 | **1000/1000** | 1001 | 1 | 267.277 | **3.741** | 231.146 |
| [Python agent](benchmark-results/python-100.json) | 1 | 100/100 | 100 | 0 | 61.835 | 1.617 | 52.899 |
| [C++ agent](benchmark-results/cpp-100.json) | 1 | 100/100 | 100 | 0 | 60.473 | 1.654 | 52.423 |
| [Initial mixed run, before fixes](benchmark-results/mixed-1000.json) | 3 | 998/1000 | 1000 | 0 | 253.105 | 3.943 | 219.323 |

The final runs permit three automatic retries. The initial run disabled retries (`max_retries=0` in that script version, although its report predates the explicit parameter field). Two C++ attempts ended with expired leases following an ambiguous Docker start and repeated cleanup errors. The [original failure histories](benchmark-results/baseline-failures.json) and [daemon-call errors](benchmark-results/baseline-worker-errors.log) are retained; that run is not a clean successful throughput result.

Launch failures now report FAILED promptly and cleanup errors stay local to an attempt. During the final 1000-job run, one C++ launch failed or lost its acknowledgement; the job retried on `worker-1` and succeeded. Its [durable attempts and events](benchmark-results/recovered-attempt.json) show the recovery. Retries provide eventual completion within the stated budget; they do not eliminate daemon failures or duplicate execution.

## Submission request latency

Seconds, using nearest-rank percentiles over each run's POST requests:

| Run | p50 | p95 | p99 |
|---|---:|---:|---:|
| Mixed, final | 0.372 | 0.545 | 2.261 |
| Python agent | 0.016 | 2.284 | 2.344 |
| C++ agent | 0.016 | 2.268 | 2.315 |
| Initial mixed run | 0.305 | 0.434 | 2.251 |

These latencies include the client connection, validation and serialized admission transaction. The small runs show a startup/tail effect that their medians do not capture; one run per configuration cannot identify its cause or establish stable request-latency distributions.

## Placement latency

Seconds from durable job creation to its final assignment:

| Run | p50 | p95 | p99 |
|---|---:|---:|---:|
| Mixed, final | 123.286 | 231.146 | 238.856 |
| Python agent | 28.835 | 52.899 | 55.510 |
| C++ agent | 28.928 | 52.423 | 54.481 |
| Initial mixed run | 120.864 | 219.323 | 227.834 |

A batch much larger than execution capacity deliberately creates a queue. These placement delays include waiting for previous jobs; they are not the duration of a scheduler transaction. For retried jobs this benchmark uses the last assignment, including earlier execution and backoff. The Prometheus `scheduler_latency_seconds` histogram instead measures each attempt from its own eligibility time to assignment.

## Interpretation and limits

Python and C++ agents achieved similar throughput here. They execute the same Python workload through the same Docker daemon; these results compare orchestration paths, not numerical language speed. Container startup, polling, reporting, cleanup and host load contribute to total wall time. A roughly 2% difference between two single runs is insufficient to claim a language advantage.

The 1000-job run validates a backlog with recovery on three agents. It does not measure physical multi-host scaling, 4/8/16-worker speedup, scheduler-only capacity or a production SLA. The one-worker comparisons use 100 jobs, so their rate cannot be treated as a controlled scaling baseline for the 1000-job batch. Images were already built and cached, runs were sequential, no warm-up/repeated-trial protocol was used, and confidence intervals are not asserted.

## Methodology

`benchmarks/run.py` submits a fixed seeded Monte Carlo workload, waits for every job to reach a terminal state and records:

- Successful completions / total wall-clock seconds, including worker discovery, submission and polling overhead. Post-run history/daemon metadata collection is outside that timer.
- Nearest-rank p50/p95/p99 submission request latency and placement delay.
- Actual worker IDs, capabilities, capacity and liveness at run start.
- Client OS/Python/CPU metadata and the local Docker daemon's CPU/RAM/version if available.
- Job count, input size, concurrency, permitted retries, total attempts and retried/failed jobs.

Use equal resource budgets and explicit implementation capabilities for agent comparisons:

```bash
uv run python benchmarks/run.py --jobs 1000 --output benchmarks/results/mixed.json
uv run python benchmarks/run.py --jobs 100 --implementation python --worker-id worker-1 --output benchmarks/results/python.json
uv run python benchmarks/run.py --jobs 100 --implementation cpp --worker-id worker-cpp --output benchmarks/results/cpp.json
```

The default deployment has two Python workers and one C++ worker. The explicit node selection above compares one worker and the same resource budget in each run; omitting that selection changes the comparison's worker count. Container startup is shared and can dominate dispatch overhead. Cross-language numerical speed is a separate question from agent overhead.

For additional admission load, `benchmarks/submit.js` provides a k6 script; it was not executed for the measurements above. For saturation experiments, reduce `STRATA_QUEUE_LIMIT`, record 429 rates separately and report offered load, admitted load and completed throughput. A request benchmark alone does not measure distributed execution throughput.
