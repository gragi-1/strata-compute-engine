# Validation evidence

Actual local verification on **2026-10-02**. The Compose deployment uses Linux containers on Docker Desktop; Python checks ran on Windows 11 / Python 3.13.5, and the complete C++ worker built under Ubuntu WSL with GCC 11.4.

## Results

| Check | Observed result |
|---|---|
| Ruff lint / format | All checks passed; 63 Python files formatted |
| Strict mypy | No issues in 23 authored source files |
| Python tests | **65 passed**, including all six real PostgreSQL tests; no skipped tests in this run |
| Coverage | **96.17%**, above the 85% floor for authored control-plane/scheduler code |
| C++ full worker build | Protobuf, gRPC, Docker client and agent compiled successfully |
| CTest | **2/2 passed**: monotonic lease tracker and numerical wave solver |
| Docker Compose | All service/workload images built; migrations completed and three workers healthy |
| Live execution | Both agents executed real containers, uploaded logs and artifacts whose SHA-256 was verified |
| C++ numerical workload | Wave simulation succeeded in its real container |
| Live cancellation / timeout | Both Python and C++ implementations reached the required terminal states |
| CLI | Submitted a 2000×2000 matrix workload, watched success and downloaded its artifact |
| Kill-worker recovery | Recorded a successful second attempt on a different worker; 65-second cast/GIF included |
| RPC outage | Both agents' actual Docker containers were exited while jobs were RETRYING; RPC restored and workers healthy afterward |
| Measured load | 1000/1000 jobs succeeded on three agents (1001 attempts); equal-budget 100-job runs succeeded on each implementation |
| Migrations | `upgrade head` and `alembic check` passed against PostgreSQL 17.11 |
| Observability | Metrics served, Prometheus target UP, Grafana reachable, Jaeger contained API/RPC/scheduler/worker traces |

The PostgreSQL tests exercise concurrent identical submissions, exact admission limits, two schedulers assigning 100 jobs once with resource reservations, cancellation versus completion, simultaneous timeout/loss recovery and the PostgreSQL clock. The other tests cover state transitions, tokens/session expiry, retries, artifacts, protocol flow, resource limits, uncertain launches and isolated cleanup failures.

The coverage scope excludes generated Protobuf modules and the scheduler process entry point. Worker behavior is tested separately; the stated percentage is not coverage of every Python/C++ line. The suite emits one upstream Starlette warning about its httpx TestClient integration; no test fails because of it.

## Reproduce

```bash
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy
# Supply a disposable PostgreSQL database with CREATE-schema permission:
STRATA_TEST_POSTGRES_URL=postgresql+psycopg://USER:PASSWORD@HOST/DB uv run pytest --cov
cmake -S worker_cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp -j2
ctest --test-dir build/cpp --output-on-failure
docker compose up --build -d
uv run python scripts/e2e_live.py
uv run python scripts/partition_probe.py
uv run python scripts/failure_demo.py
```

See [measured benchmarks](benchmarks.md), [recovery recording](demo.md) and the raw [RPC outage evidence](demo/partition-probe.json). The initial 1000-job run exposed a C++ launch/cleanup failure; its negative result and original errors are retained. Fixes report uncertain launches promptly, keep cleanup failures local to an attempt, and use Docker init for signal forwarding. A 33-second first outage observation was too early for the 30-second lease plus 5-second termination grace; the repeat uses a 38-second interval and strict container inspection.

## Release boundaries

The repository has no GitHub remote. Actions are configured but have **not** run remotely; there is no fabricated passing badge or published release. Local validation establishes this implementation's behavior in the stated environment, not production availability or multi-host scalability. At-least-once side effects, orphan overlap, trusted Docker-socket agents, local REST security and manual retention are documented in [fault tolerance](fault-tolerance.md) and [security](security.md).
