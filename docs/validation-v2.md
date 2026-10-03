# v2 workspace validation

This page records actual local checks performed on **2026-10-03** (Europe/Madrid), during preparation of v2.0.0 and before its first GitHub Actions run. These results are separate from [v1's historical CI evidence](validation.md). Consult [workflow runs](https://github.com/gragi-1/strata-compute-engine/actions/workflows/ci.yml) for verification of pushed commits; local results do not assert a GitHub CI outcome.

## Environment and evidence

One physical Windows computer, Python 3.13.5, PostgreSQL 17.11, Docker Desktop with Linux containers and Docker Engine 29.8.1. The C++ agent also built under Ubuntu WSL with GCC 11.4. The standard stack contains two Python agents and one C++ agent; their advertised resources are logical budgets on the same physical machine.

| Check | Observed result |
|---|---|
| Ruff lint / format | Passed |
| Strict mypy | Passed for 31 authored source files |
| JavaScript syntax | `node --check control_plane/web/app.js` passed |
| Python tests | **92 passed**, with the real PostgreSQL tests enabled |
| Coverage | **93.38%** for authored control-plane/scheduler code; 85% required |
| C++ build and CTest | Complete gRPC/Docker/OpenSSL agent built; **2/2** tests passed |
| Schema upgrade | Existing v1 jobs preserved during the additive PostgreSQL migration; empty-database upgrade and downgrade/upgrade/check also verified |
| Docker regression | Both agents executed real containers, uploaded verified artifacts and passed running cancellation/timeouts; C++ numerical workload succeeded |
| RPC partition | Both agents' local lease watchdogs stopped their claimed workload containers during the actual RPC outage probe |
| Read-only inputs | Both agents transferred an **8,388,636-byte** file, verified identical SHA-256 and rejected a write through `/inputs` |
| Dependency workflow | Python producer generated an artifact; C++ agent ran the dependent consumer; expected sum **6** verified |
| Parameter campaign | **48/48** jobs succeeded in **10.683 s** on the final images; repeated identical seeds produced identical estimates |
| TLS / independent daemons | Python and C++ agents verified the TLS server and ran dataset profiling on **two separate Docker daemons** |
| Dataset profiling | CSV, TSV, NumPy and Parquet numerical statistics tested; independent-daemon CSV result verified expected means and variances |
| Roles / drain | Viewer/operator/admin authorization, fail-closed production settings, worker draining, session expiry and resumed scheduling tested |
| Backup / restore | Snapshot backup, hashes, corruption rejection and restore into a separate empty PostgreSQL database verified |
| Web workspace | Actual browser job submission reached `SUCCEEDED`; dataset create/version/upload/seal/CSV preview verified; desktop and 319-pixel mobile layouts inspected |
| Scientific export | CLI generated CSV/JSON evidence, provenance and PNG/PDF figure; rendered figure inspected |
| Live Python SDK | Submitted profiling against the browser-uploaded CSV, reached `SUCCEEDED` and downloaded the verified result |

The coverage percentage excludes generated Protobuf modules and the scheduler process entry point. Worker/CLI/SDK/C++ behavior is checked separately; it is not whole-platform line coverage. The test suite emits one upstream Starlette/httpx TestClient warning. WSL emitted a filesystem clock-skew warning during the successful C++ build.

Versioned evidence: [dataset/workflow/campaign run](demo/platform-evidence.json), [verified TLS and independent daemons](demo/network-evidence.json), [workspace screenshot](demo/compute-workspace.png), [campaign figure](demo/campaign-comparison.png) and [PDF](demo/campaign-comparison.pdf). The campaign report includes [CSV](demo/campaign-results.csv) and [provenance](demo/campaign-provenance.json). Timestamps, job IDs and measured elapsed time belong to the recorded run. A small correctness campaign is not a throughput or scalability benchmark; historical benchmarks remain in [the benchmark report](benchmarks.md).

The [sealed CSV preview](demo/dataset-preview.png) records actual browser creation, upload and sealing. The [live SDK evidence](demo/sdk-evidence.json) contains the successful downstream profile and artifact metadata, with means/variances checked and downloaded bytes verified.

## Cases exercised

The new tests cover immutable version sealing, conflicting uploads, bounded native previews, missing bytes, non-finite/decimal/binary Parquet cells, storage-exhaustion HTTP errors, role restrictions, draining-worker recovery, atomic campaign admission and retry, idempotency conflicts, graph cycles/missing nodes, dependency gating and failure propagation, lease-authorized input ranges, expired leases, TLS certificate verification, verified backups and corruption rejection. SDK tests verify checksums/lengths, keep results from separate attempts and preserve previous files when a download is invalid.

The live input test crosses the 4 MiB RPC range limit and checks both worker implementations. The network probe uses disposable privileged Docker-in-Docker fixtures with separate image/volume stores and private Unix sockets. It preloads the workload image into each daemon and copies the input through the real RPC path. Both agents independently computed three rows, means **3** and **4**, and sample variances **4**. No second physical computer was available, so this proves separate-daemon execution and verified transport, not physical multi-machine speedup or availability.

Failures encountered during development were corrected and rerun: new non-null JSON columns needed server defaults for existing jobs; a web form had a JavaScript syntax error; the network test initially mounted a Docker socket directory over its certificate directory. The Windows C: drive also exhausted its capacity and stalled Docker. Strata-generated temporary/download/cache files and the disposable native PostgreSQL test cluster were removed; the application database/artifact volumes were retained. Docker was recovered, each final image built once, and the platform/TLS probes passed again. Storage capacity remains an operational constraint, especially for larger datasets, image builds and backups.

## Reproduce

```bash
uv sync --locked --extra analysis
uv run ruff check .
uv run ruff format --check .
uv run mypy
node --check control_plane/web/app.js
# Use a disposable PostgreSQL database with CREATE-schema permission.
STRATA_TEST_POSTGRES_URL=postgresql+psycopg://USER:PASSWORD@HOST/DB uv run pytest --cov
cmake -S worker_cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp -j2
ctest --test-dir build/cpp --output-on-failure
docker compose up --build -d
uv run python scripts/e2e_live.py
uv run python -m scripts.platform_e2e
uv run python -m scripts.network_probe
uv run python scripts/partition_probe.py
```

The network probe assumes the standard local `strata` Compose project, its development PostgreSQL credentials and local image tags. It requires developer dependencies, privileged test-container support and enough temporary storage. It is not intended to run against a production deployment. On sandboxed Windows, choose a writable pytest `--basetemp` and `cache_dir`. Each PostgreSQL test creates/drops its own schema; maintenance tests additionally need PostgreSQL client tools on PATH or the supported Windows installation path.

The updated CI workflow contains the new live platform and TLS/independent-daemon probes. Uploaded network evidence includes JSON/logs only; generated private keys are excluded. Its Ubuntu 24.04 job installs PostgreSQL 17 client tools explicitly, matching the test server for dump/restore operations. Record a successful GitHub run's URL and commit separately once the pushed workflow completes; follow [the publication procedure](publication.md).

## Boundaries

This is a usable trusted-team CPU batch platform with one durable PostgreSQL coordinator and filesystem blob storage. It has no GPU allocation, elastic provisioning, tenant data isolation/SSO, object-store backend, automatic retention or database failover. Multi-machine capacity, sustained performance, host failure recovery and long-term operational availability need measurements on real infrastructure. TLS support and deployment templates alone do not certify production operation; follow [deployment and capacity](deployment.md) and [security boundaries](security.md).
