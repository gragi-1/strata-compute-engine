# Development

Use Python 3.12+, uv and PostgreSQL 17. Run `uv sync --locked`. All production services use PostgreSQL; SQLite is supported only for single-process unit/protocol tests.

Configuration comes from `STRATA_*` environment variables. `.env.example` is for Compose or explicit export; services do not implicitly load a local dotenv file. The allowlist is a JSON list, for example `STRATA_ALLOWED_IMAGES='["strata/python-workloads:local","strata/wave-solver:local"]'`. All workload images must provide a world-writable `/output` directory for their non-root user. The supplied Dockerfiles set its mode to 1777. Agents create a separate output volume per attempt and remove it after successful reporting or fencing.

Start independent services:

```bash
uv run alembic upgrade head
uv run uvicorn control_plane.api:app
uv run strata-rpc
uv run strata-scheduler
STRATA_WORKER_ID=dev-worker uv run strata-worker
```

The worker requires a Linux Docker daemon. `STRATA_RPC_TARGET`, `STRATA_WORKER_CPU`, `STRATA_WORKER_MEMORY_MB` and `STRATA_WORKER_TOKEN` configure its connection and logical budget. Multiple agents on one host need distinct IDs and explicit budgets.

On Debian/Ubuntu, install C++ build dependencies:

```bash
sudo apt-get install g++ cmake make pkg-config libgrpc++-dev protobuf-compiler-grpc \
  libprotobuf-dev protobuf-compiler libcurl4-openssl-dev libarchive-dev nlohmann-json3-dev
cmake -S worker_cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp -j2
ctest --test-dir build/cpp --output-on-failure
```

`-DSTRATA_BUILD_WORKER=OFF` builds only the numerical workload and portable lease test. It does not validate the C++ agent. The full C++ agent targets Linux and a Unix Docker socket. The shared Protobuf schema is `proto/engine.proto`; `uv run python scripts/generate_proto.py` refreshes checked-in Python bindings. CMake generates C++ bindings using the installed Protobuf/gRPC toolchain.

Run actual database races by setting `STRATA_TEST_POSTGRES_URL`. Each fixture uses a fresh schema; the database user needs CREATE privileges. `uv run alembic check` verifies model/migration agreement. Generated Python protobuf code is excluded from lint, typing and coverage; all authored control-plane/scheduler code contributes to the coverage floor.
