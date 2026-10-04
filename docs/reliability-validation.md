# Reliability validation

Strata keeps execution correctness separate from measured capacity. A passing single-host test does not establish a physical cluster throughput limit, production availability or a security certification. Every reported result should identify the source revision, immutable image IDs, dependency lock, hardware, enabled test profile and retained evidence.

## Integrated runtime profile

The normal Python suite includes deterministic unit/domain tests and real PostgreSQL concurrency checks when `STRATA_TEST_POSTGRES_URL` is supplied. Actual S3, Docker, CUDA and recovery checks are opt-in so they cannot silently claim unavailable services worked. See the CI workflow for the Linux profiles.

Set `STRATA_TEST_DOCKER_RUNTIME=1`, `STRATA_TEST_POSTGRES_HA=1`, the PostgreSQL/S3 endpoint variables and the reviewed workload/worker/coordinator/infrastructure image variables to enable the full local profile. Configure `STRATA_TEST_HA_EVIDENCE` and `STRATA_TEST_OUTPUT_EVIDENCE` to retain recovery/quota observations. The CUDA profile requires a real compatible NVIDIA runtime, not a mocked device inventory.

```sh
uv run pytest --cov --cov-report=term --cov-report=xml:build/validation/coverage.xml \
  --junitxml=build/validation/junit.xml --basetemp=build/validation/temp
```

Use a fresh temporary directory per run. PostgreSQL fixtures create UUID schemas/databases and remove only their own resources. Docker fixtures label owned worker identities and remove only those resources. Preserve failed logs; changing an assertion to skip a live failure does not establish reliability.

## Bounded sustained load

`scripts.soak` submits actual container workloads through the public SDK, replays every submission key, verifies a single successful attempt, downloads outputs using SDK size/hash checks, validates the workload's computation bytes and retains per-job latency and worker provenance. It keeps at most the configured number of outstanding jobs. On failure it records incomplete jobs and requests cancellation only for jobs admitted by that run.

Use an isolated, authenticated cluster with reviewed images containing Python. No worker, database or cloud infrastructure is provisioned by the load command itself. Select a project with a scoped credential and sufficient queue/resource/storage budgets. Credentials are supplied through the SDK's environment configuration and are not written into the evidence.

```sh
export STRATA_API_KEY=YOUR_SCOPED_TEST_CREDENTIAL
export STRATA_PROJECT_ID=YOUR_TEST_PROJECT
uv run python -m scripts.soak --url http://127.0.0.1:8000 \
  --image YOUR_APPROVED_PYTHON_IMAGE --output build/soak-unique \
  --seconds 3600 --concurrency 8 --worker-kinds python,cpp --require-idle-workers
```

On PowerShell use `$env:STRATA_API_KEY` and `$env:STRATA_PROJECT_ID`. The output directory must be new. `--worker-kinds python,cpp` requires both real worker capabilities; the default `any` accepts any eligible worker. `--require-idle-workers` additionally checks exact zero final reservations and is appropriate only for dedicated test workers. Other users' concurrent jobs can legitimately leave reservations on a shared worker.

The default run lasts one hour, with a bounded drain deadline. A shorter local qualification run may be useful during development; label its actual duration explicitly. Do not extrapolate a ten-minute result into a day-long endurance test, multi-host benchmark or availability SLO. Latencies include admission, queueing, container/helper startup, execution, transfer and polling; they are not isolated numerical kernel timings. The test uses small outputs and does not characterize large input staging, large result transfers, GPU utilization or WAN throughput.

Keep the physical test host awake for the submission and drain window. On Windows, automatic sleep or Modern Standby can suspend Docker Desktop and all test processes together, producing expired leases and misleading wall-clock gaps. Preserve interrupted evidence and record the operating-system suspension/resumption events; do not count that run as a successful endurance result. A bounded temporary system sleep inhibitor may be used during supervised validation and must be released afterward. It does not prevent explicit lid/power-button suspension. See [Windows execution-state behavior](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-setthreadexecutionstate) and [Strata's failure semantics](fault-tolerance.md).

## Browser regression and accessibility

The pinned development-only Playwright/axe packages and integrity lock are in `tests/browser`. Their test server uses a fresh SQLite database, native synthetic users/projects and the actual HTTP API/browser assets on port 58005. It refuses to reuse an existing server. It starts no execution agent and does not contact the user's development service on port 8000.

```sh
uv sync --locked --extra analysis
cd tests/browser
npm ci --ignore-scripts
npm audit --audit-level=high
npx playwright install chromium firefox webkit
npm test
```

Set `STRATA_BROWSER_PYTHON` to an alternative installed Python environment when needed. `PLAYWRIGHT_BROWSERS_PATH` can locate browsers in an ignored build directory. The same seven regressions run in Chromium, Firefox and WebKit with one worker and no retries. Mutating test resources have distinct names per engine. CI installs the three engines' Linux system dependencies and retains JUnit, failed screenshots, traces and accessibility JSON attachments under `build/browser`.

The regression suite covers native sign-in/logout, persisted project selection, role-specific controls, every navigation page, idempotent recovery after a real committed submission loses its browser response, queued gang cancellation, notebook lifecycle/export and mobile keyboard-focusable tables. Automated axe checks cover configured WCAG 2 A/AA and 2.1 AA rules without excluding failures. Actual cooperating execution, persistent notebook state and private HTTP service requests are separately exercised through both Docker agents and supervised browser checks.

Automated accessibility checks do not establish conformance for every assistive technology or all dynamic states. Manual keyboard, screen-reader, zoom and cross-browser review remains part of production acceptance. Reports must retain that distinction.

## External acceptance

Physical failure domains, provider capacity/billing, durable external backups, production federation, independent security assessment and signed GitHub candidate artifacts require their actual deployment prerequisites. Use the [product ledger](product-plan.md), [high availability](high-availability.md), [distribution procedure](distribution-and-upgrades.md) and [security review](native-security-review.md) to record those separately from local evidence.
