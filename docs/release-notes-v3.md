# Strata v3.0.0: Self-hosted Compute Workspace

Strata v3.0.0 expands the durable execution engine into a self-hosted compute workspace for simulations, datasets, experiments and cooperating workloads. It supports operator-managed CPU/GPU machines and bounded Docker host worker pools. Historical v1/v2 releases remain separate; provider-specific cloud provisioning is outside this release's scope.

Strata now provides a shared compute workspace for programs, simulations and data-processing pipelines: users organize immutable datasets, submit approved container workloads, follow execution, compare experiments and keep verified results. PostgreSQL records scheduling and recovery decisions. Python and C++ agents execute CPU/GPU work with fenced leases and bounded resources. The language of a workload is determined by its approved image.

## Added

- Individual accounts, revocable sessions and service credentials, private projects, memberships, atomic project budgets and actor-specific audit history. OIDC uses verified issuer/audience, nonce and PKCE with controlled account provisioning.
- Filesystem and S3-compatible objects, persistent resumable upload sessions, verified sealed dataset versions and bounded CSV, TSV, JSON, NumPy and Parquet exploration with filters, summaries and plots.
- Experiment provenance and searchable metrics, run comparison/replay, checkpoint input contracts, durable periodic jobs, conditional dependencies, dynamic fan-out/join and signed durable webhook delivery.
- Fair concurrent scheduling, transactional whole-device GPU reservation and CPU/RAM/GPU budget enforcement on both worker implementations.
- Cooperating computation groups that reserve all ranks on distinct worker identities together, provide ordered durable barriers/reductions and cancel peers after rank failure. A retry starts a fresh complete group after earlier ranks stop.
- Persistent Python notebook sessions with bounded code/results, idle/execution deadlines and `.ipynb` export. Managed container-local HTTP services use authenticated bounded requests without publishing workload ports.
- Live job logs, responsive keyboard-focusable tables and web/CLI/SDK paths for the new features. Pinned browser regression and automated accessibility checks are part of CI.
- Enforced kernel output byte/inode quotas, independent output-retention helpers, persisted helper resource charges, reference-safe retention and bounded input caches.
- Bounded reconciliation of ambiguous Docker start replies on both agents, batch lease renewal during startup and recovery through temporary coordinator disconnects without extending unconfirmed leases. Cancellation and watchdog decisions remain authoritative.
- Durable maintenance controls, bounded Docker host CPU worker pools with safe drain/reconciliation, independent orphan cleanup, automated backups/restore drills and actionable alert rules.
- Supervised API/RPC replicas with shared objects and verified transport TLS, plus PostgreSQL/Patroni/etcd primary recovery, rejoin and quorum write fencing.
- Installable versioned packages with bundled migrations/browser assets, deterministic wheel/source builds, clean-install verification, retained dependency/image reports and a manually dispatched candidate provenance workflow.

## Local qualification

The implementation passed 273 integrated Python checks with actual PostgreSQL, S3, Docker, GPU and recovery services enabled; coordinator/scheduler coverage was 90.27%. Four native C++ checks, 21 browser regressions across Chromium/Firefox/WebKit and 45 configured accessibility reports passed. An isolated one-hour submission run completed 1,101 of 1,101 jobs through both agents, with one attempt per job, verified outputs and zero final CPU/RAM/job reservations.

These runtime measurements were recorded as `3.0.0.dev0` on one physical computer. The release preparation changes package/native version metadata to `3.0.0` and updates current guides. Reproducible package and clean-install checks for the release version are recorded separately in [v3 validation](https://github.com/gragi-1/strata-compute-engine/blob/v3.0.0/docs/validation-v3.md). Local evidence does not establish physical multi-host throughput, long-term availability or independent security approval. Exact-commit GitHub CI and candidate provenance must succeed before publication; follow [the publication procedure](https://github.com/gragi-1/strata-compute-engine/blob/v3.0.0/docs/publishing-v3.md).

## Upgrade requirements

This is a major deployment change. Drain execution, stop schedulers/provisioning, create and verify a database/blob backup, install matching coordinator/agent/client versions and run `strata-admin migrate` plus `strata-admin migrate --check`. Migration `0017` adds managed runtimes. Reviewed immutable keeper images and Linux tmpfs `noswap` support are required for bounded retained outputs. Follow [the distribution and upgrade procedure](https://github.com/gragi-1/strata-compute-engine/blob/v3.0.0/docs/distribution-and-upgrades.md); an older coordinator must not run against an unverified newer schema.

## Defined boundaries

Workload execution remains at least once after failures; submission idempotency prevents duplicate jobs, not arbitrary external effects. Managed groups provide bounded barriers/vector reductions, with 2–16 ranks, rather than MPI/RDMA or a distributed filesystem. Notebook sessions provide a stateful Python kernel and standard export; rich Jupyter extensions and arbitrary web application proxying are outside that contract. Workload outputs have kernel byte/inode quotas; the current C++ artifact transfer ceiling is 16 MiB. Multi-gigabyte persistent scratch needs a separate reviewed quota-backed adapter. Checkpoint serialization and resume logic belong to the workload.

Docker host provisioning has bounded local capacity and estimated instance-time accounting. Cloud instance/GPU provisioning and provider monetary billing require a selected provider, credentials and an approved budget. Physical multi-host/multi-GPU capacity and availability need their actual hardware/failure-domain tests. Production federation, durable external backup destinations, notification routing and redundant/fenced gateways need deployment configuration.

Raw unresolved HIGH dependency/image findings remain visible. Passing the CRITICAL/fixable-HIGH gate is not a final security approval. Independent security and manual assistive-technology review remain separate acceptance requirements. Consult the [product ledger](https://github.com/gragi-1/strata-compute-engine/blob/v3.0.0/docs/product-plan.md), [validation guide](https://github.com/gragi-1/strata-compute-engine/blob/v3.0.0/docs/reliability-validation.md) and [native component review](https://github.com/gragi-1/strata-compute-engine/blob/v3.0.0/docs/native-security-review.md) for the evidence and exact limitations.
