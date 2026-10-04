# Strata Compute Workspace

This guide describes the prepared v3 self-hosted Compute Workspace. Consult [release history](https://github.com/gragi-1/strata-compute-engine/releases) for published versions, [v3 local validation](validation-v3.md) for current evidence and [the publication procedure](publishing-v3.md) before releasing. Historical [v1](validation.md) and [v2](validation-v2.md) evidence remains separate.

Strata is a general CPU/GPU compute workspace: supply a container image and an argument array, attach inputs, request resources and collect outputs. It can run numerical simulations, data processing, model evaluation, rendering or other finite programs supported by the chosen image. The included numerical workloads are examples, not a restriction on the engine.

## Workflows you can use today

Open the **Compute Workspace** at `http://localhost:8000/`. It provides jobs, campaigns, datasets, experiments, schedules, webhooks, compute groups, interactive sessions, workers and account/project administration. The web interface, API, CLI and Python SDK share the same durable store.

### Independent runs and parameter campaigns

```bash
uv sync --locked --extra analysis
uv run strata campaign examples/campaign.yaml --idempotency-key my-study-v1
uv run strata campaign-status CAMPAIGN_ID
uv run strata report CAMPAIGN_ID results/study --x seed --y result.pi --group samples
```

The campaign template expands `${parameter}` within individual command arguments. A matrix and repetition count determine its size. Parameters remain attached to each job; `${repeat}` is available as a reserved counter. Submitting a campaign is one transaction: capacity rejection or invalid input leaves no partial campaign. Idempotency keys reject mismatched specifications and return the original campaign for identical requests.

Repeating an identical seed checks reproducibility. It does not create independent statistical replicates. Use independent seeds when estimating uncertainty. The example Monte Carlo workload reports an estimated standard error and absolute error relative to mathematical pi. The report exports the original results, campaign specification, plotting version, CSV, PNG and PDF; choose meaningful x/y/group columns for your study.

Campaign cancellation processes individual jobs and returns per-job errors. Campaign retry is atomic: all failed/cancelled/timed-out jobs are requeued in one transaction, subject to admission capacity, so dependency workflows cannot observe partially retried predecessors.

### Datasets and read-only inputs

```bash
uv run strata dataset-upload "Observations" observations.csv --label v1
uv run strata datasets
uv run strata preview FILE_ID --limit 20
```

Each dataset contains draft versions. Upload files with flat portable names, then seal a version. The SHA-256 manifest records every file name, byte length and content hash. Sealed versions are immutable; create a new version when data changes. Repeating an identical draft upload is idempotent; replacing a file with different bytes is rejected.

Attach a sealed version to a job:

```yaml
name: Profile observations
image: strata/python-workloads:local
command: [python, /app/main.py, profile, --input, /inputs/data/observations.csv]
inputs:
  - { version_id: VERSION_ID, alias: data }
resources: { cpu: 1, memory_mb: 256 }
```

Both agents download attempt-authorized byte ranges, renew leases between transfers, verify SHA-256 and populate an isolated Docker volume using a helper container that never starts. Workloads receive that volume at `/inputs` in read-only mode. Dataset transfer requires the `dataset-inputs` worker capability, preventing older workers from silently running without inputs. Write result files into `/output`.

The example profiler streams CSV, TSV and Parquet rows and memory-maps NPY arrays. It reports counts, numeric means, minimum/maximum and sample variances while excluding missing and non-finite numeric values. It is a reference workload; domain-specific validation and analysis belong in your own container.

Defaults: **1 GiB per uploaded file**, **1,000 files per version**, **16 input aliases per job**, **8 MiB preview limit**, **50 preview rows**, **16 MiB artifact transfer limit**, **1 MiB logs**. Previews support CSV, TSV, JSON, NPY and Parquet. Non-finite native values display as null, decimal values as strings and binary values as hexadecimal strings; the stored bytes are unchanged. Larger files can be downloaded or processed by a job; preview limits do not limit job input size. Uploads, staging buffers and backups require disk capacity; budget temporary storage as well as permanent blobs. Nested dataset paths and nested output artifacts are not supported. [Resumable uploads](resumable-uploads.md) handle interrupted transfers. [Dataset exploration](dataset-exploration.md) queries larger tabular files within memory/time/response budgets. [Output storage](storage-retention.md) adds kernel byte/inode limits; the current C++ transfer ceiling remains 16 MiB.

### Dependent work with artifact transfer

```bash
uv run strata workflow examples/workflow.yaml --idempotency-key observations-pipeline-v1
```

Workflow nodes refer to one another by name. `depends_on` waits for successful predecessors. An `artifact_inputs` entry also establishes a dependency and mounts the requested successful predecessor artifact under `/inputs/<alias>/<name>`. Missing nodes and cycles are rejected before any job is committed. Failed/cancelled/timed-out predecessors fail their waiting dependants, which can be retried after addressing the upstream failure. Running upstream retry history is preserved. No shell expansion or arbitrary template evaluation is performed.

For individually submitted jobs, dependencies and artifact inputs refer to existing job IDs. By default a downstream node starts after all predecessors succeed. [Conditional dependencies](periodic-jobs-and-conditions.md) also support cleanup after terminal parents and failure branches; [dynamic expansion](dynamic-workflows.md) creates parameter-driven children and joins. A predecessor that succeeds without the named artifact causes the downstream launch to fail and follow its configured retry policy.

### Python and notebooks

```python
from pathlib import Path
from strata_sdk import Client

with Client() as strata:
    version = strata.dataset("Observations", [Path("observations.csv")])
    job = strata.submit(
        {
            "name": "Profile observations",
            "image": "strata/python-workloads:local",
            "command": [
                "python",
                "/app/main.py",
                "profile",
                "--input",
                "/inputs/data/observations.csv",
            ],
            "inputs": [{"version_id": version["id"], "alias": "data"}],
            "resources": {"cpu": 1, "memory_mb": 256},
        },
        key="observations-profile-v1",
    )
    finished = strata.wait(job["id"])
    if finished["status"] != "SUCCEEDED":
        raise RuntimeError(finished)
    strata.artifacts(job["id"], Path("results"))
```

The SDK streams uploads/downloads and verifies their hashes. Artifact downloads also verify byte lengths and use temporary files before replacing a destination. If several attempts produced the same artifact name, each is saved under its own `attempt-<id>` directory; a single artifact name is saved directly in the output directory. Download after terminal completion for a stable attempt history. Configure `STRATA_API_URL`, `STRATA_ACCESS_TOKEN` and `STRATA_PROJECT_ID` for individual project access; legacy shared-key deployments use `STRATA_API_KEY`. `Client.campaign`, `Client.workflow`, `Client.results` and `Client.wait(..., campaign=True)` support larger studies.

## Access and operations

`STRATA_API_KEYS` is a JSON mapping from secret keys (at least 24 characters) to roles: `viewer`, `operator`, `admin`. Viewers can inspect and download; operators can submit/control jobs and data; administrators can also drain/resume workers. Rotation is an administrator configuration change followed by API restart. The browser keeps its key in session storage for that tab session. Worker credentials are separate and never returned by public API routes.

Enable [individual identity and projects](identity-and-projects.md) for accounts, revocable sessions/service tokens, private resources, memberships, project budgets and audit. [OIDC](federated-identity.md) supports explicitly linked provider identities. Empty legacy key configuration is local development mode; if projects exist, disabling individual identity refuses private resource access. Production requires role keys or individual identity, a strong worker token, verified RPC TLS and separately configured REST HTTPS. Workload authors and Docker-socket agents remain trusted infrastructure; application project authorization does not establish hostile-tenant host isolation.

Drain stops new assignments while current attempts keep their leases and finish normally. Resume requires a live session. Stale draining workers still recover through the normal liveness mechanism.

```bash
# PostgreSQL client tools must be available. Configure the exact database and blob directory.
uv run strata-admin backup backups/snapshot-001
uv run strata-admin verify-backup backups/snapshot-001
uv run strata-admin storage-audit
# Restore only into a newly created, empty database and empty blob directory.
uv run strata-admin restore backups/snapshot-001
```

Backups use a PostgreSQL exported repeatable-read snapshot for the dump and referenced blob list. Immutable files are copied and checked by hash; a manifest verifies both SQL dump and files. An incomplete backup has no completed manifest. Protect backups because they include data, logs and internal attempt credentials. Restore refuses existing tables or nonempty blob destinations and restores SQL in one transaction. Keep API, scheduler and workers stopped until copying completes, then allow stale sessions/leases to expire before enabling new execution. Restore has been tested against a disposable database. Storage audit reports missing and unreferenced blobs without deleting files; [supervised operations](continuous-operations.md) can run backups, restore drills, reference-safe retention and independent orphan cleanup. Scientific datasets/artifacts are preserved unless an administrator explicitly selects their documented retention operation.

## Scope and limits

The core supports finite CPU jobs, independent campaigns and DAG workflows. Adding an image enables another language/tool as long as it runs within the container restrictions. Containers have disabled network access, a read-only root filesystem, an isolated output volume and bounded CPU/RAM. Preload allowlisted images on every execution host. Pin images by digest for reproducible studies; mutable `:local` tags are for development.

The v3 feature paths include [whole-device NVIDIA GPU execution](gpu-execution.md), [S3-compatible storage](object-storage.md), [experiments and pinned replay](experiments.md), [periodic jobs](periodic-jobs-and-conditions.md), [signed webhooks](webhooks.md), [managed groups/notebooks/services](managed-runtimes.md), [maintenance controls](cluster-maintenance.md), [bounded Docker host pools](elastic-worker-pools.md) and [coordinator/database recovery](high-availability.md). Each guide defines its limits and actual acceptance evidence.

Groups provide bounded coordinator-mediated collectives, not MPI/RDMA. Notebooks provide a managed Python namespace and `.ipynb` export, not arbitrary Jupyter extensions. Outputs remain bounded; multi-gigabyte persistent scratch needs a separate reviewed quota-backed adapter. Cloud instance/GPU purchasing and monetary billing controls are deferred. Physical multi-host/multi-GPU capacity, independent security review, external backups/notifications and redundant failure domains require deployment acceptance.

Use [deployment guidance](deployment.md) to attach other machines, and [v3 validation evidence](validation-v3.md) to distinguish implemented behavior from unmeasured capacity.
