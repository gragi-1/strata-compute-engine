# Experiment registry and replay

Experiments group individual compute runs within a project. Each run atomically records its submitted job, optional full source commit hash and bounded metadata, and creates one job under the existing queue and execution budgets. Idempotency keys are project-scoped. Viewers may inspect, search and compare; operators may submit runs and record metrics.

```python
from strata_sdk import Client

with Client(access_token="YOUR_TOKEN", project_id="YOUR_PROJECT") as client:
    study = client.experiment("Numerical convergence", "Compare discretization error")
    run = client.experiment_run(
        study["id"],
        {
            "source_revision": "FULL_40_OR_64_CHARACTER_COMMIT_HASH",
            "metadata": {"seed": 42},
            "job": {
                "name": "Simulation",
                "image": "YOUR_ALLOWLISTED_IMAGE",
                "command": ["python", "simulation.py", "--seed", "42"],
                "inputs": [{"version_id": "SEALED_VERSION", "alias": "measurements"}],
            },
        },
        key="simulation-seed-42",
    )
    client.record_metrics(run["id"], error=0.0125, elapsed_seconds=8.4)
    comparison = client.compare_runs([run["id"]])
```

Use `strata experiments create/list/run/runs/inspect/metrics/compare/replay` for the equivalent CLI paths. `run` reads a YAML/JSON run specification, `metrics` reads a name-to-number mapping, and `replay --options FILE` reads optional replay/checkpoint settings. The web **Experiments** page offers run submission, inspection, JSON export, metrics and selected-run comparison. Numeric metric bounds can be queried through `GET /experiments/{id}/runs?metric=error&maximum=0.02`. Lists and comparisons are bounded.

At input manifest resolution and start, the coordinator records the actual input SHA-256 values and artifact/version identities. Both current workers report Docker's resolved image ID, in addition to the submitted image reference. Attempt provenance remains separate across retries. A source commit is supplied by the caller; Strata does not attest that a running image was built from that commit. Runtime image configuration is identified by Docker's content-addressed image ID. Metrics are caller-recorded immutable summaries: a repeated identical value is accepted, but changing an existing value returns 409.

`POST /experiment-runs/{id}/replay` requires a terminal source attempt with a recorded resolved image. It creates another run, preserving the submission, sealed datasets and pinned upstream artifact identities. The assignment requires the `image-pinning` capability. Both workers and the coordinator reject a different resolved image before execution starts. If a mutable tag has moved, restore the recorded image/tag or configure the appropriate allowlist; Strata never silently substitutes the new image. Older agents can still execute ordinary jobs but cannot execute these pinned replays. Upgrade both agents for complete provenance.

To resume a workload, publish a checkpoint as an ordinary verified artifact before the attempt ends. Select the terminal source attempt and supply its artifact ID, job ID, file name and input alias:

```json
{
  "attempt_id": "SOURCE_ATTEMPT",
  "checkpoints": [{
    "artifact_id": "CHECKPOINT_ARTIFACT", "job_id": "SOURCE_JOB",
    "name": "checkpoint.json", "alias": "checkpoint"
  }],
  "command": ["python", "simulation.py", "--resume", "/inputs/checkpoint/checkpoint.json"]
}
```

Pinned checkpoint bytes can come from a failed terminal attempt; the new job does not wait for that failed job to succeed. Alias uniqueness, project ownership and source-attempt membership are checked. The workload must implement its own state serialization and resume behavior. Strata transports and verifies that state; it does not capture arbitrary running process memory. Outputs still use the existing per-attempt artifact contract. Automatic checkpoint intervals and metric time series are separate extensions.

Local evidence includes API authorization/idempotency/search/comparison, real PostgreSQL duplicate-run admission, checkpoint replay and image mismatch rejection, actual Python and C++ worker Docker execution/cache reuse/pinned replay, and browser experiment creation/submission/metric comparison. No physical multi-host reproducibility claim is made.
