# Cluster maintenance controls

Platform administrators can independently stop new job admission and pause assignment of queued jobs. State lives in PostgreSQL and applies to every API and scheduler replica. Migration `0014` enables both controls by default, preserving normal scheduling until an administrator changes them.

In the web workspace, open **Workers → Cluster admission**. Use **Pause new jobs** to drain an existing queue, or **Pause assignments** to prevent further placement. **Edit admission & reason** sets both controls and records a bounded maintenance reason. Platform administrators can use the CLI and SDK as well:

```sh
strata admission
strata set-admission false true --reason "Drain queued work before upgrade"
strata set-admission false false --reason "Upgrade maintenance window"
strata set-admission true true --reason "Upgrade smoke tests passed"
```

```python
from strata_sdk import Client

with Client(access_token=admin_session) as client:
    state = client.update_admission(
        accepting_jobs=False, scheduling_enabled=False, reason="Upgrade maintenance window"
    )
```

`GET /cluster/admission` and `PATCH /cluster/admission` require platform administration. Project administrators and scoped automation tokens cannot change global availability. PATCH requires both boolean values; repeated identical updates do not create duplicate audit events. Changes record the actor and timestamp. Reasons are administrative text; do not enter credentials.

When admission is paused, new jobs, campaigns, workflows, manual retries and experiment replay submissions receive HTTP 503 with `Retry-After: 30`. A matching idempotent replay of an existing submission still returns its existing resource. Periodic occurrences and dynamic expansion remain pending and retry with bounded polling; they are not disabled or discarded by maintenance. Definitions and read-only dataset exploration remain available.

When assignments are paused, scheduler transactions already in progress finish before the pause succeeds. The shared PostgreSQL row fence allows concurrent schedulers during normal operation and gives the administrative update an exclusive barrier. No subsequent assignment transaction can create attempts until the control is resumed. Running attempts keep their leases, upload outputs and finish normally. Cancellation and expired-lease recovery continue; recovered retries wait in the queue while assignment is paused.

The controls persist across process restarts. They do not stop processes, cancel running work, make schema migrations compatible with live older services or create a filesystem snapshot. Follow the complete [upgrade procedure](distribution-and-upgrades.md), including verified backup/restore and stopping services before changing schema. Monitoring exposes `strata_cluster_accepting_jobs` and `strata_cluster_scheduling_enabled`; an intentional maintenance pause is not a readiness failure.
