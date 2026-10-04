# Supervised operations

The optional operations service schedules reference-safe maintenance and verified PostgreSQL backups. Status survives restarts in `maintenance_states`; schema-scoped PostgreSQL advisory locks coalesce concurrent runners. A process that dies while running an operation leaves a recoverable status, and the next runner can retry after obtaining the released lock.

## Enable the services

The operations deployment ID was introduced in migration `0012`; upgrade to the current packaged head (`0017` for this candidate), following [the full upgrade procedure](distribution-and-upgrades.md). Rebuild the coordinator/Python worker and C++ worker images before restarting agents. All upgraded components use a stable database deployment ID for Docker labels. Previously created resources without `strata.cluster` are deliberately outside independent cleanup. Stop the old agents and inspect their labelled leftovers before manually migrating or removing them. Back up before upgrading.

```sh
docker compose --profile operations up --build -d
strata operations
```

The profile adds `operations` and `host-reaper`; it preserves the deployment's existing database and artifact volumes. Backups go into the separate `backups` volume. Deploy a host reaper on each worker's Docker host, using that deployment's private worker credential and trusted RPC CA. It supports both Python and C++ workload containers. `strata-reaper once` previews decisions; `strata-reaper once --apply` applies them; `strata-reaper run` repeats cleanup independently of the workload agent.

Host cleanup first obtains the authenticated coordinator's cluster ID, then inspects only Docker objects with that ID and valid worker/attempt labels. The coordinator serializes decisions with lease renewal and protects active attempts. Expired, terminal and unknown attempts can be removed. An unavailable coordinator causes cleanup to stop. Volumes require the exact attempt-derived name and cannot be removed while mounted. The service scans rotating batches; it neither registers a worker nor extends leases. This does not prevent external side effects before an orphan is stopped.

## Retention policy

Every 60 seconds by default, supervision removes old crashed staging/pin lifetimes using kernel-held file leases. Active lifetimes remain protected even when their modification time is old. Uploads, assembly, HTTP staging and S3 pins use these leases. Download and blob-copy writers hold the root lock throughout allocation. Unknown paths, canonical scientific blobs and temporary names from older unleased versions are excluded from the staging reaper.

History and unreferenced-blob collection default to inspection. Explicitly set `STRATA_MAINTENANCE_APPLY_HISTORY=true` and/or `STRATA_MAINTENANCE_APPLY_BLOBS=true` when that retention policy is appropriate. Their existing grace periods and bounded batches apply. Expired OIDC transactions, old login throttle buckets and completed webhook deliveries join expired credentials, transfer history, heartbeat samples and terminal log text in history pruning. Jobs, attempts, job events, audit records, datasets and output metadata survive.

`strata-admin staging-collect` is a dry run; `--apply` removes eligible inactive managed paths. See [storage retention](storage-retention.md) for the reference fence, budgets and preserved data. Configure S3 lifecycle rules for unfinished multipart uploads if your provider supports them; local process cleanup cannot remove provider-side upload sessions after a machine loss.

## Backup and restore drills

The supervisor requires PostgreSQL. Configure `STRATA_BACKUP_ROOT` for a native deployment; the Compose profile sets `/app/data/backups`. The root must be separate from artifact storage. The container includes PostgreSQL 17 client tools. Backups use an exported database snapshot and the exact referenced immutable blobs, then verify the dump and every blob against the manifest.

`STRATA_BACKUP_INTERVAL_SECONDS` defaults to 86400, `STRATA_BACKUP_KEEP` to seven verified copies, and `STRATA_BACKUP_MAX_BYTES` to 100 GiB per snapshot. Dump monitoring enforces the byte budget, minimum free space and `STRATA_BACKUP_TIMEOUT_SECONDS` (one hour by default). A failed incomplete supervisor backup is removed; the last verified snapshots remain. Snapshot rotation follows a new successful verified backup. Only correctly named, immediate, unlinked children of the configured backup root can be removed.

Set `STRATA_BACKUP_RESTORE_DRILL=true` to restore each new backup into a fresh `strata_drill_UUID` database and an isolated file directory. The drill checks the admission row, database blob references and every restored checksum, then drops only its fresh database and directory. The database role needs `CREATEDB`; reserve capacity for the dump, copies and restored data. Failed drills retain the verified snapshot and retry that snapshot rather than creating repeated copies.

```sh
strata-admin operations-once
strata-admin restore-drill backups/snapshot-UUID
```

Keep backups confidential and copy them to a separate failure domain with your approved backup tooling. A volume on the same computer helps with logical recovery; it does not protect against loss of that computer or disk. No external backup destination has been supplied for this environment.

## Status and alerts

Platform administrators inspect `/operations`, `strata operations`, SDK `Client.operations()` or the web **Workers → Supervised operations** section. Failures expose an exception class rather than connection strings or secrets. Metrics report live workers, overdue active leases, oldest waiting work, operation success/age and webhook delivery state.

Prometheus loads actionable rules for coordinator unavailability, no live execution capacity, unrecovered leases, failed operations, overdue backups and exhausted webhook deliveries. The default backup-age threshold is 36 hours; adjust it if your backup interval differs. Alert rules evaluate locally. Configure an approved Alertmanager route to send notifications; no email, Slack or other third-party receiver has been configured or contacted.

## Evidence

Real PostgreSQL integration tests created, verified and restored snapshots into disposable databases, retained the last good backup after failure, rotated verified predecessors, coalesced concurrent runners and recovered interrupted status. A real Docker test preserved active containers and another deployment, then removed an expired orphan without restarting its worker. Process-exit tests also demonstrate release of staging locks after an unclean shutdown. These are local recovery checks, not measured multi-host availability.
