# Storage budgets, caches and retention

Strata preserves scientific data and execution metadata until an administrator explicitly selects a retention operation. The commands below default to a dry run. `--apply` permanently removes the eligible data shown by the operation. Retention is a local administrator CLI path, outside project-facing HTTP access.

```sh
strata-admin history-prune
strata-admin history-prune --apply --limit 1000
strata-admin storage-collect
strata-admin storage-collect --apply --limit 1000
strata-admin cache-prune
strata-admin cache-prune --apply --target-bytes 1073741824
```

`history-prune` removes upload sessions and chunk references only after their expiry plus `STRATA_HISTORY_RETENTION_SECONDS` (30 days by default). It also removes expired credentials, old worker heartbeat samples and log text on terminal attempts of terminal jobs. Datasets, output artifacts, jobs, attempts, job events and audit records remain. Each category has a bounded batch. Run again to process further history. Completed dataset files survive removal of their original transfer history.

`storage-collect` deletes only SHA-256 blob names absent from **all** dataset file, output artifact and upload-part references, and older than `STRATA_BLOB_RETENTION_SECONDS` (one day by default). PostgreSQL schema-scoped shared advisory locks cover blob writes through reference commit and snapshot backups; collection takes the exclusive lock before inspecting references. This closes the gap between storing bytes and committing their metadata. SQLite serializes these operations through its admission row. Backup takes its shared lock before establishing the exported database snapshot.

The S3 local cache evicts least recently used canonical copies before admitting new downloads. `STRATA_STORAGE_CACHE_BYTES` defaults to 4 GiB; larger individual objects require increasing it. Download responses, previews, queries, worker range reads, campaign results and backups pin their materialized files. Evicting a canonical cache name preserves active readers and the authoritative S3 object. Hard links are preferred, with a checked copy fallback. Filesystem blobs are authoritative and `cache-prune` refuses to evict them.

`STRATA_STORAGE_MAX_LOCAL_BYTES` bounds the coordinator's artifact root (20 GiB by default), including temporary uploads, assembly, downloads and pins. `STRATA_STORAGE_MIN_FREE_BYTES` preserves at least 256 MiB of free space. Physical allocations use a shared process lock on the root and return 503 on capacity or lock timeout. Counting hard-linked pins separately is conservative; these are Strata-managed directory budgets rather than operating-system quotas for unrelated applications or Docker volumes.

Both Python and C++ workers cache verified inputs by SHA-256 beneath `STRATA_WORKER_CACHE_ROOT`, partitioned by worker ID hash. `STRATA_WORKER_CACHE_BYTES` defaults to 4 GiB. Hits recheck size and checksum; corrupt entries are fetched again; admission evicts the oldest unused entries; staging holds the process lock. Every assignment still obtains an authorized input manifest and renews/checks its lease. Compose and the remote-worker template persist worker caches in separate volumes.

## Hard workload output limits

Both current agents create each output volume as a kernel-bounded tmpfs, with `noswap`, `nosuid`, `nodev`, `noexec`, UID/GID 65534 and separate byte/inode limits. `STRATA_WORKER_OUTPUT_BYTES` defaults to 64 MiB; `STRATA_WORKER_OUTPUT_INODES` defaults to 4096. Excess allocation fails with `ENOSPC`, including attempts to exhaust space using many empty files. Workloads retain a read-only root and a separately bounded `/tmp`; arbitrary output cannot grow a persistent Docker disk volume. Linux 6.4 or newer with [tmpfs `noswap` support](https://www.kernel.org/doc/html/v6.4/filesystems/tmpfs.html) is required. An unsupported mount fails the assignment rather than silently falling back to an unbounded volume.

Output pages consume memory; this path is intended for bounded result artifacts, not multi-gigabyte disk scratch. They count against the writing container's memory limit and can cause an OOM before the filesystem limit is reached. Configure sufficient job memory. Result transfer remains subject to `STRATA_ARTIFACT_MAX_BYTES` (16 MiB by default; the current C++ transfer ceiling is 16 MiB). A result exceeding the supported transfer limit fails explicitly. Large scientific output needs a separate reviewed persistent quota-backed adapter before raising these ceilings; dataset uploads already support larger durable objects.

Set `STRATA_STORAGE_KEEPER_IMAGE` to the reviewed coordinator image already present on the worker's Docker daemon. Production and remote deployments require its immutable digest. The coordinator image includes a small `worker.storage_keeper` process: it receives no RPC or Docker credentials, has no network, mounts output read-only, and runs with all capabilities dropped, a 32 MiB memory limit and 0.01 CPU. It checks that the retained mount is a non-swappable tmpfs. This independent mount preserves files after the workload exits until collection completes; an ordinary tmpfs mount alone would lose them at container exit.

The agent renews retention only after an accepted coordinator heartbeat, using a Docker signal that workloads cannot send across their separate PID namespaces. Retention expires after the lease window plus termination/collection grace when renewal stops. Loss of the keeper fails the attempt and stops its workload, rather than reporting success with missing files. Process-exit host cleanup still requires the independent [host reaper](continuous-operations.md); a keeper is not an execution lease authority. Reapers and worker cleanup remove both workload and keeper before their labelled volumes, preserving unrelated deployments.

Workers advertise `bounded-output`. The scheduler reserves each such attempt's requested resources plus 0.01 CPU and 32 MiB for retention. Migration `0016` stores these charges on the attempt, preserving exact release after worker replacement, cancellation or recovery. Project execution quotas describe the workload request; worker/pool capacity includes the helper. Pool packing uses the same additional charge. A one-CPU worker cannot accept a one-CPU request plus its helper: leave room within the configured allocation. Upgrade the coordinator and both agents together; earlier workers do not establish this output guarantee.

These limits cover Strata-managed output allocation. They do not impose a daemon-wide quota on image pulls, trusted input staging, unrelated containers or host applications. Plan separate persistent storage capacity for inputs, caches, Docker images, backups and S3. The protected output mount is ephemeral across host/daemon loss; durable artifacts are the verified copies accepted by the coordinator's configured storage backend.

Current checks cover an actual PostgreSQL collector racing an uncommitted upload, explicit/dry-run reference preservation, expired chunk cleanup, concurrent local allocations, real S3 cache eviction with an active reader, remote campaign result materialization, and Python/C++ cache hits, corruption, eviction and failed transfers. The C++ cache test runs with assertions enabled in Release builds.

`strata-admin staging-collect` previews crashed managed temporary lifetimes; `--apply` removes inactive ones after the blob grace period. Kernel-held per-lifetime leases protect active staging and pins independently of old content modification times. Tests simulate process termination without cleanup and preserve an active long-lived assembly. Old unleased temporary names and unknown paths require manual inspection. [Supervised operations](continuous-operations.md) can run this cleanup independently. No automatic scientific dataset or artifact deletion is enabled. Keep verified backups before choosing a destructive retention policy.
