# Operations

Readiness checks both database connectivity and the seeded admission row. `/health` checks API process liveness. Inspect `/jobs/{id}/events` and `/jobs/{id}/attempts` before restarting components: errors include worker loss, lease expiry, execution deadline, process exit and retry scheduling.

Prometheus scrapes durable counters from PostgreSQL, so API or scheduler restarts do not reset submitted/completed/retried counts. Histograms aggregate completed attempt runtimes and each attempt's placement delay from eligibility to assignment. Worker utilization is reserved capacity, not measured CPU consumption. Heartbeat snapshots remain in `worker_heartbeats`; retention and maintenance are manual in this release. Avoid unbounded history in long-lived deployments.

To test recovery, kill a worker agent with `docker compose kill worker-1`. Start it again with `docker compose start worker-1`; registration cleans up its labelled orphan containers and output volumes. Restart the scheduler with `docker compose restart scheduler` to verify persistence. Do not delete PostgreSQL's volume to fix a queued workload.

For saturation, inspect queue depth, priority and required capabilities/resources. An oversized or unsupported job remains queued until suitable capacity appears or a client cancels it. Set `STRATA_QUEUE_LIMIT` to a smaller outstanding-job budget and test the explicit 429 responses. Change scheduler batch/interval only with measured lock contention and latency results.

Artifact files are content-addressed, while attempt output volumes are ephemeral after upload. A crash before metadata commit may leave an unreferenced hash file; crashes before cleanup can leave labelled output volumes. Cleanup must compare durable references and live containers before deletion. There is no automatic retention/GC service in this release.

`docker compose down` stops the stack and preserves PostgreSQL/artifact volumes. `docker compose down -v` removes those volumes and permanently deletes this deployment's data. CI uses the latter only for its disposable stack.
