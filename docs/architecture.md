# Architecture

The HTTP API validates requests and delegates to `EngineService`. The same application service implements all gRPC worker operations. Domain transitions and retry/resource policies are independent of the transport; SQLAlchemy models provide infrastructure. The scheduler runs as a separate process, and workers need no database credentials.

PostgreSQL stores jobs, attempts, workers, worker heartbeat snapshots, job events and artifact metadata. An admission row serializes submissions to enforce a cluster-wide outstanding-job cap and idempotency keys. This intentionally trades submission throughput for a simple, exact admission guarantee. Scheduler throughput uses separate job/worker row locks.

Assignment is one transaction: lock queued jobs using `FOR UPDATE SKIP LOCKED`, lock candidate workers, reserve capacity, insert an attempt and change job state. A partial unique index allows only one active attempt per job. Polling returns assignments already committed to that worker session. Starting, cancelling and finishing lock the job before the worker; heartbeat updates its worker separately before renewing individual leases. No path holds a worker lock while acquiring a job lock.

Each terminal attempt releases its reservation exactly once, stores a reason and emits a transition event in the same transaction. Recovery refreshes the attempt after obtaining its job lock, so concurrent recovery loops cannot finalize it twice. Process memory caches are never authoritative. Restarting the API, scheduler or RPC server therefore preserves all decisions that committed before the restart.

Artifacts use SHA-256 filenames on a shared filesystem volume. Uploads write a temporary file and rename it, then commit metadata. A crash can leave an unreferenced file, but cannot overwrite different committed bytes under the same digest. Existing artifact names within an attempt require identical content on retry. The API downloads bytes through UUID metadata rather than accepting filesystem paths. MinIO can replace this store behind the same metadata model later.

Trace context is saved with submission, propagated in assignments and used for RPC reports and execution spans. Execution spans are reconstructed by the control plane from durable start/end times for both agent implementations; Python also emits worker report spans. This is useful correlation, not a claim that every C++ runtime operation is separately instrumented.
