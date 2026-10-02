# Reliability and failure semantics

Execution is at least once. Strata fences *database reports* from expired attempts; it cannot fence external side effects of an orphan process. A partitioned live agent has a monotonic watchdog that stops its containers when its conservative lease expires. A killed agent has no watchdog, so its containers may continue until timeout, exit or cleanup on agent restart. Use idempotent output paths/transactions for side effects outside Strata.

PostgreSQL supplies authoritative server time. Tokens are unique for each attempt and worker registration. Heartbeats cannot revive an expired worker session or expired attempt. On process startup, an agent must successfully register a new session before cleaning up its old labelled containers; a rejected duplicate worker ID must never kill the live owner's workloads. A live agent also stops its in-memory attempts when its session is rejected. Reservations for older sessions remain until the recovery transaction releases them.

## Failure matrix

| Failure | Durable response | Limitation |
|---|---|---|
| API or RPC restart | Existing state remains; clients retry ambiguous requests | Availability pauses during restart |
| Scheduler restart | New process resumes queued and expired attempts | Dispatch pauses until a scheduler returns |
| Worker killed | Mark LOST, fail old attempt, retry after backoff | Old workload may physically continue |
| Worker partition | Local watchdog stops workload; server eventually retries | Clock/message latency and daemon response bound termination |
| Expired lease, fresh worker | Reject renewal/report and recover the attempt | Heartbeat liveness alone does not preserve execution ownership |
| PostgreSQL unavailable | Scheduling and reporting stop; workers expire locally | Database availability is the control-plane availability boundary |
| Nonzero process exit / OOM | Failed attempt, bounded automatic retry | Repeatedly invalid workloads exhaust the budget |
| Container create/start failure or lost acknowledgement | Report FAILED even before start; retry under the job's budget | An uncertain start can still duplicate execution; daemon cleanup is best effort |
| Runtime deadline | TERM, grace, KILL via Docker stop; timed-out attempt | Daemon failure can prevent a successful stop |
| Artifact upload interrupted | Retry identical content/name, then finish | Unreferenced files/volumes need operational garbage collection |

## Cancellation and deadlines

A queued job cancels immediately. A scheduled, unstarted attempt cancels and releases capacity immediately. A running job enters CANCEL_REQUESTED; its next heartbeat tells the agent to stop. A later completion or recovery commits CANCELLED. If success commits first, a subsequent cancel returns the already successful job. Row lock acquisition/commit establishes the winner, not client send time.

Timeout is per attempt, starting from the server's durable start acknowledgement. Worker watchdogs also measure runtime locally. Docker stop sends the image's configured stop signal (SIGTERM for bundled images), waits the grace interval and kills if needed. Server recovery enforces the durable deadline plus grace when a worker does not report. Timeout and heartbeat loss can coincide; one transaction finalizes the attempt, with timeout classification taking precedence when its deadline has elapsed. Cancellation already committed dominates both.

Lease expiry triggers local termination; it does not mean a daemon stop has already completed at that instant. Allow the configured 5-second grace and Docker response time when measuring a stopped container. Bundled execution enables Docker init for signal forwarding. `scripts/partition_probe.py` pauses the live RPC server, checks both implementations' actual container state after 38 seconds, and restores RPC in a `finally` block.

Backoff after automatic failure number n is `min(cap, base * 2^(n-1)) + uniform(0, jitter)`. Defaults: base 2 s, cap 60 s, jitter up to 1 s. `max_retries=3` permits the initial attempt plus three automatic retries. Manual retry is explicit, valid only for FAILED, TIMED_OUT or CANCELLED, and starts a new budget without deleting past attempts. A SUCCEEDED job is immutable.

Job states are formalized in `control_plane/domain.py`; illegal edges raise an error. The tests cover every state pair, inclusive liveness boundaries, stale generation reports, cancel/completion races and simultaneous recoveries.
