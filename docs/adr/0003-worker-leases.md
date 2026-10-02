# ADR 0003: Worker sessions and attempt leases

## Context
Heartbeats prove a worker is alive, but do not establish continuing ownership of each assigned computation. Reusing a worker ID after a restart can otherwise accept old reports.

## Decision
Give each registration a fresh session and each attempt a fresh token. Use PostgreSQL time for server expiry and conservative monotonic deadlines for agent watchdogs. Renew individual active attempts; never revive expired ownership.

## Alternatives
Heartbeat-only recovery delays or misclassifies per-task failures. Client wall-clock deadlines are vulnerable to clock skew. Never-expiring reservations leak capacity indefinitely.

## Consequences
Stale completion is rejected and capacity is recovered transactionally. Local stopping is best effort and requires a live agent and responding Docker daemon. A dead agent's orphan process may overlap the replacement.
