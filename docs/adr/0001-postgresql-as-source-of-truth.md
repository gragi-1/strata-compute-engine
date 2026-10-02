# ADR 0001: PostgreSQL as source of truth

## Context
Queues must survive restarts, and assignment must atomically reserve resources and record attempts.

## Decision
Store all authoritative lifecycle state in PostgreSQL. Use row locks and `SKIP LOCKED` for allocation and recovery, with SQL constraints backing critical invariants.

## Alternatives
An in-memory queue loses decisions on restart. A separate Redis queue requires another atomicity/reconciliation boundary between assignment and durable metadata.

## Consequences
One transactional store makes correctness inspectable. Database availability bounds control-plane availability; admission serialization, event retention and scheduler lock contention must be measured.
