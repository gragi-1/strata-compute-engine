# ADR 0004: REST for clients, gRPC for workers

## Context
CLI users benefit from OpenAPI and readable JSON. Python and C++ agents need one precise, versioned protocol for assignment, lease renewal and output reporting.

## Decision
Expose FastAPI REST to clients and the `strata.v1.WorkerControl` Protobuf service to agents. Workers poll committed assignments. Both transports delegate to the same application service.

## Alternatives
All REST is simpler for one language but duplicates client serialization across agents. All gRPC would complicate ordinary CLI/API experimentation. Direct scheduler pushes require discoverable worker addresses and delivery reconciliation.

## Consequences
Transport schemas are generated and independently testable. Polling adds a small latency floor. gRPC limits, authentication metadata and retries need explicit handling; transient transport failure does not imply execution failure.
