# ADR 0002: At-least-once execution

## Context
A process may finish while its success acknowledgement is lost, and a killed worker can leave an executing container behind.

## Decision
Retry uncertain execution with a new durable attempt. Guarantee fenced metadata reports and idempotent submission, while explicitly permitting duplicated computation.

## Alternatives
Exactly-once external effects need transactional participation by every workload output sink. At-most-once execution risks silently losing useful work after ambiguous failure.

## Consequences
Clients receive reliable attempt history; workload authors must make side effects idempotent. Lease tokens are not an external consensus or fencing service.
