# Durable event webhooks

Webhooks notify an approved HTTP receiver when jobs succeed, fail, time out or are cancelled. The terminal transition, its unique event ID and every subscribed delivery are committed in one database transaction. A rolled-back transition cannot emit a webhook. Events created before a subscription are not backfilled.

The platform administrator configures target aliases and separate signing secrets:

```text
STRATA_WEBHOOK_TARGETS={"research-receiver":"https://receiver.example.org/strata/events"}
STRATA_WEBHOOK_SECRETS={"research-receiver":"<random signing secret of at least 32 characters>"}
```

Target URLs are deployment configuration. Project users cannot supply arbitrary URLs, credentials in URLs or redirect destinations. Production requires HTTPS. The administrator must approve the configured endpoint, its network reachability and its intended access to subscribed project events; use egress controls when receivers share a host/network with other services. Environment proxy settings are ignored and redirects are rejected. Target secrets are absent from HTTP responses, database records and delivery payloads. Target changes and secret rotation are deployment operations; update the receiver accordingly.

Project administrators create subscriptions in **Webhooks**, `strata webhooks create webhook.yaml`, `Client.create_webhook()` or `POST /webhooks`:

```yaml
name: Numerical experiment completion
target: research-receiver
events: [JOB_SUCCEEDED, JOB_FAILED, JOB_TIMED_OUT, JOB_CANCELLED]
```

`strata-events` is an independent dispatcher. Docker Compose supervises it in the `events` service. The scheduler does not wait for HTTP delivery. Multiple dispatchers claim rows with PostgreSQL `SKIP LOCKED`, a 30-second lease and a fencing token. A crashed dispatcher's claim is recovered. Current project ownership, enabled account, administrator membership and enabled subscription are checked before sending. Disabling a subscription suppresses future enqueue and makes pending deliveries dead when processed; enabling it does not replay those records automatically.

The payload includes event ID/type/time, project ID, job ID/status/campaign ID and the attempt ID. It excludes commands, credentials, logs, input bytes and output bytes. Requests include:

| Header | Meaning |
| --- | --- |
| `X-Strata-Delivery` | Stable delivery UUID across retries; use it as the deduplication key. |
| `X-Strata-Event` | Stable event UUID, shared by deliveries of the same transition. |
| `X-Strata-Timestamp` | Signature timestamp as Unix seconds, refreshed for each send. |
| `X-Strata-Signature` | `sha256=` plus hex HMAC-SHA256 of `timestamp + "." + raw_body`. |

Receivers must verify the HMAC with a constant-time comparison, enforce a reasonable timestamp window, record delivery IDs durably and return a 2xx status only after accepting the event. Delivery is **at least once**: a lost HTTP response can cause a duplicate even when the original receiver committed successfully. Consumer deduplication provides idempotent processing. Delivery ordering is not guaranteed. Retry signatures can differ while the payload and delivery ID remain identical.

Requests use three-second HTTP connect/read/write timeouts and stream only response headers/status; response bodies are not retained. Transient network errors, HTTP 408/429 and 5xx responses retry with exponential delays capped at one hour. Redirects and other 4xx responses are permanent failures. Eight attempts are allowed by default (`STRATA_WEBHOOK_ATTEMPT_LIMIT`); exhausted/crashed claims become `DEAD`. A project administrator can explicitly retry a dead delivery, adding a new bounded attempt budget and retaining its identity/count. There is a configurable global active-subscription limit.

Inspect history/errors and disable/enable receivers in the web, CLI, SDK or `/webhooks/{id}/deliveries`. Lease credentials are never returned. `strata_webhook_deliveries{status=...}` exposes durable state counts for alerts. Migration `0009` adds the outbox. Integration tests verify a real local HTTP receiver's signatures, transaction rollback, lost-claim recovery, stale-writer fencing, retry exhaustion, private project access and concurrent PostgreSQL dispatchers. No third-party receiver has been configured or contacted for this validation.
