# Coordinator and database recovery

Strata can run supervised coordinator replicas against one shared PostgreSQL writer endpoint and one shared S3 bucket. The replica command is `strata-coordinator`; it runs the API, RPC service and scheduler in one process. Durable jobs, sessions, leases and reservations remain in PostgreSQL. A replica's local S3 cache is disposable.

## Coordinator replicas

Upgrade the database to the current migration head once, bootstrap an individual administrator, and configure approved immutable workload images before starting replicas. Use `deploy/replicas.compose.yml` with immutable `STRATA_COORDINATOR_IMAGE` and `STRATA_HA_GATEWAY_IMAGE` references. Build the gateway from `deploy/ha/gateway.Dockerfile`; its pinned upstream base receives available Alpine security updates before scanning. Do not deploy a base image that fails the security gate.

The template requires PostgreSQL `sslmode=verify-full`, a mounted database CA, verified HTTPS S3 storage, a strong worker token and RPC TLS. Both RPC certificates must identify the stable gateway hostname used by workers. Configure `STRATA_RPC_CA` on both worker implementations. Each replica has its own bounded cache volume. Configure the same OIDC issuer/client/state key on every replica if federated sign-in is enabled.

The template exposes only private loopback API/RPC ports, 58010/58011 by default. Put the API behind your trusted HTTPS ingress before exposing it to users. RPC TLS passes through the gateway. API health checks use `/ready`; RPC backends use the same supervised readiness signal. A replica is ready only while all components are alive, scheduling has recently succeeded, the database session permits writes, the schema is readable and identity bootstrap is complete. A readable standby or read-only session is not ready. `/health` alone does not establish readiness.

HAProxy stops using an unhealthy replica and closes its old RPC sessions. Workers reconnect with the same durable session and lease credentials. RPC connection attempts allow time for certificate negotiation and use bounded reconnection backoff. A process restart after the lease expires still requires normal fenced recovery; replication does not remove the at-least-once execution contract.

For the PostgreSQL failover policy below, the replica template uses a three-second heartbeat, 60-second worker liveness and 90-second lease. These windows must exceed your measured database recovery and network delay, with margin. Larger windows also delay detection and cleanup of a failed worker. A partition lasting beyond the lease must stop execution; never lengthen a stale lease locally.

## Retry and ambiguous responses

The SDK, CLI and web client retry connection failures and HTTP 429/502/503/504 only for replayable operations. Defaults permit three retries, at most four attempts, with jitter and `Retry-After`. A ten-second window limits when another retry can start; each request still has its own HTTP timeout, so this is not a ten-second total deadline. SDK callers can set `max_retries=0` or `retry_window_seconds` explicitly.

Replayable operations include reads, dataset queries/statistics, verified upload chunks and upload sealing. Job, campaign, workflow and experiment submissions require an `Idempotency-Key`. Reuse the same key and body after a lost response. Reusing a key with another body produces a conflict. An unkeyed create, login, account/project change or one-shot file stream is never automatically replayed. Web submission forms retain their key after an error while that editor remains open. The proxy does not replay mutations.

A database failure returns generic HTTP 503 with `Retry-After: 2` or RPC `UNAVAILABLE`; logs contain the exception class and a validated SQLSTATE code, without SQL parameters or connection secrets. PostgreSQL connections default to `target_session_attrs=read-write`, a five-second connection timeout and a ten-second pool checkout timeout; explicit libpq URL settings take precedence. Readiness still rejects a read-only session when an operator overrides the connection selector. These limits do not bound every established-session query. The database lab additionally configures statement and TCP keepalive limits. A sufficiently long outage must be surfaced to the caller instead of retried forever.

## Private PostgreSQL failure lab

`deploy/ha/postgres.compose.yml` runs three PostgreSQL 17.11/Patroni 4.1.5 nodes, three etcd 3.6.15 members and a writer gateway. It publishes only the writer's loopback port, 55440 by default. All replication/client database sessions use TLS; DCS peers/clients and Patroni health probes require client certificates. The application role has no superuser, role creation, database creation or replication privilege. Restore drills requiring `CREATEDB` need a separately controlled administrative role.

Build the two infrastructure images and generate disposable seven-day lab credentials:

```sh
docker build -f deploy/ha/Dockerfile -t strata/postgres-ha:local .
docker build -f deploy/ha/gateway.Dockerfile -t strata/ha-gateway:local .
python -m scripts.ha_lab_credentials --output data/ha-lab
```

Set `STRATA_POSTGRES_HA_IMAGE` and `STRATA_HA_GATEWAY_IMAGE` to the corresponding reviewed image digests, and `STRATA_HA_CREDENTIAL_ROOT` to the absolute generated directory. Then inspect and start this specific deployment:

```sh
docker compose -f deploy/ha/postgres.compose.yml config --quiet
docker compose -f deploy/ha/postgres.compose.yml up -d
```

The generator refuses to overwrite existing credentials and never prints passwords. Keep its root private to the operator. On POSIX it uses mode 0700 for the root, with readable children for the unprivileged container UIDs; mounted PostgreSQL private keys are copied to mode 0600 before use. On Windows, enforce the corresponding NTFS access rules. The generated CA's private key is not retained. Certificates expire after seven days: replace the entire disposable lab rather than treating these credentials as a production certificate authority.

The application password is in `secrets.json`, under `application`. The database is `strata`, the user is `strata`, and the verified client CA is `tls/gateway/ca.pem`. Set the client's PostgreSQL writer URL with `sslmode=verify-full`, `sslrootcert`, bounded `connect_timeout` and an appropriate query timeout. Keep the password in a private configuration file or secret manager, rather than terminal history. A coordinator container uses its mounted `/run/strata/db-ca.pem` CA path and a reachable stable writer hostname.

The database image builds PostgreSQL from its official source archive, verifies its SHA-256, runs the upstream regression suite as an unprivileged user and retains its license/component inventory. It supports TLS, JSON, relational queries, LZ4 and Zstd. XML/XSLT, ICU collations, LLVM JIT and server-side Python/Perl/Tcl are not compiled. This avoids importing unnecessary vulnerable XML and privilege-switching binaries. Patroni's separate Python dependency file is pinned with hashes. Rescan rebuilt images because OS security updates intentionally change their resulting digest.

## Failure policy and boundaries

Patroni uses strict synchronous replication to one standby, timeline checks, checksummed data, rewind support, a 20-second DCS lease and a two-second control loop. The writer gateway accepts only a node whose authenticated `/primary` probe succeeds. On loss of DCS quorum, the primary relinquishes write authority; readiness and writes fail closed until authority is recovered. Stopped nodes retain their own durable volumes and can rejoin as replicas.

Acknowledged synchronous commits and ambiguous transactions have different guarantees. A cancelled database backend can expose a transaction before replication acknowledgement; a client losing the response must use Strata's idempotency contract. See [Patroni's replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html) and [configuration](https://patroni.readthedocs.io/en/latest/yaml_configuration.html). Do not promise universal zero data loss or exactly-once external effects.

This Compose lab shares one physical host, one Docker daemon and one writer gateway. Its watchdog is explicitly disabled. It validates process loss and quorum behavior, not host, disk, gateway or geographic availability. Production requires separate failure domains, redundant ingress/writer routing, durable replicated storage, private DCS access, certificate/credential rotation and a reviewed fencing/watchdog mechanism. Configure backup retention and restore drills independently: replicas are not backups. Mixed-version schema upgrades still follow the maintenance procedure in [distribution and upgrades](distribution-and-upgrades.md).

## Reproduce the live checks

With the reviewed images preloaded, set `STRATA_TEST_DOCKER_RUNTIME=1`, `STRATA_TEST_POSTGRES_HA=1`, the worker/control/workload/gateway/database image variables, `STRATA_TEST_POSTGRES_URL` for an isolated fixture server, and `STRATA_TEST_S3_ENDPOINT` for the private S3 fixture. `STRATA_TEST_HA_EVIDENCE` selects an ignored evidence directory.

```sh
pytest tests/e2e/test_coordinator_replicas.py tests/e2e/test_postgres_failover.py -q
pytest tests/integration/test_store_outages.py tests/integration/test_coordinator.py tests/unit/test_sdk.py -q
```

Replica checks kill one coordinator during actual Python/C++ execution, reuse the submission key, verify one attempt and released reservations, download the verified S3 output and read it through both replicas after restart. Database checks kill the actual primary during container execution, verify committed state and application recovery, rejoin the old primary, remove DCS quorum, reject writes and resume after quorum restoration. Tests create uniquely labelled private resources and remove only those resources. CI retains recovery timing JSON and test reports; pushed GitHub execution remains separate from local evidence.

On 2026-10-04, the integrated local run passed 231 tests with 90.22% coordinator/scheduler coverage, including all Docker, GPU, S3, PostgreSQL and failover opt-ins. That run observed a first successful API response about 3.47 seconds after coordinator loss on either worker path, PostgreSQL primary recovery in 25.04 seconds, and write-authority withdrawal 8.82 seconds after DCS quorum loss. These are individual measurements on one physical host, not deployment SLOs. The run also rejected plaintext database access with valid application credentials, invalid server trust, unauthenticated DCS/Patroni probes and read-only readiness.
