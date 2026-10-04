# Deployment and capacity

## Local operation

The default Compose stack binds client, database, telemetry and worker-control ports to loopback. Open the workspace at `http://localhost:8000/`. This is local development mode with an open REST API and a development worker token; keep that configuration local.

Jobs request CPU and memory; workers advertise logical budgets. Multiple workers on one physical host must share a total budget that leaves resources for Docker, PostgreSQL, the coordinator and the operating system. Each worker's reservations are enforced independently. Strata does not discover that several worker registrations represent the same hardware and does not prevent an administrator from advertising duplicate capacity.

## Trusted network deployment

Use a dedicated coordinator host and Docker execution hosts. Register one appropriately sized worker per physical host, or partition the host budget explicitly. Both agent implementations connect outbound to the coordinator; the coordinator does not open connections to worker machines.

1. Build and publish the control-plane/worker images to your own registry. Record immutable image digests. Prepare each workload image and preload the same allowlisted digests on every execution host. `docker compose up` in the development repository builds local images; the remote worker file intentionally does not build or pull arbitrary workload images on demand.
2. Provision PostgreSQL and durable blob storage with monitoring and backups. Choose the [filesystem or S3-compatible backend](object-storage.md). API/RPC replicas must share the same durable objects; separate coordinator filesystems do not provide shared storage. Follow [the replica deployment guide](high-availability.md) for its storage and database requirements.
3. Create strong URL-safe database and worker credentials. Configure [individual accounts and projects](identity-and-projects.md) for a shared workspace, or API role keys for the documented legacy trusted deployment. Keep secrets in protected configuration outside Git. The database password variable only initializes a new PostgreSQL volume; it does not rotate an existing database's password. Use a fresh dedicated deployment or explicitly rotate the database role through PostgreSQL administration.
4. Issue an RPC server certificate whose SANs include the coordinator hostname and `rpc` (for local Compose workers). Supply its certificate/key to the coordinator and only the trusted CA certificate to workers. Python and C++ use verified TLS; hostname verification stays enabled.
5. Configure HTTPS for REST. `deploy/Caddyfile` is a reverse-proxy template for Caddy on the coordinator host, forwarding to loopback port 8000. Configure `STRATA_DOMAIN` and your normal certificate/DNS process. The proxy hides operational `/metrics` and internal tick endpoints. Keep Grafana, Prometheus, Jaeger and PostgreSQL private.
6. Restrict TCP 50052 to the trusted worker network. Configure the coordinator with `deploy/tls.compose.yml` and the workers with `deploy/worker.compose.yml`. These templates are saved for review; no public service or cloud account has been created by this expansion.

Coordinator variables for the TLS override:

```text
STRATA_DATABASE_PASSWORD=strong_url_safe_secret
STRATA_WORKER_TOKEN=strong_secret_at_least_32_characters
STRATA_API_KEYS={"secret_at_least_24_characters":"admin","another_secret_at_least_24_characters":"viewer"}
STRATA_ALLOWED_IMAGES=["registry.example.org/project/workload@sha256:IMMUTABLE_DIGEST"]
STRATA_RPC_LISTEN_IP=coordinator_private_network_ip
STRATA_TLS_CERT_FILE=absolute_path_to_rpc_certificate.pem
STRATA_TLS_KEY_FILE=absolute_path_to_rpc_private_key.pem
STRATA_RPC_CA_FILE=absolute_path_to_trusted_ca.pem
```

```bash
docker compose -f docker-compose.yml -f deploy/tls.compose.yml config --quiet
docker compose -f docker-compose.yml -f deploy/tls.compose.yml up --build -d
```

Compose merges mappings such as environment variables and adds distinct volume/port entries. Review the final configuration before use. A second local stack needs explicit port overrides; setting a different project name alone isolates names/volumes but does not change published ports. See the [official Compose merge rules](https://docs.docker.com/reference/compose-file/merge/).

Each execution host uses:

```text
STRATA_WORKER_IMAGE=registry.example.org/project/worker@sha256:IMMUTABLE_DIGEST
STRATA_STORAGE_KEEPER_IMAGE=registry.example.org/project/control-plane@sha256:REVIEWED_DIGEST
STRATA_RPC_TARGET=coordinator_hostname:50052
STRATA_WORKER_ID=unique_host_worker_id
STRATA_WORKER_TOKEN=the_private_worker_token
STRATA_ALLOWED_IMAGES=["registry.example.org/project/workload@sha256:IMMUTABLE_DIGEST"]
STRATA_WORKER_CPU=host_budget_in_cores
STRATA_WORKER_MEMORY_MB=host_budget_in_mebibytes
STRATA_RPC_CA_FILE=absolute_path_to_trusted_ca.pem
```

```bash
docker compose -f deploy/worker.compose.yml config --quiet
docker compose -f deploy/worker.compose.yml up -d
```

Choose the published Python or C++ worker image. The remote template specifies `STRATA_WORKER_COMMAND`, defaulting to `strata-worker`; set it to `strata-worker-cpp` when choosing the C++ image. The template sets that executable as the entrypoint. A workload can use either agent; language and agent implementation are independent.

## Maintenance and recovery

Drain a worker before planned maintenance. Existing executions finish, while the scheduler routes new jobs elsewhere. Stop the drained worker only once its active job count is zero. A replacement must use a unique ID or wait for the previous session to expire. Changing credentials requires restarting the affected service.

`strata-admin backup`, `verify-backup`, `restore` and `storage-audit` are documented in [the workspace guide](platform.md). Test restores on a separate empty database, not on a live coordinator. Keep backups off the execution host and protect them as private data. The restore command refuses existing data. Configure [supervised backups/restore drills](continuous-operations.md) and the separate [replica/database recovery deployment](high-availability.md) when required.

## Finding the useful limit

Measure end-to-end completion time separately from kernel time and input staging. Vary case size and concurrency, record retries/failures and compare with an equivalent local baseline. Tiny jobs often spend more time in container lifecycle and coordination than in computation. Group small operations into a larger job before adding workers.

Monitor queue depth, worker reservations, CPU/RAM, database latency and disk usage. A host that is short of disk cannot safely sustain larger datasets, repeated image builds or backups. The local expansion hit an exhausted Windows C: drive; only Strata-generated download/cache/temp records were considered for cleanup. Capacity includes storage and network, not only cores.

Increase outstanding job admission, scheduler batch size or agent concurrency only after measuring them. The default outstanding job cap is 10,000, campaign cap 10,000 and per-worker container cap 16. These are configured bounds, not benchmarked capacity guarantees. Multiple schedulers coordinate through PostgreSQL row locks; the scheduling batch queries eligible worker capacity once and updates its reservations in memory within the same transaction.

`scripts/network_probe.py` starts disposable Docker-in-Docker daemons to test independent execution stores and TLS for both agents. It requires privileged local test containers, Docker, developer dependencies and enough free disk; it does not establish physical multi-host speedup. The script captures diagnostics and cleans up only the containers/volumes/network it created. Physical network latency, partitions between actual machines, sustained load and long-running operational availability still require a real deployment.

The probe preloads both the workload and output-keeper images into each independent daemon. Its 0.5-CPU job leaves capacity for the separately reserved keeper on each 1-CPU worker. Defaults target the local `strata` Compose deployment. For a disposable fixture, set `STRATA_API_URL`, a container-reachable `STRATA_DATABASE_URL`, `STRATA_PROBE_NETWORK` and `STRATA_PROBE_ARTIFACT_VOLUME`; optional `STRATA_TEST_CONTROL_IMAGE`, `STRATA_TEST_CPP_IMAGE` and `STRATA_STORAGE_KEEPER_IMAGE` select preloaded runtime images. The parent network, migrated database and shared artifact volume must already exist and belong to that fixture.
