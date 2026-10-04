# Elastic CPU worker pools

`strata-provisioner` manages actual Python or C++ worker containers on one explicitly reserved Docker host partition. It estimates eligible CPU work, creates agents within fixed limits and drains agents before removing them. Migration `0015` stores pool policies and durable provisioning intents. Pools start disabled; only a platform administrator can enable or change their policy.

## Reserve and configure a host

Choose a CPU/RAM partition that is available for this pool, separate from static workers, coordinator services, the provisioner itself and other Docker applications. A pool cannot create physical capacity. The controller rejects partitions larger than the daemon's reported capacity and permits one pool per daemon within the cluster database. Operators remain responsible for reservations across unrelated applications and other clusters sharing that daemon.

The pool hard limit is the smaller of its maximum worker count, its CPU budget divided by `worker CPU + 0.25`, and its memory budget divided by `worker memory + 256 MiB`. The additional resources belong to each agent container; workloads consume the resources it advertises separately. Agents have Docker administrator access and remain trusted infrastructure. Neither socket access nor credentials are passed to workload containers.

Within that advertised allocation, each current attempt also reserves 0.01 CPU and 32 MiB for [bounded output retention](storage-retention.md). Packing includes this charge, so a full-allocation workload needs a larger worker. Set `STRATA_STORAGE_KEEPER_IMAGE` to the reviewed coordinator image digest on the same daemon for either agent type. Output byte/inode limits are part of the pool configuration fingerprint; drain existing workers before changing them.

Supply an immutable image ID or repository digest already present on that Docker daemon. The Python agent is included in the coordinator image; the C++ agent uses the C++ worker image. Mutable tags and arbitrary startup commands are rejected.

```sh
export STRATA_DATABASE_URL='postgresql+psycopg://USER:PASSWORD@PRIVATE_DATABASE/strata'
export STRATA_WORKER_TOKEN='YOUR_PRIVATE_WORKER_TOKEN'
export STRATA_ALLOWED_IMAGES='["YOUR_APPROVED_WORKLOAD_IMAGE"]'
export STRATA_POOL_ID=cpu-host-1
export STRATA_POOL_IMAGE='sha256:YOUR_64_CHARACTER_LOCAL_IMAGE_ID'
export STRATA_STORAGE_KEEPER_IMAGE='sha256:YOUR_REVIEWED_COORDINATOR_IMAGE_ID'
export STRATA_POOL_KIND=python
export STRATA_POOL_RPC_TARGET=private-rpc:50051
export STRATA_POOL_NETWORK=private-strata-network
export STRATA_POOL_CPU_PER_WORKER=1
export STRATA_POOL_MEMORY_PER_WORKER_MB=1024
export STRATA_POOL_HOST_CPU_BUDGET=5
export STRATA_POOL_HOST_MEMORY_BUDGET_MB=5120
export STRATA_POOL_MAXIMUM_WORKERS=4
export STRATA_POOL_RPC_CA=/run/strata/ca.pem
strata-provisioner
```

Use the actual 64-digit image hash. Set `STRATA_POOL_KIND=cpp` for the C++ image. Configure a Docker network that can reach the private coordinator RPC endpoint. With production settings, a trusted CA is mandatory and host networking is rejected. When the controller itself runs in a container, the CA path supplied for an agent mount must refer to the Docker host's path, which must also be readable by the controller. Certificate secrets and worker tokens belong outside Git.

The optional local Compose profile is `docker compose --profile elastic up -d provisioner`. Set `STRATA_POOL_IMAGE` first. Its default reserved partition fits two agents, each advertising 0.5 CPU / 256 MiB, plus their overhead. The profile does not subtract that partition from the three default static workers: adjust the static configuration or use a separate host before enabling the pool. This template configures a host adapter; it is not evidence of spare capacity on an arbitrary laptop.

## Set the policy

In **Workers → Elastic worker pools**, inspect controller heartbeat, desired count, errors, allocation and the latest provisioning history. **Edit pool** sets enabled, minimum and maximum values; maximum cannot exceed the controller's hard limit. The CLI and SDK provide the same controls:

```sh
strata pools
strata set-pool cpu-host-1 true 0 4
strata pool-workers cpu-host-1
strata set-pool cpu-host-1 false 0 4
```

```python
with Client(access_token=admin_session) as client:
    client.update_worker_pool("cpu-host-1", enabled=True, minimum=0, maximum=4)
```

Disabling a pool drains all of its agents. Reducing maximum drains excess agents, including occupied agents, without cancelling their attempts. Idle scale-down waits `STRATA_POOL_IDLE_SECONDS` (default 120). An unavailable or unregistered agent is retired after the bounded startup interval; active attempts must first finish or be recovered by the scheduler. A global [assignment pause](cluster-maintenance.md) prevents further agent starts and new provisioning intents.

The planner examines at most 1,000 eligible queued jobs per pass, checks dependency conditions, allowlists, agent capabilities, project execution budgets and per-agent resource fit, and packs CPU/RAM requests. It creates at most two new intents per pass. CPU jobs that require a GPU, unsupported capability or oversized allocation do not cause unusable agents to be created. Project fairness and every actual reservation remain the scheduler's responsibility. Other static pools can also take the queued jobs; the estimate may temporarily provision spare capacity within its hard bounds.

## Recovery and removal contract

The database intent is committed before Docker create. Deterministic container names, immutable image identity and exact cluster/pool/worker labels allow another controller to adopt a creation interrupted by process exit. PostgreSQL session advisory locks permit one controller leader per pool and are released when its connection closes. A failed database operation prevents external provisioning; a failed Docker operation leaves its charged intent intact and retries after 30 seconds. Only exception classes enter public error records.

Removal first commits `DRAINING`. Registration preserves that state, and the REST resume action cannot override an agent being retired. The controller verifies no active attempts while holding the worker and admission fences, then removes only its own labelled agent and cache. A removal failure keeps the durable drain and slot charge; it cannot reopen the worker to placement or create replacement capacity beyond the hard limit. Retired worker IDs cannot register again. Workload containers and outputs remain subject to their execution leases and independent host reapers.

Changing a live pool's image, host, RPC/network configuration or allocation is rejected while it has live intents. Disable and drain it first; a changed configuration registers disabled and needs an explicit policy update. Drain before rotating worker credentials. Stopping the controller alone leaves existing agents running, allowing another controller to resume reconciliation.

Monitoring exposes enabled-controller heartbeat age, reconciliation failures and intent phase counts. Alert rules identify a missing controller or persistent provisioning failure. Configure an actual external alert receiver separately.

## Evidence and current scope

Real PostgreSQL/Docker tests on one physical Windows host have covered both worker types executing four CPU jobs across two provisioned agents, paused-assignment protection, occupied-agent drain/removal, interrupted-create adoption, abrupt controller process exit, concurrent-controller leadership, failed removal and preservation of a foreign container. API tests cover platform authorization, policy bounds, audit, registration shape and retired-worker refusal.

The adapter allocates Docker workers on already provisioned hosts. It does not purchase cloud instances, provision GPUs, manage Kubernetes nodes or establish multi-host throughput. Monetary billing limits and provider lifecycle tests need a selected provider, credentials and an approved budget. Hardware/GPU capacity and physical multi-host evidence remain separate acceptance items.
