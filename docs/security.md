# Security boundaries

Strata executes only images on the API and worker allowlists. Bundled workloads run as UID/GID 65534 with CPU quotas, hard memory/swap limits, PID limits, dropped capabilities, no-new-privileges, no network, a read-only image filesystem and a bounded `/tmp` tmpfs. Docker provides the actual isolation and accounting; [Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints/) describe those controls.

Workload containers enable Docker's init process to forward termination signals and reap children. This avoids relying on a workload running as PID 1 to handle the default stop signal; see [Docker's init and signal behavior](https://docs.docker.com/reference/cli/docker/container/run/).

The output volume is writable and retained through container exit so artifacts can be uploaded. API/archive limits bound accepted output bytes, but do not enforce a disk quota on that volume. Only trusted allowlisted workload images belong on this local cluster. Mutable local tags are convenient for demos; use immutable digests and restrict image publication for remote deployment.

Agents have the Docker socket, which grants administrator-level daemon access. Workload containers do not receive that socket or host-directory mounts. Agents are trusted infrastructure, not an untrusted tenant boundary. Their shared bearer token is configured by environment; the committed default is explicitly a local development value.

Compose publishes all ports to 127.0.0.1. REST client authentication and transport TLS are outside this local release. Before exposing the system remotely, add authenticated HTTPS/gRPC TLS, rotate the token, protect PostgreSQL/Grafana, and provision host-level CPU, RAM and output-storage budgets. This project does not claim production multitenant isolation.
