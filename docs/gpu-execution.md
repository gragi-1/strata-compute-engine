# NVIDIA GPU execution

Both agents discover NVIDIA devices through an explicitly configured, allowlisted probe image. The coordinator reserves whole devices by stable NVIDIA UUID in the same transaction as the attempt and CPU/RAM reservation. Two schedulers cannot reserve the same device. A second live agent cannot advertise a physical device already owned by another agent.

## Configure one owner per host GPU

Upgrade through migration `0013` and rebuild both agent images. Install the supported NVIDIA driver and container runtime on each execution host. Docker Desktop on Windows requires its supported WSL 2 NVIDIA GPU configuration; see the [Docker GPU guide](https://docs.docker.com/desktop/features/gpu/). Linux hosts require the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).

Build the small CUDA verification workload from the repository root:

```sh
docker build -f workloads/gpu/Dockerfile -t strata/gpu-smoke:local .
```

Add that image to `STRATA_ALLOWED_IMAGES` on the coordinator and agent. Set `STRATA_GPU_DISCOVERY_IMAGE=strata/gpu-smoke:local` only on the agent that owns the GPU. Compose exposes separate `STRATA_WORKER_1_GPU_DISCOVERY_IMAGE`, `STRATA_WORKER_2_GPU_DISCOVERY_IMAGE` and `STRATA_CPP_GPU_DISCOVERY_IMAGE` variables for that reason. Discovery overrides the image entrypoint with `nvidia-smi`, so workload-specific entrypoints do not interfere. Production deployments should use approved images pinned by digest.

The worker inventory exposes UUID, model, total memory, enabled state and current attempt through `GET /workers/{id}/gpus`, `strata worker-gpus WORKER_ID`, SDK `Client.worker_gpus()` and the web worker GPU button. Project administration includes a concurrent GPU quota; zero disables GPU scheduling for that project.

## Submit a CUDA computation

```yaml
name: cuda-verification
image: strata/gpu-smoke:local
command: ["1024", "0"]
resources: { cpu: 0.5, memory_mb: 256, gpus: 1, gpu_memory_mb: 1024 }
timeout_seconds: 30
max_retries: 0
```

Submit the file with `strata submit FILE`. The example launches a CUDA kernel that squares 1024 floating-point values, copies the result back, verifies every value and saves `/output/cuda-result.json` as a downloadable artifact. Python calls the CUDA Driver API; the numerical kernel runs on the GPU. The fixed PTX kernel targets Turing or newer NVIDIA devices (`sm_75`). No CUDA toolkit or CPU fallback is included. Its second argument holds the completed container for zero to five seconds, useful when observing allocation. This is a hardware smoke computation, not a Strata throughput benchmark.

`gpus` requests an integer number of exclusive devices. `gpu_memory_mb` is a minimum total memory requirement **per device**, not a VRAM usage limit. A job requesting more devices or memory than available remains queued and does not block suitable CPU jobs. Scheduling also enforces the project's aggregate CPU, RAM and GPU budgets.

## Isolation, completion and recovery

The agents pass only the assigned UUIDs through Docker device requests. CPU-only workloads explicitly receive `NVIDIA_VISIBLE_DEVICES=void`, including CUDA-based images. Device UUIDs are recorded in attempt provenance. Normal completion, cancellation, timeout, lease expiry and worker recovery release the reservation using the attempt owner; a stale report cannot release another attempt's device.

Strata owns scheduling within one deployment. Desktop applications, other containers and another Strata deployment on the same host remain outside its reservation system. Do not advertise the same physical GPU to independent deployments. No fractional sharing, MIG partitioning, GPU memory enforcement or preemptive device checkpointing is implemented by this whole-device contract. Containers and the Docker daemon are trusted execution infrastructure; see [security boundaries](security.md).

## Local evidence

Actual Docker execution on an NVIDIA GeForce RTX 3050 Laptop GPU with 4096 MiB passed for both Python and C++ agents. Each ran two CUDA jobs in exclusive sequence, recorded the device in provenance, transferred the smoke computation output, freed the reservation and denied GPU access to a CPU-only job using the same image. Four allocation/API tests additionally covered PostgreSQL scheduler contention, oversized requests, duplicate device registration, recovery ownership transfer, project quotas and private resource access. The final C++ image compiled with warnings treated as errors and passed four CTest checks, including Docker start reconciliation.

Run `tests/e2e/test_gpu_runtime.py` with `STRATA_TEST_GPU_IMAGE` set to the built smoke image and `STRATA_TEST_CPP_IMAGE` set to the test agent image. This opt-in test requires actual GPU hardware; ordinary CI does not establish GPU runtime support. Physical multi-host and multi-GPU measurements remain pending.
