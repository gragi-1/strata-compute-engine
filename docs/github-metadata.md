# GitHub repository metadata

Repository: [gragi-1/strata-compute-engine](https://github.com/gragi-1/strata-compute-engine).

Repository name: `strata-compute-engine`.

Description:

```text
Self-hosted compute workspace for containerized CPU/GPU workloads, datasets, experiments, workflows and notebooks, with durable scheduling, Python/C++ workers and verified results.
```

Topics:

```text
distributed-systems distributed-computing python cpp fastapi postgresql docker
scheduler fault-tolerance observability systems-programming backend grpc scientific-computing workflows datasets
```

## CI status

[CI run 37030111939](https://github.com/gragi-1/strata-compute-engine/actions/runs/37030111939) passed on **2026-10-02** for commit [9d6874d](https://github.com/gragi-1/strata-compute-engine/commit/9d6874da7ef88601f952c1bb4d475913edb14685). All three jobs (`python`, `cpp` and `docker`) completed successfully. See [validation evidence](validation.md) for the checks performed and their scope.

The v2 workspace passed [CI run 37118048929](https://github.com/gragi-1/strata-compute-engine/actions/runs/37118048929) on **2026-10-03** for commit [f7a518c](https://github.com/gragi-1/strata-compute-engine/commit/f7a518c63a179ff9fe3478360a5ebdd9ba602ce6). All three jobs (`python`, `cpp` and `docker`) completed successfully. See [v2 validation evidence](validation-v2.md) for the job links and scope.

The initial published release is [v1.0.0](https://github.com/gragi-1/strata-compute-engine/releases/tag/v1.0.0). The v2 workspace has [prepared notes](releases/v2.0.0.md); consult [release history](https://github.com/gragi-1/strata-compute-engine/releases) for published versions. Follow the [publication procedure](publication.md) to publish only a tested commit. The workflow badge reports the live status of pushed commits.

The README can use this badge, which reports the workflow's current status:

```markdown
[![CI](https://github.com/gragi-1/strata-compute-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/gragi-1/strata-compute-engine/actions/workflows/ci.yml)
```

Keep repository descriptions, release titles, release notes and documentation in English.

## Prepared v3.0.0 release

The self-hosted release is prepared with package/native version `3.0.0` and intended tag `v3.0.0`; it has not been published by this preparation. Its [local validation](validation-v3.md) is separate from the historical CI runs above. Use [the v3 publication procedure](publishing-v3.md) and [v3.0.0 release notes](release-notes-v3.md). Cloud provider provisioning, physical cluster qualification and independent production/security acceptance remain explicitly deferred requirements.

The owner pushed [commit d5dd5ee](https://github.com/gragi-1/strata-compute-engine/commit/d5dd5eecbea5b590bc006e70012a7b28bf555288). Its first [v3 CI run 37214310570](https://github.com/gragi-1/strata-compute-engine/actions/runs/37214310570) exposed two failures: Linux type checking of Windows-only file locking, and a root-owned scanner SBOM that the runner could not update. The `cpp`, `browser`, `package-3.12` and `package-3.13` jobs passed; `docker` was still running at the last inspection on 2026-10-04. The platform guards and report ownership have been corrected locally, and both image workflows now inspect the exact pinned etcd reference. The corrected source still needs an owner commit/push and all seven required jobs to pass on that exact commit. No successful v3 CI or tag is claimed here.
