# Distribution and upgrades

The prepared self-hosted release uses package version `3.0.0` and the matching tag `v3.0.0`. The version change does not publish a release or approve a production deployment. Historical v1/v2 tags and the `3.0.0.dev0` local qualification remain unchanged. Package metadata also supplies the OpenAPI version, avoiding a stale hardcoded API version.

The agreed pre-push delivery is the locally qualified self-hosted expansion. Follow [the v3 publication procedure](publishing-v3.md) to commit, validate the exact source on GitHub and prepare candidate artifacts. A source push does not itself approve production deployment or create a stable release.

## Build and install verified packages

The Python wheel contains the API, browser workspace, schedulers, Python agent, CLI, SDK and all database migrations. Hatchling and uv versions are fixed; `uv.lock` pins runtime dependencies and hashes. The release export includes the optional scientific analysis dependencies.

```sh
export SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
uv build --out-dir build/dist
uv export --locked --no-dev --extra analysis --no-emit-project --quiet -o build/runtime-requirements.txt
uv venv .venv-install
uv pip install --python .venv-install/bin/python --require-hashes -r build/runtime-requirements.txt
uv pip install --python .venv-install/bin/python --no-deps build/dist/strata_compute_engine-3.0.0-py3-none-any.whl
```

On Windows, use `.venv-install/Scripts/python.exe`. Activate the environment before invoking the installed commands. These are locally built files; no Python package has been published to PyPI by this work. Installing a wheel directly without the exported requirements allows dependency resolution within its declared ranges, rather than reproducing the tested lock.

`strata-admin migrate` upgrades the configured PostgreSQL database using the migrations packaged in the installation. It works outside the source checkout; `strata-admin migrate --check` checks model/schema agreement. PostgreSQL transaction advisory locks serialize independent migration administrators on one schema. A failed upgrade remains a failure; it does not start the application automatically. SQLite remains a single-process test backend and has no production migration support.

## Upgrade an existing deployment

1. Verify the candidate's source commit, checksums, successful CI and security reports. Preserve the currently deployed package/images.
2. Use [cluster admission controls](cluster-maintenance.md) to disable new submissions while draining queued work. Pause assignments once the queue is drained, pause provisioning, drain workers and stop schedulers/events/operations. Wait for attempts and host reapers to finish cleanup, or explicitly recover expired attempts. Maintenance automatically defers periodic occurrences and dynamic expansion without discarding them.
3. Create a verified PostgreSQL/blob backup and complete a restore drill. Test the upgrade against a restored copy first.
4. Install matching coordinator, Python/C++ agent and client versions. Run `strata-admin migrate`, then `strata-admin migrate --check` with the intended database and storage configuration.
5. For legacy unowned v2 resources, follow [identity adoption](identity-and-projects.md). For pre-cluster Docker labels, follow [orphan migration](continuous-operations.md). Configure whole-device GPU ownership using [GPU execution](gpu-execution.md). Preload the reviewed immutable coordinator image for each worker's bounded output keeper and verify Linux tmpfs `noswap` support. Migration `0016` preserves earlier reservation charges; new helper charges require the matching upgraded scheduler and agents. Migration `0017` adds [managed runtimes](managed-runtimes.md); install matching workers before admitting `runtime-bridge` jobs.
6. Start API/RPC, then schedulers, agents and supervised services. Verify readiness, native sign-in, project isolation and a real workload before reopening submissions.

This release procedure requires a maintenance window. Automatic rolling upgrades across arbitrary versions and database downgrade rollback are not promised. To revert an unsuccessful migration, restore the verified database/blob snapshot into a clean deployment of the preserved version. Never run an older coordinator against an unverified newer schema.

## Compatibility policy

Strata uses semantic versions for stable package releases. A new major version can change authorization, deployment and migration requirements. Additive REST response fields and additive Protobuf fields are intended to preserve existing fields and field numbers; do not renumber or reuse Protobuf fields. New runtime capabilities require matching upgraded workers. Run the whole cluster on the same released version unless a specific mixed-version path has been validated.

Local installation has passed on Python 3.13/Windows with PostgreSQL 17. CI adds clean Python 3.12 and 3.13 Linux installations. Other Python releases and architectures need their own evidence; declaring a minimum Python version does not establish runtime verification on every future interpreter. Linux Docker workload execution remains required by both agents.

## Signed candidate artifacts

The manual **Release artifacts** workflow prepares a candidate for the selected version tag, or an unreleased development build from `main`. It requires clean source, an exact tag/package version match and a successful CI run for the same commit, including Python, C++, Docker, browser, both installation jobs and security. It rebuilds reproducible Python archives, validates installation, scans seven Strata runtime/infrastructure images and the pinned external etcd image, records checksums and source revision, and produces versioned Linux AMD64 image archives and SBOMs.

It then requests signed GitHub build provenance attestations for the candidate files. It uploads an Actions artifact and does not create a GitHub Release, publish to PyPI, push container registry tags or change Git refs. The user controls those publication actions. The attestation step must actually run successfully on GitHub before signatures can be claimed; no local file is labelled signed here.

Download the candidate from its successful workflow run, then verify the artifact against the expected repository and workflow with [GitHub attestation verification](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations):

```sh
gh attestation verify PATH_TO_ARTIFACT --repo gragi-1/strata-compute-engine --signer-workflow gragi-1/strata-compute-engine/.github/workflows/release-artifacts.yml
sha256sum -c SHA256SUMS
docker load --input control-plane-VERSION-linux-amd64.tar.gz
```

Verify the downloaded manifest's expected version and source commit, not merely the repository name. Checksums alone do not establish origin. GitHub provenance describes the build; it does not replace runtime tests or vulnerability review. Python archives have demonstrated identical bytes at a fixed `SOURCE_DATE_EPOCH`. Container base digests and Python dependency hashes are pinned, but distribution repositories provide current security updates, so container rebuilds are not claimed byte-identical.

The candidate includes the current operating-system/language component inventory and an explicit inventory/license for the statically linked libarchive and custom PostgreSQL sources. Image archives are versioned and attestable; registry and PyPI publication remain pending user actions.
