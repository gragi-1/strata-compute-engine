# Security verification

Authorization and runtime boundaries are covered by the identity, project, upload, experiment, schedule, webhook, OIDC, RPC, GPU and cleanup tests. The source remains a trusted-compute platform: container isolation does not turn Docker-socket agents into an untrusted-host boundary. See [security boundaries](security.md) and [individual identity](identity-and-projects.md).

## Dependency and image gates

The separate hashed Patroni graph is audited as well. The CI security job exports the complete locked production/analysis Python dependency graph with hashes and audits it using pinned `pip-audit` against the public Python advisory database. A collection/network error or reported Python advisory fails the job; no global vulnerability exclusions are configured. See the [pip-audit security model](https://github.com/pypa/pip-audit#security-model) for what that advisory lookup establishes.

Seven Strata runtime/infrastructure images and the pinned external etcd image are built or obtained and scanned by a digest-pinned Trivy image. `python -m scripts.scan_images` resolves the image's immutable local ID before scanning, retains raw HIGH/CRITICAL findings, generates CycloneDX component inventories and records the scanner/image identities and timestamp. Missing/incomplete scanner output fails validation. Security gates reject **all CRITICAL findings**, even without a vendor fix, and **HIGH findings with a vendor fix**. Unfixed HIGH findings remain visible and need applicability review before a stable release. A passing gate does not mean an image is vulnerability-free.

The C++ Docker build compiles upstream libarchive 3.8.9 from its verified source digest with XML, crypto and compression integrations disabled. Docker's input/output archives use uncompressed tar; the agent enables only tar reading. Its runtime no longer needs the unused XML library that produced a critical finding. Upstream version/source hash and license are retained in the image and appended to its SBOM. OS package scanners alone do not establish coverage of statically linked C/C++ code; that explicit component also needs upstream advisory review during updates.

Base image digests and build actions are pinned. Debian security updates are applied during runtime image construction. The Python crypto dependency is explicitly a runtime dependency and has been updated to 50.0.2 after the locked 47.0.0 audit reported advisories. OIDC signature/state tests and actual CUDA/CPU executions passed after the update.

```sh
uv export --locked --no-dev --extra analysis --no-emit-project --quiet -o build/runtime-requirements.txt
uv tool run --from pip-audit==2.10.1 pip-audit -r build/runtime-requirements.txt --disable-pip --require-hashes --strict
uv run --no-sync python -m scripts.scan_images --output build/security --cache build/trivy-cache \
  strata/control-plane:local strata/worker-cpp:local strata/python-workloads:local \
  strata/wave-solver:local strata/gpu-smoke:local strata/postgres-ha:local \
  strata/ha-gateway:local gcr.io/etcd-development/etcd:v3.6.15
```

Build the images first. The scanner requires the local Linux Docker daemon; the socket grants its trusted scanning container daemon access. Windows Docker Desktop can use the same script with `--docker PATH_TO_DOCKER_EXE`. This process inspects images and does not launch their workloads or contact application users.

## Evidence and outstanding review

The updated locked production/analysis graph reported no known Python advisories in the local audit. All five updated images passed the configured critical/fixable-high gate. Raw scans still reported unfixed HIGH package findings: 47 coordinator, 56 C++ agent, 44 Python workload, 43 wave workload and 44 GPU smoke image findings. Counts refer to package/advisory pairs in the 2026-10-03 scanner database, not distinct exploitable vulnerabilities. The original Debian 12 image report and update history are retained in ignored local evidence directories.

The [native applicability review](native-security-review.md) documents constrained execution paths without suppressing findings. A human security decision, independent assessment and the actual new GitHub security workflow run remain outstanding. The [v3 local report](validation-v3.md) records sustained dispatch, managed-runtime and actual fault checks; their scope does not establish a final production security approval. No penetration-test certification, absence of unknown vulnerabilities or final production security approval is claimed. Preserve raw reports and re-run audits before each candidate build; advisory data and distribution fixes change over time.

The recovery infrastructure initially failed the same gate: the stock PostgreSQL image included vulnerable libxml2 and a Go privilege-switching binary, and the pinned HAProxy base had a fixable pcre2 finding. The custom PostgreSQL build omits XML and privilege-switching binaries, and the gateway build applies current Alpine updates. The revised PostgreSQL image has no CRITICAL/fixable-HIGH blockers, with 44 unresolved HIGH package/advisory pairs still requiring review. The revised gateway and pinned etcd image have no HIGH/CRITICAL findings. The separate Patroni dependency audit found no known advisories. PostgreSQL's explicit source hash/options/license are retained and appended to its inventory; static native source still needs advisory review. These are local observations from 2026-10-04, not a stable release approval.
