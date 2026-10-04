# Publishing Strata v3.0.0

Release title: **Strata v3.0.0: Self-hosted Compute Workspace**.

Release notes: [v3.0.0](release-notes-v3.md). Package metadata, locked project metadata and the CMake project version are `3.0.0`; the matching Git tag is `v3.0.0`. These files prepare publication; they do not claim it has happened. The repository owner runs Git and publication commands. Descriptions, commits, tags and release notes remain in English.

## Review and push the release source

Run in the repository root after reviewing [the self-hosted scope](product-plan.md), [local qualification](validation-v3.md) and [the retained security findings](native-security-review.md):

```powershell
git status --short
git diff --check
git add .
git diff --cached --stat
git diff --cached --check
git commit -m "Prepare Strata v3.0.0 release"
git pull --rebase origin main
git push origin main
git rev-parse HEAD
```

Review staged files before committing. `build/`, virtual environments, caches, local databases, `.env`, `.secrets/`, backups and `node_modules/` are ignored local resources. Resolve rebase conflicts before proceeding; do not force-push. Changes during conflict resolution need matching validation and their own CI run.

## Require exact-commit CI, then create the matching tag

Open [CI](https://github.com/gragi-1/strata-compute-engine/actions/workflows/ci.yml). Require all seven jobs to succeed for the full commit returned above: `python`, `cpp`, `docker`, `browser`, `package-3.12`, `package-3.13` and `security`. Historical green runs do not validate this release.

Keep the selected source commit unchanged through tagging and candidate generation. If you commit updated CI links, notes or fixes, wait for that new commit's successful CI before tagging it. Preserve every published tag. If `v3.0.0` already exists, inspect its target and release state instead of deleting or moving it.

After exact-commit CI succeeds:

```powershell
git tag -a v3.0.0 -m "Release Strata v3.0.0"
git push origin v3.0.0
git rev-parse 'v3.0.0^{commit}'
```

Verify that the peeled tag commit equals the selected release commit and that its CI remains successful. Tag push can trigger another CI run for the same source. Local CUDA and one-host recovery retain their own hardware scope; hosted CI does not establish physical multi-host or GPU qualification.

## Generate and verify candidate artifacts

Dispatch [Release artifacts](https://github.com/gragi-1/strata-compute-engine/actions/workflows/release-artifacts.yml) from the matching **v3.0.0** tag with an authenticated GitHub CLI:

```powershell
gh workflow run release-artifacts.yml --ref v3.0.0 --repo gragi-1/strata-compute-engine
```

The CLI's `--ref` accepts a branch or tag, as documented in [the official workflow-run reference](https://cli.github.com/manual/gh_workflow_run). The stable candidate must use its matching tag: `main` is accepted by the preflight only for development versions, so dispatching this `3.0.0` stable build from `main` is rejected. The manual workflow must first exist on the default branch, which the source push above establishes.

Require the workflow to finish successfully. It validates clean source and exact-commit CI, builds reproducible archives, verifies a fresh installed schema, audits dependencies, scans runtime/infrastructure images and requests GitHub provenance attestations. It uploads `strata-release-candidate`; it does not deploy a service, publish PyPI/registry packages or create a GitHub Release. An artifact uploaded after a failed run is diagnostic output and is not an accepted candidate.

Follow [distribution and upgrades](distribution-and-upgrades.md) to download and verify checksums, version, tag commit, repository, workflow and attestations. Keep raw unresolved HIGH findings and SBOMs visible. Passing functional/security gates and verifying provenance do not substitute for a deployment-specific applicability/security decision.

## Publish the GitHub Release

After candidate verification, create the release in [GitHub Releases](https://github.com/gragi-1/strata-compute-engine/releases/new), choose the existing **v3.0.0** tag, use the English title above and copy [the prepared release notes](release-notes-v3.md). Add the observed successful CI and candidate-run links to the release body. Attach only the verified candidate files you intend to distribute, including checksums, provenance references, inventories and security reports. Use the stable release setting once these publication checks are satisfied.

Registry and PyPI publication are separate owner actions; do not claim either happened merely because a GitHub Release exists. Record actual release/run URLs in repository metadata through a later documentation commit, preserving the release tag's source identity.

Production identity, external backups and notifications, physical failure domains, longer endurance, independent security assessment and manual assistive-technology review remain deployment requirements. Provider-specific cloud instance/GPU provisioning requires a selected provider and approved budget and is outside this self-hosted release. Follow [the maintenance-window upgrade procedure](distribution-and-upgrades.md) before replacing an existing deployment.
