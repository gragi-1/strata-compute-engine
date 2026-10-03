# Publishing Strata v2.0.0

Release title: **Strata v2.0.0: Compute Workspace**.

Release notes: [v2.0.0](releases/v2.0.0.md). Repository descriptions, commit messages, tag messages, release titles and release notes stay in English.

## Push the implementation

Run from the repository root after reviewing `git status`:

```powershell
git add .
git commit -m "Expand Strata into a compute workspace"
git push origin main
```

If Git reports a non-fast-forward rejection, preserve the commit and integrate the remote changes:

```powershell
git pull --rebase origin main
git push origin main
```

Resolve any reported conflict before continuing; do not force-push. A resolved code change needs its own successful CI run.

## Verify the exact commit

Open [GitHub Actions](https://github.com/gragi-1/strata-compute-engine/actions). All three CI jobs (`python`, `cpp`, `docker`) must succeed for the commit to be released. Check the run's commit against `git rev-parse HEAD`. A green badge from v1 or another commit does not validate v2.

Record the successful workflow URL and tested commit in the publication evidence when available. If any implementation changes after that run, push and validate the new commit before tagging it.

## Tag the tested commit

After CI succeeds for the exact local commit and `git status` is clean:

```powershell
git tag -a v2.0.0 -m "Strata v2.0.0: Compute Workspace"
git push origin v2.0.0
```

If `v2.0.0` already exists, inspect it rather than overwriting or moving a published tag. Pushing a tag alone does not create a GitHub Release. The current workflow also validates tag pushes; wait for that run before publishing the release.

## Create the GitHub Release

Open [New release](https://github.com/gragi-1/strata-compute-engine/releases/new), choose the existing `v2.0.0` tag, use **Strata v2.0.0: Compute Workspace** as the title and paste the body from [the prepared notes](releases/v2.0.0.md). Exclude the initial Markdown title because GitHub has a separate title field. Include the successful CI run link near the verification paragraph.

Publish as a stable release after the tested tag's checks pass. GitHub provides the tagged source archives automatically. This procedure publishes the repository and release; it does not deploy a public service or publish Python/container packages to a registry.

Expected release URL: `https://github.com/gragi-1/strata-compute-engine/releases/tag/v2.0.0`. Verify that the actual release page resolves, shows the intended tag/commit and contains the English notes. Update repository metadata and validation links with observed publication details; never label a pending run as successful.
