"""Check clean source identity and successful CI before attesting manual release candidates."""

import json
import os
import subprocess
import tomllib
import urllib.parse
import urllib.request
from pathlib import Path


def preflight() -> dict:
    metadata = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    version = metadata["project"]["version"]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if commit != os.environ["GITHUB_SHA"]:
        raise ValueError("checked out commit differs from the dispatched workflow commit")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("release source must be clean")
    kind, ref = os.environ["GITHUB_REF_TYPE"], os.environ["GITHUB_REF_NAME"]
    if kind == "tag":
        if ref != "v" + version:
            raise ValueError("release tag must match package version exactly")
    elif kind != "branch" or ref != "main" or ".dev" not in version:
        raise ValueError("use a matching version tag, or main for an unreleased development build")
    repository = os.environ["GITHUB_REPOSITORY"]
    required = {"python", "cpp", "docker", "browser", "package-3.12", "package-3.13", "security"}

    def read(path):
        request = urllib.request.Request(
            f"https://api.github.com/repos/{repository}{path}",
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "Strata-release",
                "Authorization": "Bearer " + os.environ["GH_TOKEN"],
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)

    query = urllib.parse.urlencode({"head_sha": commit, "event": "push", "per_page": 30})
    runs = read("/actions/workflows/ci.yml/runs?" + query)
    for run in runs["workflow_runs"]:
        if run["head_sha"] != commit or run["conclusion"] != "success":
            continue
        jobs = read(f"/actions/runs/{run['id']}/jobs?per_page=100")
        passed = {job["name"] for job in jobs["jobs"] if job["conclusion"] == "success"}
        if required.issubset(passed):
            return {"version": version, "commit": commit, "ci_run": run["html_url"]}
    raise ValueError(
        "exact source commit needs successful Python, C++, Docker, browser, package and security CI"
    )


if __name__ == "__main__":
    print(json.dumps(preflight(), indent=2))
