"""Reject every critical and fixable high finding; retain unresolved high findings."""

import argparse
import json
from pathlib import Path


def findings(report: Path) -> list[dict]:
    data = json.loads(report.read_text(encoding="utf-8"))
    if (
        data.get("SchemaVersion") != 2
        or data.get("ArtifactType") != "container_image"
        or not isinstance(data.get("Results"), list)
        or not any(result.get("Class") == "os-pkgs" for result in data["Results"])
    ):
        raise ValueError("invalid or incomplete Trivy image report")
    return [item for result in data["Results"] for item in result.get("Vulnerabilities", [])]


def check(reports: list[Path]) -> None:
    blocked = []
    for report in reports:
        items = findings(report)
        fixable = [
            item
            for item in items
            if item["Severity"] == "CRITICAL"
            or item["Severity"] == "HIGH"
            and item.get("FixedVersion")
        ]
        pending = [
            item for item in items if item["Severity"] == "HIGH" and not item.get("FixedVersion")
        ]
        print(
            f"{report.name}: {len(fixable)} blocking findings; "
            f"{len(pending)} unresolved high findings requiring review"
        )
        for item in fixable:
            blocked.append(
                f"{report.name}: {item['VulnerabilityID']} {item['PkgName']} "
                f"{item['InstalledVersion']} -> {item.get('FixedVersion') or 'no vendor fix'}"
            )
    if blocked:
        raise ValueError("blocking image vulnerabilities:\n" + "\n".join(blocked))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, nargs="+")
    check(parser.parse_args().reports)
