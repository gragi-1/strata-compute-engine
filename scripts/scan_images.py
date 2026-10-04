"""Scan local Linux Docker images by immutable ID, retain findings and generate SBOMs."""

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from scripts.security_gate import check

SCANNER = (
    "aquasec/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa"
)


def scan(images: list[str], output: Path, cache: Path, docker: str = "docker") -> None:
    output, cache = output.resolve(), cache.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    base = [
        docker,
        "run",
        "--rm",
        "--label",
        "strata.purpose=product-security-scan",
        "--cpus",
        "1",
        "--memory",
        "1g",
        "--mount",
        "type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock,readonly",
        "--mount",
        f"type=bind,source={output},target=/reports",
        "--mount",
        f"type=bind,source={cache},target=/root/.cache",
        SCANNER,
        "image",
        "--quiet",
        "--timeout",
        "5m",
        "--scanners",
        "vuln",
    ]
    reports, records = [], []
    for image in images:
        if image.startswith("-") or len(image) > 256:
            raise ValueError("invalid image name")
        metadata = json.loads(subprocess.check_output([docker, "image", "inspect", image]))[0]
        name = image.replace("/", "_").replace(":", "_").replace("@", "_")
        report, sbom = output / (name + ".vulnerabilities.json"), output / (name + ".sbom.json")
        subprocess.run(
            base
            + [
                "--severity",
                "HIGH,CRITICAL",
                "--format",
                "json",
                "--output",
                "/reports/" + report.name,
                metadata["Id"],
            ],
            check=True,
        )
        subprocess.run(
            base + ["--format", "cyclonedx", "--output", "/reports/" + sbom.name, metadata["Id"]],
            check=True,
        )
        component_file = {
            "worker-cpp": Path("worker_cpp/vendor-components.json"),
            "postgres-ha": Path("deploy/ha/vendor-components.json"),
        }.get(image.split(":")[0].rsplit("/", 1)[-1])
        if component_file is not None:
            inventory = json.loads(sbom.read_text(encoding="utf-8"))
            vendor = json.loads(component_file.read_text(encoding="utf-8"))
            inventory.setdefault("components", []).extend(vendor["components"])
            sbom.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
        records.append(
            {"image": image, "image_id": metadata["Id"], "report": report.name, "sbom": sbom.name}
        )
        reports.append(report)
    record = {"scanner": SCANNER, "scanned_at": datetime.now(UTC).isoformat(), "images": records}
    (output / "scan-manifest.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    check(reports)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("images", nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--docker", default="docker")
    args = parser.parse_args()
    scan(args.images, args.output, args.cache, args.docker)
