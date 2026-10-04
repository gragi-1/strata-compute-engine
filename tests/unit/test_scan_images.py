import json
import subprocess

import pytest

from scripts.scan_images import scan


def scanner(monkeypatch, *, findings=(), fail=False):
    image_id = "sha256:" + "a" * 64
    calls = []

    def inspect(arguments):
        calls.append(arguments)
        return json.dumps([{"Id": image_id}]).encode()

    def run(arguments, *, stdout, check):
        assert check and arguments[-1] == image_id
        assert "--output" not in arguments
        assert not any("target=/reports" in argument for argument in arguments)
        if fail:
            raise subprocess.CalledProcessError(1, arguments)
        if arguments[arguments.index("--format") + 1] == "json":
            body = {
                "SchemaVersion": 2,
                "ArtifactType": "container_image",
                "Results": [{"Class": "os-pkgs", "Vulnerabilities": list(findings)}],
            }
        else:
            body = {"bomFormat": "CycloneDX", "components": [{"name": "runtime-package"}]}
        stdout.write(json.dumps(body).encode())

    monkeypatch.setattr("scripts.scan_images.subprocess.check_output", inspect)
    monkeypatch.setattr("scripts.scan_images.subprocess.run", run)
    return image_id, calls


@pytest.mark.parametrize(
    ("image", "vendor"),
    [
        ("strata/worker-cpp:3.0.0", "worker_cpp/vendor-components.json"),
        ("strata/postgres-ha:3.0.0", "deploy/ha/vendor-components.json"),
    ],
)
def test_host_owned_reports_accept_native_vendor_inventory(tmp_path, monkeypatch, image, vendor):
    monkeypatch.chdir(tmp_path)
    component = tmp_path / vendor
    component.parent.mkdir(parents=True)
    component.write_text(json.dumps({"components": [{"name": "static-source"}]}))
    image_id, calls = scanner(monkeypatch)
    output = tmp_path / "reports"
    scan([image], output, tmp_path / "cache")
    record = json.loads((output / "scan-manifest.json").read_text())["images"][0]
    assert record["image_id"] == image_id and calls == [["docker", "image", "inspect", image]]
    inventory = json.loads((output / record["sbom"]).read_text())
    assert [item["name"] for item in inventory["components"]] == [
        "runtime-package",
        "static-source",
    ]
    assert (output / record["report"]).stat().st_uid == (output / record["sbom"]).stat().st_uid


def test_digest_only_reference_does_not_require_a_local_tag(tmp_path, monkeypatch):
    image = "gcr.io/etcd-development/etcd:v3.6.15@sha256:" + "b" * 64
    _, calls = scanner(monkeypatch)
    scan([image], tmp_path / "reports", tmp_path / "cache")
    assert calls == [["docker", "image", "inspect", image]]


def test_scanner_error_cannot_be_reported_as_a_success(tmp_path, monkeypatch):
    scanner(monkeypatch, fail=True)
    output = tmp_path / "reports"
    with pytest.raises(subprocess.CalledProcessError):
        scan(["strata/control-plane:3.0.0"], output, tmp_path / "cache")
    assert not (output / "scan-manifest.json").exists()


def test_streamed_findings_still_enforce_the_security_gate(tmp_path, monkeypatch):
    scanner(
        monkeypatch,
        findings=[
            {
                "Severity": "CRITICAL",
                "FixedVersion": "",
                "VulnerabilityID": "synthetic-critical",
                "PkgName": "library",
                "InstalledVersion": "1",
            }
        ],
    )
    with pytest.raises(ValueError, match="synthetic-critical"):
        scan(["strata/control-plane:3.0.0"], tmp_path / "reports", tmp_path / "cache")
