import json

import pytest

from scripts.security_gate import check


def report(tmp_path, vulnerabilities):
    path = tmp_path / "image.json"
    path.write_text(
        json.dumps(
            {
                "SchemaVersion": 2,
                "ArtifactType": "container_image",
                "Results": [{"Class": "os-pkgs", "Vulnerabilities": vulnerabilities}],
            }
        )
    )
    return path


def test_security_gate_rejects_critical_without_a_fix_and_fixable_high(tmp_path):
    for severity, fix in [("CRITICAL", ""), ("CRITICAL", "2"), ("HIGH", "2")]:
        path = report(
            tmp_path,
            [
                {
                    "VulnerabilityID": "synthetic-finding",
                    "PkgName": "library",
                    "InstalledVersion": "1",
                    "Severity": severity,
                    "FixedVersion": fix,
                }
            ],
        )
        with pytest.raises(ValueError, match="synthetic-finding"):
            check([path])


def test_security_gate_retains_unfixed_high_and_refuses_missing_scan(tmp_path, capsys):
    path = report(tmp_path, [{"Severity": "HIGH", "FixedVersion": ""}])
    check([path])
    assert "1 unresolved high" in capsys.readouterr().out
    path.write_text('{"SchemaVersion":2,"Results":[]}')
    with pytest.raises(ValueError, match="incomplete"):
        check([path])
