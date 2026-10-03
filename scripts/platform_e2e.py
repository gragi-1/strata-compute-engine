"""Live dataset transfer, read-only mounts, workflows and campaign evidence."""

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

from scripts.e2e_live import wait_job, wait_ready


def main():
    key = os.getenv("STRATA_API_KEY")
    evidence = {
        "timestamp": datetime.now(UTC).isoformat(),
        "environment": "single physical host",
        "workers": {},
        "workflow": {},
        "campaign": {},
    }
    with httpx.Client(
        base_url=os.getenv("STRATA_API_URL", "http://localhost:8000"),
        timeout=60,
        headers={"Authorization": f"Bearer {key}"} if key else {},
    ) as client:
        wait_ready(client)

        def post(path, body=None):
            r = client.post(path, json=body)
            r.raise_for_status()
            return r.json()

        dataset = post("/datasets", {"name": "Live validation dataset"})
        version = post(f"/datasets/{dataset['id']}/versions", {"label": "verified-v1"})
        content = b"abcd" * (2 * 1024 * 1024 + 7)
        r = client.put(f"/dataset-versions/{version['id']}/files/payload.bin", content=content)
        r.raise_for_status()
        file = r.json()
        post(f"/dataset-versions/{version['id']}/seal")
        for worker in ["python", "cpp"]:
            code = (
                "import hashlib,json,pathlib; p=pathlib.Path('/inputs/data/payload.bin'); "
                "result={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'size':p.stat().st_size};\n"
                "try:\n p.write_bytes(b'corruption');"
                " raise RuntimeError('input mount is writable')\n"
                "except OSError: result['readonly']=True\n"
                "pathlib.Path('/output/result.json').write_text(json.dumps(result));print(json.dumps(result))"
            )
            job = post(
                "/jobs",
                {
                    "name": f"Dataset validation {worker}",
                    "image": "strata/python-workloads:local",
                    "command": ["python", "-c", code],
                    "capabilities": [f"worker-{worker}"],
                    "inputs": [{"version_id": version["id"], "alias": "data"}],
                    "max_retries": 0,
                    "timeout_seconds": 120,
                },
            )
            final = wait_job(client, job["id"], 180)
            assert final["status"] == "SUCCEEDED", final
            artifacts = client.get(f"/jobs/{job['id']}/artifacts").json()
            result = client.get(artifacts[0]["uri"]).json()
            assert result == {"sha256": file["sha256"], "size": len(content), "readonly": True}, (
                result
            )
            evidence["workers"][worker] = {"job_id": job["id"], **result}
        image = "strata/python-workloads:local"
        producer = {
            "name": "Generate observations",
            "image": image,
            "capabilities": ["worker-python"],
            "command": [
                "python",
                "-c",
                "from pathlib import Path;"
                "Path('/output/observations.csv').write_text('x\\n1\\n2\\n3\\n')",
            ],
            "max_retries": 0,
        }
        consumer = {
            "name": "Analyze observations",
            "image": image,
            "capabilities": ["worker-cpp"],
            "artifact_inputs": [
                {"job_id": "generate", "name": "observations.csv", "alias": "source"}
            ],
            "command": [
                "python",
                "-c",
                "import csv,json;from pathlib import Path;"
                "v=sum(int(r['x']) for r in csv.DictReader("
                "Path('/inputs/source/observations.csv').open()));"
                "Path('/output/result.json').write_text(json.dumps({'sum':v}))",
            ],
            "max_retries": 0,
        }
        workflow = post(
            "/workflows",
            {"name": "Generate and analyze", "nodes": {"generate": producer, "analyze": consumer}},
        )
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            workflow = client.get(f"/campaigns/{workflow['id']}").json()
            if workflow["status"] == "COMPLETED":
                break
            time.sleep(0.5)
        assert workflow["counts"] == {"SUCCEEDED": 2}, workflow
        results = client.get(f"/campaigns/{workflow['id']}/results").json()
        assert next(r for r in results if r["node"] == "analyze")["result.sum"] == 6, results
        evidence["workflow"] = {"id": workflow["id"], "results": results}
        started = time.perf_counter()
        campaign = post(
            "/campaigns",
            {
                "name": "Monte Carlo reproducibility study",
                "template": {
                    "name": "Estimate pi",
                    "image": image,
                    "command": [
                        "python",
                        "/app/main.py",
                        "monte-carlo",
                        "--samples",
                        "${samples}",
                        "--seed",
                        "${seed}",
                    ],
                    "resources": {"cpu": 0.5, "memory_mb": 128},
                    "max_retries": 2,
                },
                "matrix": {"samples": [10000, 100000], "seed": list(range(12))},
                "repeats": 2,
            },
        )
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            campaign = client.get(f"/campaigns/{campaign['id']}").json()
            if campaign["status"] == "COMPLETED":
                break
            time.sleep(0.5)
        assert campaign["counts"] == {"SUCCEEDED": 48}, campaign
        results = client.get(f"/campaigns/{campaign['id']}/results").json()
        for seed in range(12):
            for samples in [10000, 100000]:
                values = [
                    r["result.pi"] for r in results if r["seed"] == seed and r["samples"] == samples
                ]
                assert len(values) == 2 and values[0] == values[1]
        evidence["campaign"] = {
            "id": campaign["id"],
            "jobs": 48,
            "elapsed_seconds": time.perf_counter() - started,
            "results": results,
        }
        csv_response = client.get(f"/campaigns/{campaign['id']}/results?format=csv")
        csv_response.raise_for_status()
        output = Path("build/platform-validation")
        output.mkdir(parents=True, exist_ok=True)
        (output / "campaign.csv").write_text(csv_response.text, encoding="utf-8")
        (output / "evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {
                    "workers": list(evidence["workers"]),
                    "workflow": "SUCCEEDED",
                    "campaign_id": campaign["id"],
                    "campaign_jobs": 48,
                    "elapsed_seconds": evidence["campaign"]["elapsed_seconds"],
                }
            )
        )


if __name__ == "__main__":
    main()
