"""Hash only explicit release artifacts and capture their source revision."""

import argparse
import hashlib
import json
import subprocess
import tomllib
from pathlib import Path


def manifest(root: Path) -> dict[str, object]:
    root = root.resolve()
    artifacts = sorted(
        path
        for path in root.iterdir()
        if path.is_file() and path.name not in {"SHA256SUMS", "manifest.json", ".gitignore"}
    )
    if not artifacts or any(path.is_symlink() for path in artifacts):
        raise ValueError("release artifacts must be explicit regular files")
    files = {}
    for path in artifacts:
        with path.open("rb") as stream:
            files[path.name] = {
                "sha256": hashlib.file_digest(stream, "sha256").hexdigest(),
                "bytes": path.stat().st_size,
            }
    version = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"][
        "version"
    ]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    record: dict[str, object] = {
        "version": version,
        "source_commit": commit,
        "source_dirty": dirty,
        "artifacts": files,
    }
    (root / "manifest.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    (root / "SHA256SUMS").write_text(
        "".join(f"{info['sha256']}  {name}\n" for name, info in files.items()),
        encoding="utf-8",
    )
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    print(json.dumps(manifest(parser.parse_args().directory), indent=2))
