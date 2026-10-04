"""Require identical wheel and source archive bytes across two fixed-timestamp builds."""

import argparse
import hashlib
from pathlib import Path


def check(first: Path, second: Path) -> None:
    left = {path.name: path for path in first.iterdir() if path.name.endswith((".whl", ".tar.gz"))}
    right = {
        path.name: path for path in second.iterdir() if path.name.endswith((".whl", ".tar.gz"))
    }
    if not left or left.keys() != right.keys():
        raise ValueError("package builds contain different artifact names")
    for name in sorted(left):
        with left[name].open("rb") as a, right[name].open("rb") as b:
            if (
                hashlib.file_digest(a, "sha256").digest()
                != hashlib.file_digest(b, "sha256").digest()
            ):
                raise ValueError(f"package is not reproducible: {name}")
        print(f"Identical package bytes: {name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("first", type=Path)
    parser.add_argument("second", type=Path)
    args = parser.parse_args()
    check(args.first, args.second)
