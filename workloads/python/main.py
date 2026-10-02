import argparse
import json
import random
import time
from pathlib import Path


def monte_carlo(samples: int, seed: int) -> dict[str, float | int]:
    rng = random.Random(seed)
    inside = sum(rng.random() ** 2 + rng.random() ** 2 <= 1 for _ in range(samples))
    return {"samples": samples, "seed": seed, "pi": 4 * inside / samples}


def matrix(size: int, seed: int) -> dict[str, float | int]:
    import numpy as np

    rng = np.random.default_rng(seed)
    a, b = rng.random((size, size)), rng.random((size, size))
    return {"size": size, "seed": seed, "checksum": float((a @ b).sum())}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["monte-carlo", "matrix"])
    parser.add_argument("--samples", type=int, default=1000000)
    parser.add_argument("--size", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("/output/result.json"))
    args = parser.parse_args()
    if not 1 <= args.samples <= 1000000000 or not 1 <= args.size <= 10000:
        parser.error("samples or size outside supported range")
    started = time.perf_counter()
    result = (
        monte_carlo(args.samples, args.seed)
        if args.kind == "monte-carlo"
        else matrix(args.size, args.seed)
    )
    result["duration_seconds"] = time.perf_counter() - started
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
