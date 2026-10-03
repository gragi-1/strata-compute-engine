import argparse
import json
import math
import random
import time
from pathlib import Path


def monte_carlo(samples: int, seed: int) -> dict[str, float | int]:
    rng = random.Random(seed)
    inside = sum(rng.random() ** 2 + rng.random() ** 2 <= 1 for _ in range(samples))
    p = inside / samples
    return {
        "samples": samples,
        "seed": seed,
        "pi": 4 * p,
        "absolute_error": abs(4 * p - math.pi),
        "standard_error": 4 * math.sqrt(p * (1 - p) / samples),
    }


def profile(path: Path) -> dict:
    import csv

    import numpy as np

    stats = {}
    count = 0

    def observe(name, value):
        try:
            value = float(value)
        except (ValueError, TypeError):
            return
        if not math.isfinite(value):
            return
        state = stats.setdefault(
            name, {"count": 0, "mean": 0.0, "m2": 0.0, "min": value, "max": value}
        )
        state["count"] += 1
        delta = value - state["mean"]
        state["mean"] += delta / state["count"]
        state["m2"] += delta * (value - state["mean"])
        state["min"], state["max"] = min(state["min"], value), max(state["max"], value)

    if path.suffix.lower() in {".csv", ".tsv"}:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(
                stream, delimiter="\t" if path.suffix.lower() == ".tsv" else ","
            )
            columns = reader.fieldnames or []
            if len(columns) > 1000:
                raise ValueError("profile supports at most 1000 columns")
            for row in reader:
                count += 1
                for name, value in row.items():
                    observe(name, value)
    elif path.suffix.lower() == ".parquet":
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        columns = parquet.schema.names
        if len(columns) > 1000:
            raise ValueError("profile supports at most 1000 columns")
        for batch in parquet.iter_batches(batch_size=4096):
            for row in batch.to_pylist():
                count += 1
                for name, value in row.items():
                    observe(name, value)
    elif path.suffix.lower() == ".npy":
        array = np.load(path, mmap_mode="r", allow_pickle=False)
        columns = ["value"]
        for value in array.reshape(-1, order="A"):
            count += 1
            observe("value", value)
    else:
        raise ValueError("profile supports CSV, TSV, NPY and Parquet")
    for state in stats.values():
        state["sample_variance"] = (
            state.pop("m2") / (state["count"] - 1) if state["count"] > 1 else None
        )
    return {"rows": count, "columns": columns, "statistics": stats}


def matrix(size: int, seed: int) -> dict[str, float | int]:
    import numpy as np

    rng = np.random.default_rng(seed)
    a, b = rng.random((size, size)), rng.random((size, size))
    return {"size": size, "seed": seed, "checksum": float((a @ b).sum())}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["monte-carlo", "matrix", "profile"])
    parser.add_argument("--input", type=Path)
    parser.add_argument("--samples", type=int, default=1000000)
    parser.add_argument("--size", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("/output/result.json"))
    args = parser.parse_args()
    if not 1 <= args.samples <= 1000000000 or not 1 <= args.size <= 10000:
        parser.error("samples or size outside supported range")
    started = time.perf_counter()
    if args.kind == "profile" and args.input is None:
        parser.error("profile requires --input")
    result = (
        profile(args.input)
        if args.kind == "profile"
        else (
            monte_carlo(args.samples, args.seed)
            if args.kind == "monte-carlo"
            else matrix(args.size, args.seed)
        )
    )
    result["duration_seconds"] = time.perf_counter() - started
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
