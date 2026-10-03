import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from workloads.python.main import monte_carlo, profile


@pytest.mark.parametrize("format", ["csv", "tsv", "parquet", "npy"])
def test_streamed_profile_ignores_nonfinite_values_and_computes_sample_variance(tmp_path, format):
    path = tmp_path / f"data.{format}"
    if format == "npy":
        np.save(path, np.array([1, 2, 3, float("nan")]))
    elif format == "parquet":
        pq.write_table(pa.table({"x": [1.0, 2.0, 3.0, float("nan")]}), path)
    else:
        path.write_text("x\n1\n2\n3\nNaN\n")
    result = profile(path)
    stats = next(iter(result["statistics"].values()))
    assert result["rows"] == 4
    assert stats == {"count": 3, "mean": 2.0, "min": 1.0, "max": 3.0, "sample_variance": 1.0}


def test_monte_carlo_seed_reproducibility_and_reported_error():
    a = monte_carlo(10000, 42)
    assert a == monte_carlo(10000, 42)
    assert 0 < a["standard_error"] < 0.1
    assert a["absolute_error"] >= 0
