import io

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from control_plane.analytics import DatasetAnalytics, DatasetQuery
from control_plane.api import create_app
from control_plane.datasets import DatasetService
from control_plane.schemas import NamedResource
from control_plane.services import DomainError
from tests.integration.test_identity import dataset, headers, prepare


def stored(service, name, content):
    datasets = DatasetService(service)
    data = datasets.create(NamedResource(name="Analytics evidence"))
    version = datasets.version(data.id, "v1")
    row = datasets.upload(version.id, name, [content])
    datasets.seal(version.id)
    return row


@pytest.mark.parametrize("suffix", ["csv", "tsv", "json", "parquet", "npy"])
def test_native_formats_query_and_full_statistics(service, suffix):
    buffer = io.BytesIO()
    columns = ["x", "y"]
    if suffix == "npy":
        np.save(buffer, np.array([[1, 2], [3, 4], [5, 6]]))
        columns = ["column_0", "column_1"]
    elif suffix == "parquet":
        pq.write_table(pa.table({"x": [1, 3, 5], "y": [2, 4, 6]}), buffer)
    elif suffix == "json":
        buffer.write(b'[{"x":1,"y":2},{"x":3,"y":4},{"x":5,"y":6}]')
    else:
        buffer.write(b"x,y\n1,2\n3,4\n5,6\n".replace(b",", b"\t" if suffix == "tsv" else b","))
    row = stored(service, f"data.{suffix}", buffer.getvalue())
    analytics = DatasetAnalytics(service)
    query = DatasetQuery.model_validate(
        {
            "columns": columns,
            "filters": [
                {"column": columns[0], "operator": "gte", "value": 3},
            ],
            "sort_by": columns[0],
            "descending": True,
            "limit": 1,
        }
    )
    response = analytics.query(row.id, query)
    assert response["rows"] == [[5, 6]] and response["next_offset"] == 1
    assert analytics.query(row.id, query.model_copy(update={"offset": 1}))["rows"] == [[3, 4]]
    statistics = analytics.statistics(row.id, query)
    assert statistics["rows"] == 2
    assert statistics["columns"][columns[0]]["mean"] == 4
    assert statistics["columns"][columns[0]]["stddev"] == 1


def test_queries_work_beyond_the_small_preview_limit(service):
    service.settings.preview_max_bytes = 1024
    row = stored(service, "large.csv", b"x,y\n" + b"1,2\n" * 100000)
    with pytest.raises(DomainError, match="preview limit"):
        DatasetService(service).preview(row.id)
    analytics = DatasetAnalytics(service)
    assert analytics.query(row.id, DatasetQuery(offset=99998, limit=10))["rows"] == [[1, 2], [1, 2]]
    assert analytics.statistics(row.id, DatasetQuery())["rows"] == 100000


def test_user_input_cannot_change_sql_or_read_another_file(service):
    row = stored(service, "data.csv", b'name,value\n"Robert; DROP TABLE data;",1\nAlice,2\n')
    client = TestClient(create_app(service.settings, service))
    query = {"filters": [{"column": "name", "operator": "eq", "value": "Robert; DROP TABLE data;"}]}
    result = client.post(f"/dataset-files/{row.id}/query", json=query)
    assert result.status_code == 200 and result.json()["rows"] == [["Robert; DROP TABLE data;", 1]]
    query["filters"][0]["column"] = "name\" FROM read_csv('C:/Windows/win.ini');--"
    assert client.post(f"/dataset-files/{row.id}/query", json=query).status_code == 422
    assert (
        client.post(
            f"/dataset-files/{row.id}/query", json={"sql": "SELECT * FROM secrets"}
        ).status_code
        == 422
    )
    assert (
        client.post(f"/dataset-files/{row.id}/query", json={"sort_by": "unknown"}).status_code
        == 422
    )


def test_viewers_can_query_private_datasets_but_cannot_write(workspace):
    client, _, root, alpha, beta, _, tokens = workspace
    _, _, file = dataset(client, headers(root, beta))
    bob = headers(tokens["bob"], beta)
    for action in ("query", "statistics"):
        assert (
            client.post(f"/dataset-files/{file['id']}/{action}", headers=bob, json={}).status_code
            == 200
        )
        assert (
            client.post(
                f"/dataset-files/{file['id']}/{action}", headers=headers(root, alpha), json={}
            ).status_code
            == 404
        )
    assert client.post("/datasets", headers=bob, json={"name": "Forbidden"}).status_code == 403


@pytest.fixture
def workspace(service):
    values = prepare(service)
    yield values
    values[0].close()


def test_response_and_concurrency_limits_and_null_statistics(service):
    row = stored(service, "data.csv", b"name,value\n" + (b"x" * 10000 + b",1\n") * 30 + b"none,\n")
    service.settings.query_response_bytes = 65536
    analytics = DatasetAnalytics(service)
    result = analytics.query(row.id, DatasetQuery(limit=200))
    assert result["truncated_cells"] and result["next_offset"] is not None
    assert len(result["rows"]) < 30 and len(result["rows"][0][0]) <= 4097
    stats = analytics.statistics(row.id, DatasetQuery(columns=["value"]))
    assert stats["columns"]["value"]["nulls"] == 1
    assert stats["columns"]["value"]["count"] == 30
    service.settings.query_max_concurrent = 1
    limited = DatasetAnalytics(service)
    with limited.connection(row.id), pytest.raises(DomainError, match="capacity"):
        limited.query(row.id, DatasetQuery())


def test_real_query_deadline_interrupts_long_computation_and_releases_slot(service):
    row = stored(service, "data.csv", b"x\n1\n")
    service.settings.query_timeout_seconds = 1
    service.settings.query_max_concurrent = 1
    analytics = DatasetAnalytics(service)
    with pytest.raises(DomainError, match="deadline"), analytics.connection(row.id) as db:
        db.execute("SELECT sum(sin(i)) FROM range(10000000000) AS r(i)").fetchone()
    assert analytics.query(row.id, DatasetQuery())["rows"] == [[1]]
