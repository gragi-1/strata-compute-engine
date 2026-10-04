"""Authorized, bounded dataset exploration with a typed query vocabulary."""

import json
import math
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from threading import BoundedSemaphore, Timer
from typing import Any, Literal

import duckdb
from pydantic import Field, field_validator

from control_plane.datasets import DatasetService, preview_value
from control_plane.errors import DomainError
from control_plane.schemas import StrictModel
from control_plane.services import EngineService


class Filter(StrictModel):
    column: str = Field(min_length=1, max_length=256)
    operator: Literal["eq", "ne", "lt", "lte", "gt", "gte", "contains", "is_null", "not_null"] = (
        "eq"
    )
    value: str | int | float | bool | None = None

    @field_validator("value")
    @classmethod
    def bounded_value(cls, value: Any) -> Any:
        if isinstance(value, str) and len(value) > 4096:
            raise ValueError("filter strings are limited to 4096 characters")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("filter values must be finite")
        return value


class DatasetQuery(StrictModel):
    columns: list[str] | None = Field(default=None, min_length=1, max_length=100)
    filters: list[Filter] = Field(default_factory=list, max_length=20)
    sort_by: str | None = Field(default=None, max_length=256)
    descending: bool = False
    offset: int = Field(default=0, ge=0, le=1000000)
    limit: int = Field(default=50, ge=1, le=200)


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class DatasetAnalytics:
    def __init__(self, service: EngineService) -> None:
        self.svc = service
        self.slots = BoundedSemaphore(service.settings.query_max_concurrent)

    @contextmanager
    def connection(self, file_id: str) -> Iterator[duckdb.DuckDBPyConnection]:
        if not self.slots.acquire(blocking=False):
            raise DomainError(429, "dataset query capacity reached")
        db = None
        timer = None
        pins = ExitStack()
        try:
            row, path = pins.enter_context(DatasetService(self.svc).open_file(file_id))
            db = duckdb.connect(
                ":memory:",
                config={
                    "threads": 2,
                    "memory_limit": f"{self.svc.settings.query_memory_mb}MB",
                    "temp_directory": "",
                    "max_temp_directory_size": "0MB",
                    "autoinstall_known_extensions": False,
                    "autoload_known_extensions": False,
                    "preserve_insertion_order": True,
                },
            )
            db.execute("SET enable_progress_bar = false")
            timer = Timer(self.svc.settings.query_timeout_seconds, db.interrupt)
            timer.daemon = True
            timer.start()
            suffix = Path(row.name).suffix.lower()
            if suffix in {".csv", ".tsv"}:
                db.read_csv(
                    str(path),
                    header=True,
                    delimiter="\t" if suffix == ".tsv" else ",",
                    max_line_size=4 * 1024**2,
                ).create_view("data")
            elif suffix == ".parquet":
                db.read_parquet(str(path)).create_view("data")
            elif suffix in {".json", ".jsonl", ".ndjson"}:
                db.read_json(str(path), maximum_object_size=16 * 1024**2).create_view("data")
            elif suffix == ".npy":
                self.numpy_view(db, path)
            else:
                raise DomainError(
                    415, "query supports CSV, TSV, JSON, Parquet and numeric NumPy arrays"
                )
            yield db
        except duckdb.OutOfMemoryException as exc:
            raise DomainError(413, "dataset query exceeds its memory budget") from exc
        except duckdb.InterruptException as exc:
            raise DomainError(408, "dataset query deadline exceeded") from exc
        except (duckdb.Error, ValueError) as exc:
            raise DomainError(
                422, "dataset cannot be queried with the supplied types and filters"
            ) from exc
        finally:
            if timer:
                timer.cancel()
                timer.join()
            if db:
                db.close()
            pins.close()
            self.slots.release()

    def numpy_view(self, db: duckdb.DuckDBPyConnection, path: Path) -> None:
        import numpy as np
        import pyarrow as pa

        array = np.load(path, mmap_mode="r", allow_pickle=False)
        if array.ndim not in {1, 2} or array.dtype.kind not in "biuf":
            raise DomainError(415, "NumPy queries require a numeric vector or matrix")
        if array.ndim == 1:
            array = array.reshape(-1, 1)
        if array.shape[1] > 100:
            raise DomainError(413, "dataset has more than 100 columns")
        names = [f"column_{i}" for i in range(array.shape[1])]
        schema = pa.schema([(name, pa.from_numpy_dtype(array.dtype)) for name in names])
        batch_rows = max(
            1, min(50000, 8 * 1024**2 // max(1, array.shape[1] * array.dtype.itemsize))
        )

        def batches() -> Iterator[Any]:
            for start in range(0, len(array), batch_rows):
                values = [
                    pa.array(array[start : start + batch_rows, i]) for i in range(array.shape[1])
                ]
                yield pa.RecordBatch.from_arrays(values, schema=schema)

        reader = pa.RecordBatchReader.from_batches(schema, batches())
        db.from_arrow(reader).create_view("data")

    def schema(self, db: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
        rows = db.execute("DESCRIBE data").fetchall()
        if len(rows) > 100:
            raise DomainError(413, "dataset has more than 100 columns")
        if any(len(row[0]) > 256 or len(row[1]) > 1024 for row in rows):
            raise DomainError(413, "dataset schema exceeds query metadata limits")
        return [(row[0], row[1]) for row in rows]

    def where(self, body: DatasetQuery, names: set[str]) -> tuple[str, list[Any]]:
        clauses, values = [], []
        operators = {"eq": "=", "ne": "!=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">="}
        for condition in body.filters:
            if condition.column not in names:
                raise DomainError(422, "unknown filter column")
            column = quoted(condition.column)
            if condition.operator in {"is_null", "not_null"}:
                clauses.append(
                    column + (" IS NULL" if condition.operator == "is_null" else " IS NOT NULL")
                )
            elif condition.operator == "contains":
                clauses.append(f"contains(CAST({column} AS VARCHAR), ?)")
                values.append(condition.value)
            else:
                clauses.append(column + " " + operators[condition.operator] + " ?")
                values.append(condition.value)
        return " AND ".join(clauses) or "TRUE", values

    def query(self, file_id: str, body: DatasetQuery) -> dict[str, Any]:
        with self.connection(file_id) as db:
            schema = self.schema(db)
            names = {row[0] for row in schema}
            selected = body.columns or [row[0] for row in schema]
            if any(name not in names for name in selected) or len(set(selected)) != len(selected):
                raise DomainError(422, "unknown or duplicated query column")
            if body.sort_by and body.sort_by not in names:
                raise DomainError(422, "unknown sort column")
            where, parameters = self.where(body, names)
            order = (
                f" ORDER BY {quoted(body.sort_by)} {'DESC' if body.descending else 'ASC'}"
                if body.sort_by
                else ""
            )
            cursor = db.execute(
                "SELECT "
                + ",".join(quoted(name) for name in selected)
                + " FROM data WHERE "
                + where
                + order
                + " LIMIT ? OFFSET ?",
                parameters + [body.limit + 1, body.offset],
            )
            rows: list[list[Any]] = []
            used = len(json.dumps({"columns": selected, "types": dict(schema)}).encode()) + 256
            truncated_cells = False
            more = False
            while (record := cursor.fetchone()) is not None:
                if len(rows) >= body.limit:
                    more = True
                    break
                values = []
                for value in record:
                    value = preview_value(value)
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, default=str, ensure_ascii=False)
                    if isinstance(value, str) and len(value.encode("utf-8")) > 4096:
                        value = value.encode("utf-8")[:4096].decode("utf-8", errors="ignore") + "…"
                        truncated_cells = True
                    values.append(value)
                size = len(json.dumps(values, default=str, ensure_ascii=False).encode("utf-8"))
                if used + size > self.svc.settings.query_response_bytes:
                    if not rows:
                        raise DomainError(
                            413, "single query row exceeds response budget; select fewer columns"
                        )
                    more = True
                    break
                rows.append(values)
                used += size
            return {
                "columns": selected,
                "types": dict(schema),
                "rows": rows,
                "offset": body.offset,
                "next_offset": body.offset + len(rows) if more else None,
                "truncated_cells": truncated_cells,
            }

    def statistics(self, file_id: str, body: DatasetQuery) -> dict[str, Any]:
        with self.connection(file_id) as db:
            schema = self.schema(db)
            names = {row[0] for row in schema}
            selected = body.columns or list(names)
            if any(name not in names for name in selected):
                raise DomainError(422, "unknown statistics column")
            numeric = {
                name
                for name, kind in schema
                if kind.startswith(
                    (
                        "TINYINT",
                        "SMALLINT",
                        "INTEGER",
                        "BIGINT",
                        "HUGEINT",
                        "UTINYINT",
                        "USMALLINT",
                        "UINTEGER",
                        "UBIGINT",
                        "UHUGEINT",
                        "FLOAT",
                        "DOUBLE",
                        "DECIMAL",
                    )
                )
            }
            expressions = ["count(*)"]
            for name in selected:
                column = quoted(name)
                expressions.extend([f"count({column})", f"count(*)-count({column})"])
                if name in numeric:
                    expressions.extend(
                        [
                            f"min({column})",
                            f"max({column})",
                            f"avg({column})",
                            f"stddev_pop({column})",
                        ]
                    )
            where, parameters = self.where(body, names)
            record = db.execute(
                "SELECT " + ",".join(expressions) + " FROM data WHERE " + where, parameters
            ).fetchone()
            assert record is not None
            index, columns = 1, {}
            for name in selected:
                keys = ["count", "nulls"] + (
                    ["min", "max", "mean", "stddev"] if name in numeric else []
                )
                columns[name] = dict(
                    zip(keys, preview_value(list(record[index : index + len(keys)])), strict=True)
                )
                index += len(keys)
            return {"rows": record[0], "columns": columns, "scope": "all matching rows"}
