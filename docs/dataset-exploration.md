# Large dataset exploration

The v3 dataset explorer provides paged queries, filters, sorting, statistics and a chart of the current page for CSV, TSV, JSON/JSONL, Parquet and numeric NumPy vectors/matrices. Open a dataset version and choose **Explore** beside a file. This path works for files larger than the existing small-preview limit, within the deployment's dataset-file size and query budgets.

Viewers can query and summarize files in their own projects. Queries are read-only even though the HTTP endpoints use POST for their typed request body. The API accepts column names, comparison operators and parameter values; it does not accept SQL, paths, URLs or function calls. File access always starts from an authorized database reference and verified blob. Extension auto-installation and auto-loading are disabled.

CLI users run `strata dataset-query FILE_ID` or `strata dataset-statistics FILE_ID`. `--specification PATH` accepts a JSON/YAML query with `columns`, `filters`, `sort_by`, `descending`, `offset` and `limit`. The SDK provides `Client.query_dataset(file_id, **query)` and `Client.dataset_statistics(file_id, **query)`. Follow `next_offset` for pagination; a null value means the result ended. NumPy columns are named `column_0`, `column_1`, and so on. NumPy object arrays and higher-dimensional arrays require a compute job or conversion to a supported tabular representation.

For example:

```json
{
  "columns": ["time", "temperature"],
  "filters": [{ "column": "temperature", "operator": "gte", "value": 20 }],
  "sort_by": "time",
  "limit": 100,
  "offset": 0
}
```

Supported comparisons are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `contains`, `is_null` and `not_null`. Use explicit null comparisons for missing values. Statistics describe all matching rows and include present/missing counts plus minimum, maximum, mean and population standard deviation for numeric columns. The browser chart describes the current page only; it does not imply a representative sample of the whole dataset.

Defaults are 256 MB of DuckDB engine memory, two engine threads, a 15-second computation deadline, two simultaneous queries per API process, 100 columns, 200 result rows, 20 filters, a maximum offset of one million and a 2 MiB response budget. Configure `STRATA_QUERY_MEMORY_MB`, `STRATA_QUERY_TIMEOUT_SECONDS`, `STRATA_QUERY_MAX_CONCURRENT` and `STRATA_QUERY_RESPONSE_BYTES`. Cells longer than 4096 UTF-8 bytes are shortened and flagged; rows stop at the response budget and provide a continuation offset. Select fewer columns if one row alone exceeds the budget.

CSV lines and JSON objects also have bounded sizes. Numerical NumPy arrays use a memory map and Arrow batches rather than loading the whole matrix. Spill-to-disk is disabled. Memory limits govern the analytical engine, not total Python/Arrow process memory; deployments should also enforce API container/process memory limits. The deadline interrupts database computation. Source download and SHA-256 verification have their storage transfer limits and occur before the computation timer. Expensive processing that does not fit these limits belongs in a resource-bounded compute job.

Tests cover all five source formats, full filtered statistics, pagination past the small-preview limit, hostile column/value input, viewer/cross-project boundaries, response/concurrency limits and an actual long computation interrupted at its deadline with the slot subsequently reused.
