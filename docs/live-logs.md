# Live execution logs

Both upgraded worker agents publish bounded UTF-8 log snapshots approximately every two seconds while a container runs, and once more before its terminal report. Docker log rotation and the configured coordinator log byte limit bound storage. This is a tail snapshot; older output can be overwritten. Write complete scientific results as artifacts.

The web job detail view follows the latest attempt automatically. Disable **Follow the latest attempt** to pause updates. The client stops polling terminal jobs and hidden pages. Every poll rechecks authentication and project access. Attempt changes and terminal status are visible beside the log text.

```sh
strata logs JOB_ID --follow --timeout 3600
strata logs JOB_ID --attempt 1
```

`GET /jobs/{id}/log-snapshot?cursor=REVISION` returns revision, latest attempt number and job state. When unchanged, `changed` is false and `text` is null. Revision changes include attempt and job state as well as log content. Snapshots contain the full bounded tail, not append-only byte offsets, so truncation and retries do not silently duplicate or omit a claimed stream segment.

SDK `Client.log_snapshot(job_id, cursor)` implements the same contract. `Client.follow_logs(job_id, timeout=3600)` yields changed snapshots until a terminal job state or timeout. A running program may buffer stdout; use unbuffered output or flush when progress should appear immediately.

API tests cover active output, unchanged revisions and final status. Opt-in Docker tests execute an unbuffered program on each worker, observe its output before completion and verify invalid UTF-8 replacement. Live polling cannot retrieve output from a crashed or disconnected agent until a snapshot has reached the durable coordinator.
