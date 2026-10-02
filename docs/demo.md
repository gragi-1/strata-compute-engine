# Recording the failure recovery demo

Start the standard Compose stack, confirm three healthy workers and run `uv run python scripts/e2e_live.py`. Then run:

```bash
uv run python scripts/failure_demo.py
```

The script records actual API event timestamps, kills the worker selected for its live job, asserts a different worker succeeds, and restarts the killed worker in a `finally` block. It stores an asciinema v2 recording in `docs/demo/failure-recovery.cast`. The default 65-second recording holds its final result if recovery finishes sooner. Failures raise errors instead of writing a false success.

Replay with `asciinema play docs/demo/failure-recovery.cast`. `scripts/render_demo.py` can render this same recording into a GIF when Pillow is installed. The recording format is data: it is not a simulator, and it does not substitute fictional event timestamps for server events.

For a live interview, show `strata events JOB_ID`, `strata attempts JOB_ID`, the structured worker/scheduler logs and Grafana next to the terminal. Explain both the durable recovery and the possible physical overlap of the orphan and replacement container.

## Included recording

The actual 2026-10-02 run lasts 65.001 seconds. After killing `worker-1`, the replacement on `worker-2` reached RUNNING in 14.928 seconds and completed in 23.414 seconds. The job succeeded on its second attempt. These are timings from this run, not recovery bounds or averages; see [the recording summary](demo/failure-recovery-summary.json) and [GIF](demo/failure-recovery.gif).

## Live communication failure

`uv run python scripts/partition_probe.py` pauses the RPC container while one workload runs on each agent implementation. After a 38-second observation interval, it inspects their real Docker state, requires both to have stopped and their jobs to be RETRYING, then cancels the jobs and unpauses RPC. The committed [probe evidence](demo/partition-probe.json) records two exited containers. Its 40.438-second elapsed value includes inspection and API calls; it is not an exact termination latency. Both workers registered healthy sessions again afterward.
