# Periodic jobs and dependency conditions

Schedules persist their job template, timezone, next occurrence, owner and execution history in PostgreSQL. The scheduler submits each occurrence in the same transaction as its `schedule_fires` record and advances the next occurrence only when admission succeeds. Multiple scheduler processes coordinate through database locks; `(schedule_id, scheduled_at)` is unique.

Create a schedule from the web's **Schedules** page, with `strata schedules create schedule.yaml`, or `Client.create_schedule(specification)`:

```yaml
name: Daily numerical experiment
cron: "0 9 * * *"
timezone: Europe/Madrid
catch_up: false
job:
  name: Daily Monte Carlo
  image: strata/python-workloads:local
  command: [python, /app/main.py, monte-carlo, --samples, "100000", --seed, "42"]
  resources: {cpu: 1, memory_mb: 128}
```

The five cron fields are minute, hour, day of month, month and weekday. Named IANA timezones are supported. Day of month and weekday use cron's usual OR rule when both are restricted. Each occurrence uses the owner's **current** enabled account, project membership, image allowlist and queue quota. Permission or template failures pause the schedule with a visible reason. Capacity failures keep its due occurrence and retry after 30 seconds.

The default restart behavior submits only the latest missed occurrence. `catch_up: true` submits every missed occurrence, with at most one per schedule and 20 total per tick. Resuming a paused schedule starts from the next future occurrence; resuming an already enabled schedule preserves its pending occurrence. Pausing stops future admission; jobs already submitted retain their ordinary lifecycle. Occurrences expose the schedule ID and scheduled time in job parameters.

Periodic templates accept sealed dataset inputs. They do not refer to existing job dependencies or artifacts, whose lifecycle would make repeated execution ambiguous. Immutable datasets and an optional `expected_image_digest` make an explicit repeatable template. There is a configurable global active-schedule limit (`STRATA_SCHEDULE_MAX_ACTIVE`). Execution history is retained; it is paged through `/schedules/{id}/fires`, `strata schedules fires`, the SDK and the web.

## Conditional dependencies

Jobs and workflow nodes accept `dependency_policy`:

| Policy | Behavior |
| --- | --- |
| `all_succeeded` | Default. Runs after every parent succeeds; a failed, timed-out or cancelled parent fails the child. |
| `all_terminal` | Runs after every parent finishes, including failure/cancellation. Useful for cleanup. |
| `any_failed` | Waits for every parent to finish, then runs if one failed, timed out or was cancelled. Otherwise the child is cancelled with a condition-false event. Requires a parent. |

Conditional nodes can consume datasets and explicitly pinned artifacts from terminal attempts. Unpinned artifact inputs require successful parents and are rejected for the other policies. Experiment replay resets the dependency condition after selecting a terminal source execution and preserves its resolved inputs.

The new default fields preserve the published v2 job/campaign/workflow idempotency hashes. Migration `0007` adds schedule tables and the default job policy. Upgrade with schedulers stopped, as documented in the deployment procedure. Tests cover restart behavior, quota backoff, owner revocation, project boundaries, daylight-saving transitions, conditional failure/cleanup and simultaneous PostgreSQL schedulers.
