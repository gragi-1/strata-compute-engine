from collections.abc import Iterator

from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
    Metric,
)
from sqlalchemy import func, select

from control_plane.domain import WAITING
from control_plane.models import Attempt, Job, JobEvent, Worker
from control_plane.services import EngineService


class DurableCollector:
    """Read the shared store; API restarts do not reset scheduler or worker counters."""

    def __init__(self, service: EngineService) -> None:
        self.service = service

    def collect(self) -> Iterator[Metric]:
        with self.service.factory() as session:
            for name, event in [
                ("jobs_submitted", "JOB_CREATED"),
                ("jobs_completed", "JOB_SUCCEEDED"),
                ("jobs_failed", "JOB_FAILED"),
                ("jobs_retried", "JOB_RETRYING"),
            ]:
                count = (
                    session.scalar(
                        select(func.count())
                        .select_from(JobEvent)
                        .where(
                            JobEvent.kind == event,
                        )
                    )
                    or 0
                )
                yield CounterMetricFamily(name, f"Durable count of {event}", value=count)
            yield GaugeMetricFamily(
                "queue_depth",
                "Jobs awaiting placement",
                value=session.scalar(
                    select(func.count()).select_from(Job).where(Job.status.in_(WAITING)),
                )
                or 0,
            )
            for name, column in [
                ("worker_heartbeats", Worker.heartbeats_count),
                ("worker_failures", Worker.failures_count),
            ]:
                yield CounterMetricFamily(
                    name, name, value=session.scalar(select(func.sum(column))) or 0
                )
            utilization = GaugeMetricFamily(
                "worker_utilization",
                "Reserved capacity fraction",
                labels=[
                    "worker_id",
                    "resource",
                ],
            )
            for worker in session.scalars(select(Worker)):
                utilization.add_metric([worker.id, "cpu"], worker.cpu_reserved / worker.cpu_total)
                utilization.add_metric(
                    [worker.id, "memory"], worker.memory_reserved_mb / worker.memory_total_mb
                )
            yield utilization
            # SQL aggregation avoids fetching the full attempt history into the API process.
            for name, first, last in [
                ("job_duration_seconds", Attempt.started_at, Attempt.finished_at),
                ("scheduler_latency_seconds", Attempt.eligible_at, Attempt.scheduled_at),
            ]:
                postgres = session.bind is not None and session.bind.dialect.name == "postgresql"
                duration = (
                    func.extract("epoch", last - first)
                    if postgres
                    else (func.julianday(last) - func.julianday(first)) * 86400
                )
                conditions = [first.is_not(None), last.is_not(None)]
                buckets = []
                for upper in [0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 3600]:
                    count = (
                        session.scalar(select(func.count()).where(*conditions, duration <= upper))
                        or 0
                    )
                    buckets.append((str(upper), count))
                count, total = session.execute(
                    select(func.count(), func.sum(duration)).where(
                        *conditions,
                    )
                ).one()
                buckets.append(("+Inf", count))
                yield HistogramMetricFamily(name, name, buckets=buckets, sum_value=total or 0)
