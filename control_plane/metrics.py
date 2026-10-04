from collections.abc import Iterator

from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
    Metric,
)
from sqlalchemy import func, select

from control_plane.domain import ACTIVE, WAITING
from control_plane.models import (
    Admission,
    Attempt,
    Job,
    JobEvent,
    MaintenanceState,
    ProvisionedWorker,
    WebhookDelivery,
    Worker,
    WorkerPool,
)
from control_plane.services import EngineService


class DurableCollector:
    """Read the shared store; API restarts do not reset scheduler or worker counters."""

    def __init__(self, service: EngineService) -> None:
        self.service = service

    def collect(self) -> Iterator[Metric]:
        with self.service.factory() as session:
            now = self.service.now(session)
            admission = session.get(Admission, 1)
            for field in ("accepting_jobs", "scheduling_enabled"):
                yield GaugeMetricFamily(
                    "strata_cluster_" + field,
                    "Durable cluster " + field,
                    value=int(bool(admission and getattr(admission, field))),
                )
            from datetime import timedelta

            yield GaugeMetricFamily(
                "strata_live_workers",
                "Workers inside their liveness window",
                value=session.scalar(
                    select(func.count())
                    .select_from(Worker)
                    .where(
                        Worker.status.in_({"HEALTHY", "DRAINING"}),
                        Worker.last_heartbeat
                        > now - timedelta(seconds=self.service.settings.worker_timeout),
                    )
                )
                or 0,
            )
            oldest = session.scalar(select(func.min(Job.created_at)).where(Job.status.in_(WAITING)))
            yield GaugeMetricFamily(
                "strata_oldest_waiting_seconds",
                "Age of the oldest waiting job",
                value=max(0, (now - oldest).total_seconds()) if oldest else 0,
            )
            yield GaugeMetricFamily(
                "strata_expired_active_leases",
                "Active attempts past their lease",
                value=session.scalar(
                    select(func.count())
                    .select_from(Attempt)
                    .where(Attempt.status.in_(ACTIVE), Attempt.lease_expires_at <= now)
                )
                or 0,
            )
            successful = GaugeMetricFamily(
                "strata_operation_success", "Last operation succeeded", labels=["operation"]
            )
            age = GaugeMetricFamily(
                "strata_operation_success_age_seconds", "Age of last success", labels=["operation"]
            )
            for operation in session.scalars(select(MaintenanceState)):
                successful.add_metric([operation.name], int(operation.status == "SUCCEEDED"))
                age.add_metric(
                    [operation.name],
                    max(0, (now - operation.succeeded_at).total_seconds())
                    if operation.succeeded_at
                    else -1,
                )
            yield successful
            yield age
            pool_age = GaugeMetricFamily(
                "strata_pool_controller_age_seconds",
                "Age of enabled host pool controller heartbeat",
                labels=["pool"],
            )
            pool_failures = GaugeMetricFamily(
                "strata_pool_error",
                "Host pool has an unresolved reconcile error",
                labels=["pool"],
            )
            for pool in session.scalars(select(WorkerPool).where(WorkerPool.enabled.is_(True))):
                pool_age.add_metric([pool.id], max(0, (now - pool.last_seen_at).total_seconds()))
                pool_failures.add_metric([pool.id], int(pool.last_error is not None))
            yield pool_age
            yield pool_failures
            pool_workers = GaugeMetricFamily(
                "strata_provisioned_workers",
                "Durable host provisioning intents",
                labels=["pool", "phase"],
            )
            for pool_id, phase, count in session.execute(
                select(ProvisionedWorker.pool_id, ProvisionedWorker.phase, func.count()).group_by(
                    ProvisionedWorker.pool_id, ProvisionedWorker.phase
                )
            ):
                pool_workers.add_metric([pool_id, phase], count)
            yield pool_workers
            outbox = GaugeMetricFamily(
                "strata_webhook_deliveries", "Durable webhook delivery state", labels=["status"]
            )
            for status, count in session.execute(
                select(WebhookDelivery.status, func.count()).group_by(WebhookDelivery.status)
            ):
                outbox.add_metric([status], count)
            yield outbox
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
