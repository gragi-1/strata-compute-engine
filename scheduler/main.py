import logging
import signal
import threading

from control_plane.config import Settings
from control_plane.database import make_engine, sessions
from control_plane.logging import configure_logging
from control_plane.services import EngineService
from control_plane.tracing import configure_tracing, tracer
from scheduler.core import Scheduler


def main() -> None:
    configure_logging()
    configure_tracing("strata-scheduler")
    settings = Settings()
    scheduler = Scheduler(EngineService(sessions(make_engine(settings.database_url)), settings))
    stopped = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    while not stopped.is_set():
        try:
            with tracer.start_as_current_span("scheduler.tick"):
                scheduler.tick()
        except Exception:
            logging.getLogger(__name__).exception("scheduler_tick_failed")
        stopped.wait(settings.scheduler_interval)


if __name__ == "__main__":
    main()
