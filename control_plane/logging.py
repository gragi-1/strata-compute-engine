import json
import logging
import re
from datetime import UTC, datetime


def database_failure(exc: BaseException) -> str:
    """Useful fault classification without SQL, connection URLs or driver error messages."""
    original = getattr(exc, "orig", None)
    code = getattr(original, "sqlstate", None)
    if isinstance(code, str) and re.fullmatch(r"[0-9A-Z]{5}", code):
        return f"{type(exc).__name__} sqlstate={code}"
    return type(exc).__name__


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        for key in ("job_id", "attempt_id", "worker_id"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
