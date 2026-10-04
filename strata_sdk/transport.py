"""Bounded retries only for reads and explicitly replayable Strata operations."""

import math
import random
import re
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

KEYED_SUBMISSIONS = re.compile(
    r"/(jobs|campaigns|workflows|experiments/[^/]+/runs|experiment-runs/[^/]+/replay|"
    r"compute-groups(?:/[^/]+/retry)?|interactive-sessions(?:/[^/]+/(?:cells|proxy))?)"
)
DATASET_READS = re.compile(r"/dataset-files/[^/]+/(query|statistics)")
CHUNKS = re.compile(r"/uploads/[^/]+/chunks/[0-9]+")


def replay_safe(method: str, path: str, headers: httpx.Headers) -> bool:
    if method in {"GET", "HEAD", "OPTIONS"}:
        return True
    if method == "POST":
        return bool(
            DATASET_READS.fullmatch(path)
            or KEYED_SUBMISSIONS.fullmatch(path)
            and headers.get("Idempotency-Key")
            or re.fullmatch(r"/uploads/[^/]+/complete", path)
        )
    return bool(method == "PUT" and CHUNKS.fullmatch(path) and headers.get("X-Chunk-SHA256"))


def retry_delay(attempt: int, response: httpx.Response | None = None) -> float:
    delay: float = min(2.0, 0.25 * 2**attempt) + random.uniform(0, 0.1)
    if response is None or not (value := response.headers.get("Retry-After")):
        return delay
    try:
        if value.isdigit():
            return max(delay, float(value))
        deadline = parsedate_to_datetime(value)
        if deadline.tzinfo is not None:
            return max(delay, (deadline - datetime.now(UTC)).total_seconds())
    except (ValueError, OverflowError):
        pass
    return delay


def request_with_retry(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    max_retries: int = 3,
    retry_window_seconds: float = 10,
    **kwargs: Any,
) -> httpx.Response:
    if not isinstance(max_retries, int) or not 0 <= max_retries <= 10:
        raise ValueError("max_retries must be 0..10")
    if not math.isfinite(retry_window_seconds) or not 0 <= retry_window_seconds <= 300:
        raise ValueError("retry_window_seconds must be finite and 0..300")
    headers = client.headers.copy()
    headers.update(kwargs.get("headers") or {})
    method = method.upper()
    # A generator, file or multipart stream cannot be replayed without a separate resume contract.
    reusable = (
        kwargs.get("files") is None
        and kwargs.get("data") is None
        and (kwargs.get("content") is None or isinstance(kwargs["content"], (bytes, str)))
    )
    safe = reusable and replay_safe(method, path, headers)
    deadline = time.monotonic() + retry_window_seconds
    for attempt in range(max_retries + 1):
        response = None
        try:
            response = client.request(method, path, **kwargs)
        except httpx.TransportError:
            delay = retry_delay(attempt)
            if not safe or attempt >= max_retries or time.monotonic() + delay >= deadline:
                raise
        else:
            if response.status_code not in {429, 502, 503, 504} or not safe:
                return response
            delay = retry_delay(attempt, response)
            if attempt >= max_retries or time.monotonic() + delay >= deadline:
                return response
            response.close()
        time.sleep(delay)
    raise RuntimeError("retry loop ended unexpectedly")
