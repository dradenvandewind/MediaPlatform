"""Retry decorator asynchrone avec back-off exponentiel."""
import asyncio
import functools
import logging

from processing_pipeline.shared.config import MAX_RETRIES, RETRY_DELAY

log = logging.getLogger(__name__)


def async_retry(retries: int = MAX_RETRIES, delay: float = RETRY_DELAY):
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(1, retries + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    wait = delay * (2 ** (attempt - 1))
                    log.warning(
                        "[retry %d/%d] %s failed: %s – retry in %.1fs",
                        attempt, retries, fn.__name__, exc, wait,
                    )
                    await asyncio.sleep(wait)
            raise RuntimeError(f"{fn.__name__} failed after {retries} retries") from last_exc
        return wrapper
    return decorator
