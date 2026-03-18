"""
shared/metrics.py
=================
Custom Prometheus metrics for the OTT pipeline.
Each node exposes:
  - ott_jobs_total               (counter)   jobs received by status
  - ott_job_duration_seconds     (histogram) processing duration
  - ott_jobs_in_progress         (gauge)     jobs in progress
  - ott_s3_upload_bytes          (counter)   bytes uploaded to S3
  - ott_s3_operations_total      (counter)   S3 operations by type/status
  - ott_ffmpeg_duration_seconds  (histogram) duration of ffmpeg calls
  - ott_redis_queue_length       (gauge)     messages pending by stream
  - ott_redis_queue_pending      (gauge)     pending messages (non-ACKed) by stream

Lightweight Python metrics (no CPU/RAM):
  - python_gc_collections_total  (counter)   GC passes by generation
  - python_gc_objects_collected  (counter)   objects collected by generation
  - python_threads_total         (gauge)     active threads
  - python_open_fds              (gauge)     open file descriptors
  - python_asyncio_tasks_total   (gauge)     ongoing asyncio tasks

Note: prometheus_client process collectors (CPU/RSS) are disabled
      via REGISTRY.unregister() at startup to avoid any overhead.
"""

import gc
import threading
import time
from functools import wraps

from prometheus_client import Counter, Gauge, Histogram, REGISTRY
from prometheus_client import ProcessCollector, PlatformCollector, GCCollector,REGISTRY

# ProcessCollector(registry=REGISTRY)
# PlatformCollector(registry=REGISTRY)  #PYTHON

# ---------------------------------------------------------------------------
# Disable default resource-intensive collectors (CPU / RAM / platform)
# ---------------------------------------------------------------------------

def _disable_default_collectors() -> None:
    """
    Removes ProcessCollector (cpu, rss, fds via /proc) and PlatformCollector
    (system info) from the global registry.
    GCCollector is also removed as we replace it with our own gauges.
    Each unregister is in its own try/except: if a collector is not
    registered (tests, multiple imports) we continue silently.
    """
    for collector_cls in (ProcessCollector, PlatformCollector, GCCollector):
        try:
            REGISTRY.unregister(collector_cls(registry=None))
        except Exception:
            pass
    # Fallback: direct registry traversal if constructors fail
    to_remove = []
    for collector in list(REGISTRY._names_to_collectors.values()):
        name = type(collector).__name__
        if name in ("ProcessCollector", "PlatformCollector", "GCCollector"):
            to_remove.append(collector)
    for collector in set(to_remove):
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass

_disable_default_collectors()

# ---------------------------------------------------------------------------
# Custom OTT metrics
# ---------------------------------------------------------------------------

JOBS_TOTAL = Counter(
    "ott_jobs_total",
    "Total number of jobs processed",
    ["node", "status"],          # status: accepted | completed | failed
)

JOB_DURATION = Histogram(
    "ott_job_duration_seconds",
    "Job processing duration (seconds)",
    ["node"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800],
)

JOBS_IN_PROGRESS = Gauge(
    "ott_jobs_in_progress",
    "Jobs currently being processed",
    ["node"],
)

S3_UPLOAD_BYTES = Counter(
    "ott_s3_upload_bytes_total",
    "Bytes uploaded to S3",
    ["node", "bucket"],
)

S3_OPS_TOTAL = Counter(
    "ott_s3_operations_total",
    "S3 operations (upload/download/head) by status",
    ["node", "operation", "status"],  # status: success | error
)

FFMPEG_DURATION = Histogram(
    "ott_ffmpeg_duration_seconds",
    "Duration of ffmpeg/ffprobe calls",
    ["node", "operation"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

CHECKPOINT_TOTAL = Counter(
    "ott_checkpoints_total",
    "S3 checkpoints created after an interruption",
    ["node"],
)

REDIS_QUEUE_LENGTH = Gauge(
    "ott_redis_queue_length",
    "Total number of messages in the Redis stream (xlen)",
    ["node", "stream"],
)

REDIS_QUEUE_PENDING = Gauge(
    "ott_redis_queue_pending",
    "Pending non-ACKed messages in the consumer group",
    ["node", "stream", "group"],
)

# ---------------------------------------------------------------------------
# Lightweight Python metrics (GC + threads + fds + asyncio)
# Reading from in-memory counters → zero syscall /proc
# ---------------------------------------------------------------------------

PYTHON_GC_COLLECTIONS = Counter(
    "python_gc_collections_total",
    "Number of garbage collector passes by generation",
    ["generation"],
)

PYTHON_GC_OBJECTS_COLLECTED = Counter(
    "python_gc_objects_collected_total",
    "Objects collected by the GC by generation",
    ["generation"],
)

PYTHON_THREADS = Gauge(
    "python_threads_total",
    "Number of active Python threads",
)

PYTHON_OPEN_FDS = Gauge(
    "python_open_fds",
    "Number of open file descriptors (via /proc/self/fd, best-effort)",
)

PYTHON_ASYNCIO_TASKS = Gauge(
    "python_asyncio_tasks_total",
    "Number of asyncio tasks currently running",
)


def collect_python_metrics() -> None:
    """
    Refreshes the lightweight Python metrics.
    To be called periodically (e.g., every 30s) in an asyncio task.
    No heavy syscalls — reading Python internal counters only,
    except /proc/self/fd (listed but not read).
    """
    # ── GC: in-memory stats, no I/O ────────────────────────────────────
    for gen, stats in enumerate(gc.get_stats()):
        PYTHON_GC_COLLECTIONS.labels(generation=str(gen)).inc(
            stats.get("collections", 0)
        )
        PYTHON_GC_OBJECTS_COLLECTED.labels(generation=str(gen)).inc(
            stats.get("collected", 0)
        )

    # ── Threads: threading.active_count() = simple counter ────────────────
    PYTHON_THREADS.set(threading.active_count())

    # ── File descriptors: listdir /proc/self/fd (best-effort, very fast) ─
    try:
        import os
        fd_count = len(os.listdir("/proc/self/fd"))
        PYTHON_OPEN_FDS.set(fd_count)
    except Exception:
        pass  # non-Linux ou permission refusée → on ignore

    # ── Asyncio tasks: reading the current loop ──────────────────────────────
    try:
        import asyncio
        loop = asyncio.get_running_loop()
        PYTHON_ASYNCIO_TASKS.set(len(asyncio.all_tasks(loop)))
    except RuntimeError:
        pass  # appelé hors d'une boucle asyncio → on ignore


async def python_metrics_task(interval: int = 30) -> None:
    """
    Asyncio task to launch at worker startup.
    Refreshes Python metrics every `interval` seconds.

    Usage dans lifespan FastAPI :
        asyncio.create_task(python_metrics_task())
    """
    import asyncio
    while True:
        collect_python_metrics()
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class JobTracker:
    """Context manager async pour tracker durée + statut d'un job."""

    def __init__(self, node: str):
        self.node   = node
        self._start: float = 0.0

    async def __aenter__(self):
        self._start = time.monotonic()
        JOBS_IN_PROGRESS.labels(node=self.node).inc()
        JOBS_TOTAL.labels(node=self.node, status="accepted").inc()
        return self

    async def __aexit__(self, exc_type, *_):
        elapsed = time.monotonic() - self._start
        JOB_DURATION.labels(node=self.node).observe(elapsed)
        JOBS_IN_PROGRESS.labels(node=self.node).dec()
        status = "failed" if exc_type else "completed"
        JOBS_TOTAL.labels(node=self.node, status=status).inc()


def track_ffmpeg(node: str, operation: str):
    """Décorateur pour mesurer la durée d'un appel ffmpeg."""
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return await fn(*args, **kwargs)
            finally:
                FFMPEG_DURATION.labels(
                    node=node, operation=operation
                ).observe(time.monotonic() - start)
        return wrapper
    return decorator


def track_s3(node: str, operation: str, bucket: str = ""):
    """Décorateur pour tracker les opérations S3."""
    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                result = await fn(*args, **kwargs)
                S3_OPS_TOTAL.labels(
                    node=node, operation=operation, status="success"
                ).inc()
                return result
            except Exception:
                S3_OPS_TOTAL.labels(
                    node=node, operation=operation, status="error"
                ).inc()
                raise
        return wrapper
    return decorator


async def update_queue_metrics(r, node: str, stream: str, group: str) -> None:
    """
    Met à jour les gauges Redis queue + pending.
    À appeler dans la boucle consume() de BaseWorker.

    Args:
        r      : client redis.asyncio
        node   : nom du worker (ex: 'transcoder')
        stream : nom du stream Redis (ex: 'jobs:transcoder')
        group  : nom du consumer group (ex: 'workers-transcoder')
    """
    try:
        length = await r.xlen(stream)
        REDIS_QUEUE_LENGTH.labels(node=node, stream=stream).set(length)
    except Exception:
        pass

    try:
        info = await r.xpending(stream, group)
        pending = info.get("pending", 0) if isinstance(info, dict) else 0
        REDIS_QUEUE_PENDING.labels(node=node, stream=stream, group=group).set(pending)
    except Exception:
        pass