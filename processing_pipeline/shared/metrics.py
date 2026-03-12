"""
shared/metrics.py
=================
Métriques Prometheus custom pour le pipeline OTT.

Chaque nœud expose :
  - ott_jobs_total          (counter)   jobs reçus par statut
  - ott_job_duration_seconds (histogram) durée de traitement
  - ott_jobs_in_progress    (gauge)     jobs en cours
  - ott_s3_upload_bytes     (counter)   octets uploadés vers S3
  - ott_s3_operations_total (counter)   opérations S3 par type/statut
  - ott_ffmpeg_duration_seconds (histogram) durée des appels ffmpeg
"""
import time
from contextlib import asynccontextmanager
from functools import wraps

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Définitions des métriques (singletons – ne jamais recréer)
# ---------------------------------------------------------------------------

JOBS_TOTAL = Counter(
    "ott_jobs_total",
    "Nombre total de jobs traités",
    ["node", "status"],          # status: accepted | completed | failed
)

JOB_DURATION = Histogram(
    "ott_job_duration_seconds",
    "Durée de traitement d'un job (secondes)",
    ["node"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800],
)

JOBS_IN_PROGRESS = Gauge(
    "ott_jobs_in_progress",
    "Jobs actuellement en cours de traitement",
    ["node"],
)

S3_UPLOAD_BYTES = Counter(
    "ott_s3_upload_bytes_total",
    "Octets uploadés vers S3",
    ["node", "bucket"],
)

S3_OPS_TOTAL = Counter(
    "ott_s3_operations_total",
    "Opérations S3 (upload/download/head) par statut",
    ["node", "operation", "status"],  # status: success | error
)

FFMPEG_DURATION = Histogram(
    "ott_ffmpeg_duration_seconds",
    "Durée des appels ffmpeg/ffprobe",
    ["node", "operation"],            # operation: transcode_1080p | encode_audio | hls | …
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

CHECKPOINT_TOTAL = Counter(
    "ott_checkpoints_total",
    "Checkpoints S3 créés suite à une interruption",
    ["node"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class JobTracker:
    """Context manager async pour tracker durée + statut d'un job."""

    def __init__(self, node: str):
        self.node  = node
        self._start: float = 0.0

    async def __aenter__(self):
        self._start = time.monotonic()
        JOBS_IN_PROGRESS.labels(node=self.node).inc()
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
