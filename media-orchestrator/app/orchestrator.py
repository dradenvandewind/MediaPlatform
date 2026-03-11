"""
Orchestrateur central – Redis Streams pour les queues, PostgreSQL pour l'état
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from sqlalchemy import delete, select, update

from .database import JobRecord, JobStatusEnum, create_tables, dispose_engine, get_session, init_engine
from .models import JobStatus

logger = logging.getLogger(__name__)


class OrchestratorError(Exception):
    """Erreur métier de l'orchestrateur"""


class JobNotFoundError(OrchestratorError):
    def __init__(self, job_id: str):
        super().__init__(f"Job '{job_id}' not found")
        self.job_id = job_id


class MediaOrchestrator:
    PIPELINE_STAGES = [
        "ingest",
        "metadata-extraction",
        "media-processing",
        "transcoding",
        "audio-encoding",
        "subtitle-processing",
        "watermarking",
        "drm-encryption",
        "muxing",
        "packaging",
        "completed",
    ]

    STREAM_PREFIX = "jobs"

    def __init__(self, redis_url: str, database_url: str):
        self.redis_url    = redis_url
        self.database_url = database_url
        self.redis: aioredis.Redis | None = None

    # ── Cycle de vie ──────────────────────────────────────────────────────────

    async def startup(self):
        # PostgreSQL
        init_engine(self.database_url)
        await create_tables()
        logger.info("✅ PostgreSQL ready")

        # Redis
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        logger.info("✅ Redis ready at %s", self.redis_url)

        await self._initialize_consumer_groups()
        asyncio.create_task(self._monitor_jobs())

    async def shutdown(self):
        if self.redis:
            await self.redis.aclose()
        await dispose_engine()
        logger.info("🔌 Connections closed")

    # ── Consumer groups ───────────────────────────────────────────────────────

    async def _initialize_consumer_groups(self):
        for stage in self.PIPELINE_STAGES[:-1]:
            stream = f"{self.STREAM_PREFIX}:{stage}"
            group  = f"workers-{stage}"
            try:
                await self.redis.xgroup_create(stream, group, id="0", mkstream=True)
                logger.debug("Consumer group ready: %s → %s", group, stream)
            except aioredis.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    logger.warning("Could not create group %s: %s", group, exc)

    # ── PostgreSQL helpers ────────────────────────────────────────────────────

    async def save_job(self, status: JobStatus) -> None:
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(JobRecord).where(JobRecord.job_id == status.job_id)
                )
                record = result.scalar_one_or_none()
                now = datetime.now(timezone.utc)
                if record is None:
                    record = JobRecord(
                        job_id        = status.job_id,
                        status        = JobStatusEnum(status.status),
                        current_stage = status.current_stage,
                        updated_at    = now,
                        metadata_json = json.dumps(status.metadata),
                        results_json  = json.dumps(status.results),
                        error         = status.error,
                    )
                    session.add(record)
                else:
                    record.status        = JobStatusEnum(status.status)
                    record.current_stage = status.current_stage
                    record.updated_at    = now
                    record.results_json  = json.dumps(status.results)
                    record.error         = status.error
                await session.commit()
        except OrchestratorError:
            raise
        except Exception as exc:
            logger.error("DB save failed for job %s: %s", status.job_id, exc)
            raise OrchestratorError(f"Failed to save job {status.job_id}") from exc

    async def load_job(self, job_id: str) -> Optional[JobStatus]:
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(JobRecord).where(JobRecord.job_id == job_id)
                )
                record = result.scalar_one_or_none()
                if record is None:
                    return None
                return self._record_to_status(record)
        except OrchestratorError:
            raise
        except Exception as exc:
            logger.error("DB load failed for job %s: %s", job_id, exc)
            raise OrchestratorError(f"Failed to load job {job_id}") from exc

    async def delete_job(self, job_id: str) -> None:
        try:
            async with get_session() as session:
                await session.execute(
                    delete(JobRecord).where(JobRecord.job_id == job_id)
                )
                await session.commit()
                logger.info("🗑️  Job %s deleted from DB", job_id)
        except OrchestratorError:
            raise
        except Exception as exc:
            logger.error("DB delete failed for job %s: %s", job_id, exc)
            raise OrchestratorError(f"Failed to delete job {job_id}") from exc

    async def list_jobs(self, status_filter: Optional[str] = None) -> List[Dict]:
        try:
            async with get_session() as session:
                stmt = select(JobRecord).order_by(JobRecord.created_at.desc())
                if status_filter:
                    stmt = stmt.where(JobRecord.status == JobStatusEnum(status_filter))
                result  = await session.execute(stmt)
                records = result.scalars().all()
                return [self._record_to_dict(r) for r in records]
        except OrchestratorError:
            raise
        except Exception as exc:
            logger.error("DB list failed: %s", exc)
            raise OrchestratorError("Failed to list jobs") from exc
    
    # ── Redis Stream helpers ──────────────────────────────────────────────────

    async def send_to_stage(self, stage: str, job_data: Dict[str, Any]) -> None:
        stream = f"{self.STREAM_PREFIX}:{stage}"
        fields = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in job_data.items()
        }
        try:
            msg_id = await self.redis.xadd(stream, fields, maxlen=10000)
            logger.info("📤 Job %s → stage '%s' (msg %s)", job_data.get("job_id"), stage, msg_id)
        except Exception as exc:
            logger.error("Redis xadd failed for stage %s: %s", stage, exc)
            raise OrchestratorError(f"Failed to enqueue job to stage '{stage}'") from exc

    def get_next_stage(self, current_stage: str) -> Optional[str]:
        try:
            idx = self.PIPELINE_STAGES.index(current_stage)
            if idx < len(self.PIPELINE_STAGES) - 1:
                return self.PIPELINE_STAGES[idx + 1]
        except ValueError:
            logger.warning("Unknown stage: %s", current_stage)
        return None

    # ── Stats ─────────────────────────────────────────────────────────────────

    async def collect_stats(self) -> Dict[str, Any]:
        streams: Dict[str, Any] = {}
        for stage in self.PIPELINE_STAGES[:-1]:
            stream = f"{self.STREAM_PREFIX}:{stage}"
            try:
                info   = await self.redis.xinfo_stream(stream)
                length = info.get("length", 0)
            except Exception:
                length = 0
            streams[stage] = {"queue_length": length, "stream": stream}

        async with get_session() as session:
            counts: Dict[str, int] = {}
            for s in ("pending", "processing", "completed", "failed"):
                result = await session.execute(
                    select(JobRecord).where(JobRecord.status == JobStatusEnum(s))
                )
                counts[s] = len(result.scalars().all())

        return {"streams": streams, "jobs_by_status": counts}

    # ── Background monitor ────────────────────────────────────────────────────

    async def _monitor_jobs(self):
        while True:
            try:
                await asyncio.sleep(60)
                # TODO: détecter les jobs processing > 30 min et les relancer
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Monitor error: %s", exc)

    # ── Conversions ───────────────────────────────────────────────────────────

    @staticmethod
    def _record_to_status(r: JobRecord) -> JobStatus:
        return JobStatus(
            job_id        = r.job_id,
            status        = r.status.value,
            current_stage = r.current_stage,
            created_at    = r.created_at.isoformat() if r.created_at else "",
            updated_at    = r.updated_at.isoformat() if r.updated_at else "",
            metadata      = json.loads(r.metadata_json),
            results       = json.loads(r.results_json),
            error         = r.error,
        )

    @staticmethod
    def _record_to_dict(r: JobRecord) -> Dict:
        return {
            "job_id":        r.job_id,
            "status":        r.status.value,
            "current_stage": r.current_stage,
            "created_at":    r.created_at.isoformat() if r.created_at else "",
            "updated_at":    r.updated_at.isoformat() if r.updated_at else "",
            "metadata":      json.loads(r.metadata_json),
            "results":       json.loads(r.results_json),
            "error":         r.error,
        }