"""
Orchestrateur central – logique métier découplée de FastAPI
"""

import asyncio
import json
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from .models import JobStatus


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
    STATUS_KEY_PREFIX = "job-status"

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis: aioredis.Redis | None = None

    # ── Cycle de vie ──────────────────────────────────────────────────────────

    async def startup(self):
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        print(f"✅ Orchestrator connected to Redis at {self.redis_url}")
        await self.initialize_consumer_groups()
        asyncio.create_task(self.monitor_jobs())

    async def shutdown(self):
        if self.redis:
            await self.redis.aclose()
            print("🔌 Redis connection closed")

    # ── Consumer groups ───────────────────────────────────────────────────────

    async def initialize_consumer_groups(self):
        for stage in self.PIPELINE_STAGES[:-1]:
            stream_name = f"{self.STREAM_PREFIX}:{stage}"
            group_name = f"workers-{stage}"
            try:
                await self.redis.xgroup_create(
                    stream_name, group_name, id="0", mkstream=True
                )
                print(f"📊 Consumer group ready: {group_name} → {stream_name}")
            except aioredis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    print(f"⚠️  Error creating group {group_name}: {e}")

    # ── Redis helpers ─────────────────────────────────────────────────────────

    async def save_job_status(self, status: JobStatus) -> None:
        key = f"{self.STATUS_KEY_PREFIX}:{status.job_id}"
        await self.redis.set(key, json.dumps(asdict(status)), ex=7 * 24 * 3600)

    async def load_job_status(self, job_id: str) -> Optional[JobStatus]:
        key = f"{self.STATUS_KEY_PREFIX}:{job_id}"
        value = await self.redis.get(key)
        return JobStatus(**json.loads(value)) if value else None

    async def send_to_stage(self, stage: str, job_data: Dict[str, Any]) -> None:
        stream_name = f"{self.STREAM_PREFIX}:{stage}"
        fields = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in job_data.items()
        }
        message_id = await self.redis.xadd(stream_name, fields, maxlen=10000)
        print(f"📤 Job {job_data.get('job_id')} → '{stage}' (msg {message_id})")

    def get_next_stage(self, current_stage: str) -> Optional[str]:
        try:
            idx = self.PIPELINE_STAGES.index(current_stage)
            if idx < len(self.PIPELINE_STAGES) - 1:
                return self.PIPELINE_STAGES[idx + 1]
        except ValueError:
            pass
        return None

    # ── Monitoring background ─────────────────────────────────────────────────

    async def monitor_jobs(self):
        """Détecte les jobs bloqués et relance si nécessaire (toutes les 60s)."""
        while True:
            try:
                await asyncio.sleep(60)
                # TODO: vérifier les jobs 'processing' depuis > 30 min
                #       et les pending messages non consommés
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Monitor error: {e}")

    # ── Stats ─────────────────────────────────────────────────────────────────

    async def collect_stats(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "streams": {},
            "jobs_by_status": {
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
            },
        }

        for stage in self.PIPELINE_STAGES[:-1]:
            stream_name = f"{self.STREAM_PREFIX}:{stage}"
            try:
                info = await self.redis.xinfo_stream(stream_name)
                length = info.get("length", 0)
            except Exception:
                length = 0
            stats["streams"][stage] = {"queue_length": length, "stream": stream_name}

        cursor = 0
        pattern = f"{self.STATUS_KEY_PREFIX}:*"
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
            for key in keys:
                raw = await self.redis.get(key)
                if raw:
                    job_status = json.loads(raw).get("status", "unknown")
                    if job_status in stats["jobs_by_status"]:
                        stats["jobs_by_status"][job_status] += 1
            if cursor == 0:
                break

        return stats
