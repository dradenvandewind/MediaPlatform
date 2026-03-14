"""
BaseWorker – classe abstraite commune à tous les nœuds du pipeline.
Fournit : publish vers l'orchestrateur, checkpoint S3 en cas d'interruption.
"""
import asyncio
import json
import logging
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

import aiohttp

from processing_pipeline.shared.config import ORCHESTRATOR_URL, S3_BUCKET, S3_REGION
from processing_pipeline.shared.s3 import S3Manager

log = logging.getLogger(__name__)


class BaseWorker(ABC):
    def __init__(
        self,
        node_type:        str,
        s3_bucket:        str = S3_BUCKET,
        s3_region:        str = S3_REGION,
        orchestrator_url: str = ORCHESTRATOR_URL,
    ):
        self.node_type        = node_type
        self.s3_bucket        = s3_bucket
        self.s3_region        = s3_region
        self.orchestrator_url = orchestrator_url

    # ------------------------------------------------------------------ #
    #  To implement                                                        #
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def run(self, job_id: str, job_data: dict) -> dict:
        ...

    # ------------------------------------------------------------------ #
    #  Shared helpers                                                      #
    # ------------------------------------------------------------------ #

    async def publish(self, event: dict) -> None:
        job_id = event.get("job_id")
        if not job_id:
            log.warning("publish: no job_id in event %s", event)
            return

        event_type = event.get("event", "")

        if event_type.endswith(".completed"):
            url  = f"{self.orchestrator_url}/jobs/{job_id}/next"
            body = {
                "stage":  self.node_type,
                "result": event.get("result", {}),
            }
        elif event_type.endswith(".failed"):
            url  = f"{self.orchestrator_url}/jobs/{job_id}/failed"
            body = {
                "stage": self.node_type,
                "error": event.get("error", "Unknown error"),
            }
        else:
            log.debug("publish: unhandled event type %s", event_type)
            return

        async with aiohttp.ClientSession() as http:
            try:
                resp = await http.post(
                    url,
                    json=body,
                    timeout=aiohttp.ClientTimeout(total=5),
                )
                log.info("publish %s → %s (HTTP %s)", event_type, url, resp.status)
            except Exception as exc:
                log.warning("publish failed (non-blocking): %s", exc)

    async def checkpoint(self, job_id: str, job_data: dict) -> None:
        """Sauvegarde l'état du job sur S3 pour reprise après interruption."""
        payload = {
            "job_id":    job_id,
            "node_type": self.node_type,
            "job_data":  job_data,
            "timestamp": asyncio.get_event_loop().time(),
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            json.dump(payload, tmp)
            tmp_path = tmp.name

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            await s3.upload(tmp_path, f"checkpoints/{self.node_type}/{job_id}.json")

        Path(tmp_path).unlink(missing_ok=True)
        log.info("[%s] checkpoint saved for job %s", self.node_type, job_id)

    async def consume(self) -> None:
        """Lit en continu le stream Redis jobs:{node_type}"""
        import redis.asyncio as aioredis
        from processing_pipeline.shared.config import REDIS_URL

        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        stream  = f"jobs:{self.node_type}"
        group   = f"workers-{self.node_type}"
        consumer = f"{self.node_type}-1"

        # Créer le groupe si besoin
        try:
            await r.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception:
            pass  # BUSYGROUP déjà existant

        while True:
            msgs = await r.xreadgroup(
                group, consumer, {stream: ">"}, count=1, block=5000
            )
            if not msgs:
                continue
            for _, messages in msgs:
                for msg_id, fields in messages:
                    try:
                        job_id   = fields["job_id"]
                        job_data = json.loads(fields.get("input", "{}"))
                        await self.run(job_id, job_data)
                        await r.xack(stream, group, msg_id)
                    except Exception as exc:
                        log.error("consume error: %s", exc)