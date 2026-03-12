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
        async with aiohttp.ClientSession() as http:
            try:
                await http.post(
                    f"{self.orchestrator_url}/publish",
                    json=event,
                    timeout=aiohttp.ClientTimeout(total=5),
                )
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
