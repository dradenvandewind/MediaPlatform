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

    def _build_job_data(self, fields: dict) -> dict:
        """Reconstruit job_data en fusionnant input + previous_results + champs directs."""
        job_data = json.loads(fields.get("input", "{}"))

        # ── Fusionner previous_results ──────────────────────────────────────
        prev_raw = fields.get("previous_results")
        if prev_raw:
            try:
                previous_results = json.loads(prev_raw)
                log.info("[%s] previous_results stages: %s", self.node_type, list(previous_results.keys()))
                # Aplatir chaque résultat d'étape dans job_data
                for stage, stage_result in previous_results.items():
                    if isinstance(stage_result, dict):
                        for key, value in stage_result.items():
                            job_data[key] = value
                            log.debug("[%s] injected from stage '%s': %s", self.node_type, stage, key)
            except Exception as exc:
                log.warning("[%s] could not parse previous_results: %s", self.node_type, exc)

        # ── Ajouter les champs directs du stream ────────────────────────────
        skip = {"job_id", "input", "current_stage", "previous_results"}
        for k, v in fields.items():
            #if k in skip:
            #    continue
            try:
                job_data[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                job_data[k] = v

        log.info("[%s] job_data keys after merge: %s", self.node_type, list(job_data.keys()))
        return job_data

    async def consume(self) -> None:
        import redis.asyncio as aioredis
        from processing_pipeline.shared.config import REDIS_URL
        log.info("[%s] consume() starting — REDIS_URL=%s", self.node_type, REDIS_URL)
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            await r.ping()
            log.info("[%s] Redis connected OK", self.node_type)
        except Exception as exc:
            log.error("[%s] Redis connection FAILED: %s", self.node_type, exc)
            raise

        stream   = f"jobs:{self.node_type}"
        group    = f"workers-{self.node_type}"
        consumer = f"{self.node_type}-1"
        log.info("[%s] listening on stream=%s group=%s", self.node_type, stream, group)

        try:
            await r.xgroup_create(stream, group, id="0", mkstream=True)
            log.info("[%s] consumer group created", self.node_type)
        except Exception as exc:
            log.debug("[%s] group already exists: %s", self.node_type, exc)

        # ── Pending messages : retraiter les messages non ACKés au démarrage ──
        log.info("[%s] checking pending messages...", self.node_type)
        try:
            pending_msgs = await r.xreadgroup(
                group, consumer, {stream: "0"}, count=10
            )
            if pending_msgs:
                log.info("[%s] found pending messages to reprocess", self.node_type)
                for _, messages in pending_msgs:
                    for msg_id, fields in messages:
                        job_id = fields.get("job_id", "unknown")
                        log.info("[%s] reprocessing pending job=%s", self.node_type, job_id)
                        try:
                            job_data = json.loads(fields.get("input", "{}"))
                            for k, v in fields.items():
                                if k in ("job_id", "input", "current_stage"):
                                    continue
                                try:
                                    job_data[k] = json.loads(v)
                                except (json.JSONDecodeError, TypeError):
                                    job_data[k] = v
                                    
                            job_data = self._build_job_data(fields)
                            
                            result = await self.run(job_id, self._build_job_data(fields))
                            await r.xack(stream, group, msg_id)
                            log.info("[%s] pending job=%s completed", self.node_type, job_id)
                            await self.publish({
                                "event":  f"{self.node_type}.completed",
                                "job_id": job_id,
                                "result": result,
                            })
                        except Exception as exc:
                            log.error("[%s] pending job=%s FAILED: %s", self.node_type, job_id, exc)
                            await r.xack(stream, group, msg_id)
                            await self.publish({
                                "event":  f"{self.node_type}.failed",
                                "job_id": job_id,
                                "error":  str(exc),
                            })
        except Exception as exc:
            log.warning("[%s] could not read pending messages: %s", self.node_type, exc)

        # ── Boucle principale : nouveaux messages ────────────────────────────
        while True:
            try:
                msgs = await r.xreadgroup(
                    group, consumer, {stream: ">"}, count=1, block=5000
                )
                if not msgs:
                    log.debug("[%s] no messages (timeout 5s), retrying...", self.node_type)
                    continue
                for _, messages in msgs:
                    for msg_id, fields in messages:
                        job_id = fields.get("job_id", "unknown")
                        log.info("[%s] received job=%s msg_id=%s", self.node_type, job_id, msg_id)
                        try:
                            job_data = json.loads(fields.get("input", "{}"))
                            for k, v in fields.items():
                                if k in ("job_id", "input", "current_stage"):
                                    continue
                                try:
                                    job_data[k] = json.loads(v)
                                except (json.JSONDecodeError, TypeError):
                                    job_data[k] = v
                            log.debug("[%s] job=%s data=%s", self.node_type, job_id, job_data)
                            result = await self.run(job_id, self._build_job_data(fields))
                            await r.xack(stream, group, msg_id)
                            log.info("[%s] job=%s completed, ACK sent", self.node_type, job_id)
                            await self.publish({
                                "event":  f"{self.node_type}.completed",
                                "job_id": job_id,
                                "result": result,
                            })
                        except Exception as exc:
                            log.error("[%s] job=%s FAILED: %s", self.node_type, job_id, exc, exc_info=True)
                            await self.publish({
                                "event":  f"{self.node_type}.failed",
                                "job_id": job_id,
                                "error":  str(exc),
                            })
            except asyncio.CancelledError:
                log.info("[%s] consume() cancelled, shutting down", self.node_type)
                raise
            except Exception as exc:
                log.error("[%s] consume() loop error: %s — retrying in 2s", self.node_type, exc)
                await asyncio.sleep(2)