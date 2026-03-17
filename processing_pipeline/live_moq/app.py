"""
app.py – FastAPI du nœud live_ingest.

Routes héritées (create_worker_app) :
  GET  /health
  POST /process       ← démarre un ingest live en background
  GET  /metrics

Routes spécifiques au live :
  GET  /live                   → liste tous les streams actifs
  GET  /live/{stream_id}       → statut détaillé d'un stream
  POST /live/{stream_id}/stop  → arrêt propre d'un stream
"""
import uuid
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, status
from pydantic import BaseModel, Field

from processing_pipeline.shared.app_factory import create_worker_app
from processing_pipeline.live_ingest.worker import LiveIngestWorker

log = logging.getLogger(__name__)


# ── Schémas ───────────────────────────────────────────────────────────────────

class LiveStartRequest(BaseModel):
    """Corps du POST /live/start (API orientée domaine)."""
    namespace: str = Field(
        ...,
        description="Namespace MoQ du publisher, ex: 'live/my-channel'",
        examples=["live/breaking-news"],
    )
    tracks: list[str] = Field(
        default=["video", "audio"],
        description="Tracks MoQ à souscrire",
    )
    duration: Optional[int] = Field(
        default=None,
        ge=10,
        le=86400,
        description="Durée max d'ingestion en secondes (défaut : 12h)",
    )
    stream_id: Optional[str] = Field(
        default=None,
        description="Identifiant métier (généré si absent)",
    )


class LiveStartResponse(BaseModel):
    status:    str
    stream_id: str
    job_id:    str
    message:   str


class LiveStatusResponse(BaseModel):
    stream_id:  str
    job_id:     str
    namespace:  str
    tracks:     list[str]
    segments:   int
    bytes_total: int
    uptime_s:   float
    error:      Optional[str] = None


class LiveStopResponse(BaseModel):
    stream_id: str
    stopped:   bool
    message:   str


# ── Factory ───────────────────────────────────────────────────────────────────

def create_live_ingest_app(worker: LiveIngestWorker | None = None) -> FastAPI:
    worker = worker or LiveIngestWorker()

    app = create_worker_app(
        worker,
        title       = "OTT – Live Ingest Node (MoQ-rs)",
        description = (
            "Ingère des flux live via Media over QUIC Transport (moq-rs). "
            "Lance moq-relay en local et souscrit aux tracks du publisher."
        ),
        version = "1.0.0",
    )

    # ── GET /live ─────────────────────────────────────────────────────────────

    @app.get(
        "/live",
        summary        = "Lister les streams actifs",
        tags           = ["live"],
    )
    async def list_streams():
        return {
            "count":   len(worker.list_streams()),
            "streams": worker.list_streams(),
        }

    # ── POST /live/start ──────────────────────────────────────────────────────

    @app.post(
        "/live/start",
        response_model = LiveStartResponse,
        status_code    = status.HTTP_202_ACCEPTED,
        summary        = "Démarrer l'ingestion d'un flux live",
        tags           = ["live"],
    )
    async def start_live(req: LiveStartRequest, background_tasks: BackgroundTasks):
        job_id    = str(uuid.uuid4())
        stream_id = req.stream_id or f"stream-{uuid.uuid4().hex[:8]}"

        job_data = {
            "stream_id": stream_id,
            "namespace": req.namespace,
            "tracks":    req.tracks,
        }
        if req.duration:
            job_data["duration"] = req.duration

        async def _task():
            try:
                result = await worker.run(job_id, job_data)
                await worker.publish({
                    "event":  "live_ingest.completed",
                    "job_id": job_id,
                    "result": result,
                })
            except Exception as exc:
                log.error("[%s] live ingest failed: %s", job_id, exc)
                await worker.checkpoint(job_id, job_data)
                await worker.publish({
                    "event":  "live_ingest.failed",
                    "job_id": job_id,
                    "error":  str(exc),
                })

        background_tasks.add_task(_task)
        return LiveStartResponse(
            status    = "accepted",
            stream_id = stream_id,
            job_id    = job_id,
            message   = (
                f"Ingestion démarrée – connectez votre publisher sur "
                f"moqt://{{HOST}}:4443 avec le namespace '{req.namespace}'"
            ),
        )

    # ── GET /live/{stream_id} ─────────────────────────────────────────────────

    @app.get(
        "/live/{stream_id}",
        response_model = LiveStatusResponse,
        summary        = "Statut d'un stream actif",
        tags           = ["live"],
    )
    async def get_stream(stream_id: str):
        state = worker.get_stream_state(stream_id)
        if state is None:
            raise HTTPException(
                status_code = 404,
                detail      = f"Stream '{stream_id}' not found or already terminated",
            )
        import time
        return LiveStatusResponse(
            stream_id   = state.stream_id,
            job_id      = state.job_id,
            namespace   = state.namespace,
            tracks      = state.tracks,
            segments    = len(state.segments),
            bytes_total = state.bytes_total,
            uptime_s    = round(time.time() - state.started_at, 1),
            error       = state.error,
        )

    # ── POST /live/{stream_id}/stop ────────────────────────────────────────────

    @app.post(
        "/live/{stream_id}/stop",
        response_model = LiveStopResponse,
        summary        = "Arrêter proprement un stream",
        tags           = ["live"],
    )
    async def stop_stream(stream_id: str):
        stopped = await worker.stop_stream(stream_id)
        if not stopped:
            raise HTTPException(
                status_code = 404,
                detail      = f"Stream '{stream_id}' not found or already stopped",
            )
        return LiveStopResponse(
            stream_id = stream_id,
            stopped   = True,
            message   = "Stop signal sent – final segments will be flushed to S3",
        )

    return app