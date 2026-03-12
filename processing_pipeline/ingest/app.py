"""
Point d'entrée FastAPI du nœud Ingest.

Routes spécifiques (en plus de /health et /process hérités) :
  POST /ingest  – API orientée domaine (payload typé IngestRequest)
"""
import uuid
import logging
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, status

from processing_pipeline.shared.app_factory import create_worker_app
from processing_pipeline.ingest.schemas import IngestRequest, IngestResponse
from processing_pipeline.ingest.worker import IngestWorker

log = logging.getLogger(__name__)


def create_ingest_app(worker: IngestWorker | None = None) -> FastAPI:
    worker = worker or IngestWorker()
    app    = create_worker_app(
        worker,
        title       = "OTT – Ingest Node",
        description = "Reçoit une source vidéo, la valide et la pousse vers S3.",
        version     = "1.0.0",
    )

    # Route métier enrichie (payload Pydantic fort)
    @app.post(
        "/ingest",
        response_model = IngestResponse,
        status_code    = status.HTTP_202_ACCEPTED,
        tags           = ["ingest"],
    )
    async def ingest(req: IngestRequest, background_tasks: BackgroundTasks):
        job_id = str(uuid.uuid4())

        async def _task():
            try:
                result = await worker.run(job_id, req.model_dump())
                await worker.publish({
                    "event":  "ingest.completed",
                    "job_id": job_id,
                    **result,
                })
            except Exception as exc:
                log.error("[%s] failed: %s", job_id, exc)
                await worker.checkpoint(job_id, req.model_dump())
                await worker.publish({
                    "event":  "ingest.failed",
                    "job_id": job_id,
                    "error":  str(exc),
                })

        background_tasks.add_task(_task)
        return IngestResponse(status="accepted", job_id=job_id)

    return app
