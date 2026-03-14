"""
app_factory – construit une FastAPI autour de n'importe quel BaseWorker.
Routes communes : GET /health, POST /process, GET /metrics.
"""
import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from starlette.responses import Response

from processing_pipeline.shared.metrics import CHECKPOINT_TOTAL, JOBS_TOTAL, JobTracker
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)


# ---- Commun Schemas  -------------------------------------------------------

class ProcessRequest(BaseModel):
    job_id:  str | None = None
    payload: dict       = {}


class ProcessResponse(BaseModel):
    status: str
    job_id: str
    detail: str | None = None


class HealthResponse(BaseModel):
    status: str
    node:   str


# ---- Factory ------------------------------------------------------------

def create_worker_app(worker: BaseWorker, **fastapi_kwargs) -> FastAPI:
    """
    Enveloppe un BaseWorker dans une application FastAPI.
    Les kwargs supplémentaires sont passés à FastAPI() (title, version…).
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("[%s] starting up", worker.node_type)
        # ← lancer le consumer ici, dans l'event loop uvicorn
        consumer_task = asyncio.create_task(worker.consume())
        yield
        # ← arrêt propre
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        log.info("[%s] shutting down", worker.node_type)

    app = FastAPI(lifespan=lifespan, **fastapi_kwargs)

    # ── Prometheus HTTP instrumentation (latences, codes HTTP…) ─────────────
    Instrumentator(
        should_group_status_codes=True,
        excluded_handlers=["/metrics", "/health"],
    ).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

    # ── Routes ──────────────────────────────────────────────────────────────

    @app.get("/health", response_model=HealthResponse, tags=["ops"])
    async def health():
        return HealthResponse(status="healthy", node=worker.node_type)

    @app.post(
        "/process",
        response_model=ProcessResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["worker"],
    )
    async def process(req: ProcessRequest, background_tasks: BackgroundTasks):
        job_id = req.job_id or str(uuid.uuid4())
        JOBS_TOTAL.labels(node=worker.node_type, status="accepted").inc()

        async def _task():
            async with JobTracker(worker.node_type):
                try:
                    result = await worker.run(job_id, req.payload)
                    await worker.publish({
                        "event":  f"{worker.node_type}.completed",
                        "job_id": job_id,
                        "result": result,
                    })
                except Exception as exc:
                    log.error("[%s] job %s failed: %s", worker.node_type, job_id, exc)
                    CHECKPOINT_TOTAL.labels(node=worker.node_type).inc()
                    await worker.checkpoint(job_id, req.payload)
                    await worker.publish({
                        "event":  f"{worker.node_type}.failed",
                        "job_id": job_id,
                        "error":  str(exc),
                    })
                    raise   # re-raise pour que JobTracker détecte l'échec

        background_tasks.add_task(_task)
        return ProcessResponse(status="accepted", job_id=job_id)

    return app
