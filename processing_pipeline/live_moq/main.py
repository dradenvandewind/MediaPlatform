import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
#from processing_pipeline.live_moq.worker import LiveIngestWorker
#from processing_pipeline.shared.app_factory import create_worker_app
from worker import LiveIngestWorker
from processing_pipeline.shared.app_factory import create_worker_app


worker = LiveIngestWorker()

@asynccontextmanager
async def lifespan(app):
    # Lance le consumer Redis en background comme les autres workers
    task = asyncio.create_task(worker.consume())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

app = create_worker_app(worker, title="OTT – Live Ingest Node (MoQ-rs)")
app.router.lifespan_context = lifespan