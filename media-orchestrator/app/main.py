"""
FastAPI application entry point
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .orchestrator import MediaOrchestrator
from .routes import jobs_router, monitoring_router
from .settings import Settings

settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    orc = MediaOrchestrator(redis_url=settings.redis_url)
    app.state.orchestrator = orc
    await orc.startup()
    yield
    await orc.shutdown()


app = FastAPI(
    title="Media Platform Orchestrator",
    description="Central orchestrator – distribute jobs via Redis Streams, manage job lifecycle, and provide monitoring endpoints.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(jobs_router)
app.include_router(monitoring_router)
