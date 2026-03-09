"""
FastAPI application entry point
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .orchestrator import MediaOrchestrator
from .routes import jobs_router, monitoring_router
from .settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger   = logging.getLogger(__name__)
settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    orc = MediaOrchestrator(
        redis_url    = settings.redis_url,
        database_url = settings.database_url,
    )
    app.state.orchestrator = orc
    await orc.startup()
    yield
    await orc.shutdown()


app = FastAPI(
    title       = "Media Platform Orchestrator",
    description = "Central orchestrator – Redis Streams + PostgreSQL",
    version     = "2.0.0",
    lifespan    = lifespan,
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": type(exc).__name__},
    )


app.include_router(jobs_router)
app.include_router(monitoring_router)