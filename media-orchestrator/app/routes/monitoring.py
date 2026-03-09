"""
Routes /health et /stats
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from ..database import get_session
from ..orchestrator import MediaOrchestrator, OrchestratorError
from .deps import get_orchestrator

router = APIRouter(tags=["Monitoring"])


@router.get("/health")
async def health_check(orc: MediaOrchestrator = Depends(get_orchestrator)):
    redis_status = "disconnected"
    pg_status    = "disconnected"

    try:
        await orc.redis.ping()
        redis_status = "connected"
    except Exception:
        pass

    try:
        async with get_session() as session:
            await session.execute(text("SELECT 1"))
        pg_status = "connected"
    except Exception:
        pass

    overall = "healthy" if redis_status == "connected" and pg_status == "connected" else "degraded"

    return {
        "status":    overall,
        "service":   "media-orchestrator",
        "redis":     redis_status,
        "postgres":  pg_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/stats")
async def get_stats(orc: MediaOrchestrator = Depends(get_orchestrator)):
    try:
        return await orc.collect_stats()
    except OrchestratorError as exc:
        raise HTTPException(status_code=503, detail=str(exc))