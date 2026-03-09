"""
Routes /health et /stats
"""

from datetime import datetime

from fastapi import APIRouter, Depends

from ..orchestrator import MediaOrchestrator
from .deps import get_orchestrator

router = APIRouter(tags=["Monitoring"])


@router.get("/health")
async def health_check(orc: MediaOrchestrator = Depends(get_orchestrator)):
    try:
        await orc.redis.ping()
        redis_status = "connected"
    except Exception:
        redis_status = "disconnected"

    return {
        "status": "healthy",
        "service": "media-orchestrator",
        "redis": redis_status,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/stats")
async def get_stats(orc: MediaOrchestrator = Depends(get_orchestrator)):
    return await orc.collect_stats()
