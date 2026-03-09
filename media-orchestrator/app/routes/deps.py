"""
Shared FastAPI dependencies between routers
"""

from fastapi import Request

from ..orchestrator import MediaOrchestrator


def get_orchestrator(request: Request) -> MediaOrchestrator:
    """Injects the orchestrator stored in app.state"""
    return request.app.state.orchestrator
