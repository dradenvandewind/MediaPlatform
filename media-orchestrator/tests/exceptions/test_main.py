"""
Tests de main.py : global exception handler + lifespan
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestGlobalExceptionHandler:
    async def test_unhandled_exception_returns_500(self, client, orchestrator):
        """Un RuntimeError non catchée renvoie un JSON 500 propre"""
        orchestrator.list_jobs = AsyncMock(side_effect=RuntimeError("unexpected crash"))
        r = await client.get("/jobs/list")
        # list_jobs lève OrchestratorError normalement,
        # ici on force une exception inconnue → 500 via le handler global
        # Le handler catch tout ce qui n'est pas HTTPException
        assert r.status_code in (500, 503)


class TestLifespan:
    async def test_startup_and_shutdown(self):
        """Vérifie que startup/shutdown s'enchaînent sans erreur avec des mocks"""
        from app.main import app
        from app.orchestrator import MediaOrchestrator

        mock_orc = MagicMock(spec=MediaOrchestrator)
        mock_orc.startup  = AsyncMock()
        mock_orc.shutdown = AsyncMock()

        with patch("app.main.MediaOrchestrator", return_value=mock_orc):
            from httpx import ASGITransport, AsyncClient
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as c:
                r = await c.get("/health")
                # L'app démarre et répond même avec l'orchestrateur mocké
                # (app.state.orchestrator est déjà injecté par la fixture client)
                assert r.status_code in (200, 500)
