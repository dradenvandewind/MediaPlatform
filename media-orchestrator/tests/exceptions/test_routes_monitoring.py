"""
Tests des routes /health et /stats
"""

import pytest
from unittest.mock import AsyncMock, patch

from app.orchestrator import OrchestratorError
from tests.conftest import make_job_status


class TestHealth:
    async def test_healthy(self, client):
        r = await client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["service"]  == "media-orchestrator"
        assert data["redis"]    == "connected"
        assert data["postgres"] == "connected"
        assert data["status"]   == "healthy"
        assert "timestamp" in data

    async def test_redis_down(self, client, orchestrator):
        orchestrator.redis.ping = AsyncMock(side_effect=Exception("Redis down"))
        r = await client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["redis"]  == "disconnected"
        assert data["status"] == "degraded"

    async def test_postgres_down(self, client):
        from sqlalchemy import text
        import app.database as db_module

        original = db_module.get_session

        def bad_session():
            raise RuntimeError("PG down")

        db_module.get_session = bad_session
        try:
            r = await client.get("/health")
            assert r.status_code == 200
            assert r.json()["postgres"] == "disconnected"
            assert r.json()["status"]   == "degraded"
        finally:
            db_module.get_session = original

    async def test_both_down(self, client, orchestrator):
        import app.database as db_module
        orchestrator.redis.ping = AsyncMock(side_effect=Exception("down"))
        original = db_module.get_session

        def bad_session():
            raise RuntimeError("PG down")

        db_module.get_session = bad_session
        try:
            r = await client.get("/health")
            assert r.json()["status"] == "degraded"
        finally:
            db_module.get_session = original


class TestStats:
    async def test_structure(self, client):
        r = await client.get("/stats")
        assert r.status_code == 200
        data = r.json()
        assert "streams"        in data
        assert "jobs_by_status" in data

    async def test_counts_reflect_db(self, client, orchestrator):
        await orchestrator.save_job(make_job_status("job-stat-r-01", status="pending"))
        r = await client.get("/stats")
        assert r.json()["jobs_by_status"]["pending"] >= 1

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.collect_stats = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.get("/stats")
        assert r.status_code == 503
