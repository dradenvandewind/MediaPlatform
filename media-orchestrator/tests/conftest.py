"""
Fixtures partagées entre tous les tests
- PostgreSQL : SQLite en mémoire (pas besoin de PG réel)
- Redis      : fakeredis (pas besoin de Redis réel)
- App        : client httpx avec lifespan mocké
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base, JobRecord, JobStatusEnum, get_session
from app.models import JobStatus
from app.orchestrator import MediaOrchestrator


# ── SQLite in-memory engine ───────────────────────────────────────────────────

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="session")
async def engine():
    from sqlalchemy.ext.asyncio import create_async_engine
    eng = create_async_engine(TEST_DB_URL, echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine) -> AsyncGenerator[AsyncSession, None]:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s
        await s.rollback()
        # Nettoyer la table entre chaque test
        from sqlalchemy import delete
        await s.execute(delete(JobRecord))
        await s.commit()


# ── Fake Redis ────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


# ── Orchestrateur avec mocks ──────────────────────────────────────────────────

@pytest_asyncio.fixture
async def orchestrator(engine, fake_redis):
    """Orchestrateur branché sur SQLite + fakeredis"""
    import app.database as db_module

    # Remplacer l'engine global par le SQLite de test
    factory = async_sessionmaker(engine, expire_on_commit=False)
    db_module._engine          = engine
    db_module._session_factory = factory

    orc = MediaOrchestrator(
        redis_url    = "redis://localhost:6379",
        database_url = TEST_DB_URL,
    )
    orc.redis = fake_redis

    # Initialiser les consumer groups sans appel réseau réel
    await orc.initialize_consumer_groups()

    yield orc


# ── Client HTTP ───────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def client(orchestrator) -> AsyncGenerator[AsyncClient, None]:
    """Client httpx avec l'app FastAPI, lifespan bypassé"""
    from app.main import app

    # Injecter l'orchestrateur directement dans app.state
    app.state.orchestrator = orchestrator

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_job_status(
    job_id:        str  = "job-test001",
    status:        str  = "pending",
    current_stage: str  = "ingest",
    error:         str  = None,
) -> JobStatus:
    now = datetime.now(timezone.utc).isoformat()
    return JobStatus(
        job_id        = job_id,
        status        = status,
        current_stage = current_stage,
        created_at    = now,
        updated_at    = now,
        metadata      = {"video_url": "s3://bucket/video.mp4", "resolutions": ["1080p"]},
        results       = {},
        error         = error,
    )
