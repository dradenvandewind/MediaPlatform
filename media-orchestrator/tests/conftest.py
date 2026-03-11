import asyncio
import json
from datetime import datetime, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis as fakeredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete as sa_delete                          # ← AJOUT
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base, JobRecord, JobStatusEnum, get_session
from app.models import JobStatus
from app.orchestrator import MediaOrchestrator


TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="session")
async def engine():
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
        await s.execute(sa_delete(JobRecord))
        await s.commit()


@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


@pytest_asyncio.fixture
async def orchestrator(engine, fake_redis):
    import app.database as db_module

    factory = async_sessionmaker(engine, expire_on_commit=False)
    db_module._engine          = engine
    db_module._session_factory = factory

    # Nettoyer AVANT le test
    async with factory() as s:
        await s.execute(sa_delete(JobRecord))
        await s.commit()

    orc = MediaOrchestrator(
        redis_url    = "redis://localhost:6379",
        database_url = TEST_DB_URL,
    )
    orc.redis = fake_redis
    await orc._initialize_consumer_groups()

    yield orc

    # Nettoyer APRÈS le test
    async with factory() as s:
        await s.execute(sa_delete(JobRecord))
        await s.commit()


@pytest_asyncio.fixture
async def client(orchestrator) -> AsyncGenerator[AsyncClient, None]:
    from app.main import app
    app.state.orchestrator = orchestrator
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c


def make_job_status(
    job_id:        str = "job-test001",
    status:        str = "pending",
    current_stage: str = "ingest",
    error:         str = None,
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