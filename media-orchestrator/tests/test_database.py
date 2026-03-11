"""Tests du module database : JobRecord, init_engine, create_tables"""

import json
import pytest
import pytest_asyncio
from sqlalchemy import select

import app.database as db_module
from app.database import (
    Base, JobRecord, JobStatusEnum,
    create_tables, dispose_engine, get_session, init_engine,
)


class TestJobStatusEnum:
    def test_values(self):
        assert JobStatusEnum.pending    == "pending"
        assert JobStatusEnum.processing == "processing"
        assert JobStatusEnum.completed  == "completed"
        assert JobStatusEnum.failed     == "failed"


class TestInitEngine:
    def test_sets_globals(self):
        init_engine("sqlite+aiosqlite:///:memory:")
        assert db_module._engine          is not None
        assert db_module._session_factory is not None

    def test_get_session_returns_session(self):
        init_engine("sqlite+aiosqlite:///:memory:")
        s = get_session()
        assert s is not None


class TestCreateAndDispose:
    async def test_create_tables(self, engine):
        # create_tables must be able to run multiple times (idempotent)
        db_module._engine = engine
        await create_tables()
    """
    async def test_dispose_engine(self, engine):
        db_module._engine = engine
        await dispose_engine()
        # Réinitialiser pour les autres tests
        db_module._engine = engine
    """
    async def test_dispose_engine(self):
        # Utiliser un engine jetable, pas le partagé session-scoped
        from sqlalchemy.ext.asyncio import create_async_engine
        local_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        db_module._engine = local_engine
        await dispose_engine()
        # Remettre à None proprement
        db_module._engine = None


class TestJobRecord:
    async def test_insert_and_select(self, session, engine):
        db_module._engine          = engine
        db_module._session_factory = __import__(
            "sqlalchemy.ext.asyncio", fromlist=["async_sessionmaker"]
        ).async_sessionmaker(engine, expire_on_commit=False)

        record = JobRecord(
            job_id        = "job-db-001",
            status        = JobStatusEnum.pending,
            current_stage = "ingest",
            metadata_json = json.dumps({"video_url": "s3://v.mp4"}),
            results_json  = "{}",
        )
        session.add(record)
        await session.commit()

        result = await session.execute(
            select(JobRecord).where(JobRecord.job_id == "job-db-001")
        )
        found = result.scalar_one_or_none()
        assert found is not None
        assert found.status == JobStatusEnum.pending

    async def test_nullable_error(self, session, engine):
        record = JobRecord(
            job_id        = "job-db-002",
            status        = JobStatusEnum.failed,
            current_stage = "transcoding",
            metadata_json = "{}",
            results_json  = "{}",
            error         = "OOM",
        )
        session.add(record)
        await session.commit()

        result = await session.execute(
            select(JobRecord).where(JobRecord.job_id == "job-db-002")
        )
        found = result.scalar_one()
        assert found.error == "OOM"
