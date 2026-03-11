"""
Connexion PostgreSQL via SQLAlchemy async
"""

from sqlalchemy import (
    Column, DateTime, Enum, String, Text,
    text, func
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

import enum


class JobStatusEnum(str, enum.Enum):
    pending    = "pending"
    processing = "processing"
    completed  = "completed"
    failed     = "failed"


class Base(DeclarativeBase):
    pass


class JobRecord(Base):
    __tablename__ = "jobs"

    job_id        = Column(String(64),  primary_key=True)
    status        = Column(Enum(JobStatusEnum), nullable=False, default=JobStatusEnum.pending)
    current_stage = Column(String(64),  nullable=False)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    metadata_json = Column(Text, nullable=False, default="{}")
    results_json  = Column(Text, nullable=False, default="{}")
    error         = Column(Text, nullable=True)


# ── Engine / session factory ──────────────────────────────────────────────────

_engine = None
_session_factory = None


def init_engine(database_url: str):
    global _engine, _session_factory
    kwargs = {}
    if not database_url.startswith("sqlite"):
        kwargs = {"pool_size": 10, "max_overflow": 20}
    _engine = create_async_engine(
        database_url,
        echo=False,
        pool_pre_ping=True,
        **kwargs,
    )
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)

async def create_tables():
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def dispose_engine():
    if _engine:
        await _engine.dispose()


def get_session() -> AsyncSession:
    return _session_factory()