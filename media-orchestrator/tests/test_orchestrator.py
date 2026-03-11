"""
Tests de MediaOrchestrator :
- save / load / delete / list
- send_to_stage
- get_next_stage
- collect_stats
- exceptions OrchestratorError / JobNotFoundError
- _monitor_jobs (cancel)
- _record_to_status / _record_to_dict
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.database import JobRecord, JobStatusEnum
from app.models import JobStatus
from app.orchestrator import JobNotFoundError, MediaOrchestrator, OrchestratorError
from tests.conftest import make_job_status


# ── save_job ──────────────────────────────────────────────────────────────────

class TestSaveJob:
    async def test_insert(self, orchestrator):
        js = make_job_status("job-save-01")
        await orchestrator.save_job(js)
        loaded = await orchestrator.load_job("job-save-01")
        assert loaded.job_id == "job-save-01"
        assert loaded.status == "pending"

    async def test_update(self, orchestrator):
        js = make_job_status("job-save-02")
        await orchestrator.save_job(js)
        js.status        = "processing"
        js.current_stage = "transcoding"
        await orchestrator.save_job(js)
        loaded = await orchestrator.load_job("job-save-02")
        assert loaded.status        == "processing"
        assert loaded.current_stage == "transcoding"

    async def test_db_error_raises_orchestrator_error(self, orchestrator):
        import app.orchestrator as orc_module   # ← orc_module, pas db_module
        original = orc_module.get_session
        def bad_session():
            raise RuntimeError("DB down")
        orc_module.get_session = bad_session
        try:
            with pytest.raises(OrchestratorError):
                await orchestrator.save_job(make_job_status("job-err"))
        finally:
            orc_module.get_session = original


# ── load_job ──────────────────────────────────────────────────────────────────

class TestLoadJob:
    async def test_not_found_returns_none(self, orchestrator):
        result = await orchestrator.load_job("nonexistent")
        assert result is None

    async def test_db_error_raises(self, orchestrator):
        import app.orchestrator as orc_module
        original = orc_module.get_session
        orc_module.get_session = bad_session
        try:
            with pytest.raises(OrchestratorError):
                await orchestrator.load_job("any")
        finally:
            orc_module.get_session = original
                   
            


# ── delete_job ────────────────────────────────────────────────────────────────

class TestDeleteJob:
    async def test_delete_existing(self, orchestrator):
        js = make_job_status("job-del-01")
        await orchestrator.save_job(js)
        await orchestrator.delete_job("job-del-01")
        assert await orchestrator.load_job("job-del-01") is None
        

    async def test_delete_nonexistent_no_error(self, orchestrator):
        # Supprimer un job inexistant ne doit pas lever d'exception
        await orchestrator.delete_job("ghost-job")

    async def test_db_error_raises(self, orchestrator):
        import app.orchestrator as orc_module
        original = orc_module.get_session
        orc_module.get_session = bad_session
        try:
            with pytest.raises(OrchestratorError):
                await orchestrator.delete_job("any")
        finally:
            orc_module.get_session = original


# ── list_jobs ─────────────────────────────────────────────────────────────────

class TestListJobs:
    async def test_empty(self, orchestrator):
        jobs = await orchestrator.list_jobs()
        assert jobs == []

    async def test_list_all(self, orchestrator):
        await orchestrator.save_job(make_job_status("job-list-01", status="pending"))
        await orchestrator.save_job(make_job_status("job-list-02", status="failed"))
        jobs = await orchestrator.list_jobs()
        assert len(jobs) == 2

    async def test_filter_by_status(self, orchestrator):
        await orchestrator.save_job(make_job_status("job-f-01", status="pending"))
        await orchestrator.save_job(make_job_status("job-f-02", status="failed"))
        pending = await orchestrator.list_jobs(status_filter="pending")
        assert len(pending) == 1
        assert pending[0]["status"] == "pending"

    async def test_db_error_raises(self, orchestrator):
        import app.orchestrator as orc_module
        original = orc_module.get_session
        orc_module.get_session = bad_session
        try:
            with pytest.raises(OrchestratorError):
                await orchestrator.list_jobs()
        finally:
            orc_module.get_session = original
        


# ── send_to_stage ─────────────────────────────────────────────────────────────

class TestSendToStage:
    async def test_adds_message_to_stream(self, orchestrator):
        await orchestrator.send_to_stage("ingest", {
            "job_id": "job-stream-01",
            "input":  {"video_url": "s3://v.mp4"},
        })
        length = await orchestrator.redis.xlen("jobs:ingest")
        assert length >= 1

    async def test_redis_error_raises(self, orchestrator):
        orchestrator.redis.xadd = AsyncMock(side_effect=Exception("Redis down"))
        with pytest.raises(OrchestratorError):
            await orchestrator.send_to_stage("ingest", {"job_id": "x"})


# ── get_next_stage ────────────────────────────────────────────────────────────

class TestGetNextStage:
    def test_first_stage(self, orchestrator):
        assert orchestrator.get_next_stage("ingest") == "metadata-extraction"

    def test_middle_stage(self, orchestrator):
        assert orchestrator.get_next_stage("transcoding") == "audio-encoding"

    def test_last_stage_returns_completed(self, orchestrator):
        assert orchestrator.get_next_stage("packaging") == "completed"

    def test_completed_returns_none(self, orchestrator):
        assert orchestrator.get_next_stage("completed") is None

    def test_unknown_stage_returns_none(self, orchestrator):
        assert orchestrator.get_next_stage("unknown-stage") is None


# ── collect_stats ─────────────────────────────────────────────────────────────

class TestCollectStats:
    async def test_structure(self, orchestrator):
        stats = await orchestrator.collect_stats()
        assert "streams"        in stats
        assert "jobs_by_status" in stats
        assert "pending"        in stats["jobs_by_status"]
        assert "failed"         in stats["jobs_by_status"]

    async def test_counts_jobs(self, orchestrator):
        await orchestrator.save_job(make_job_status("job-stats-01", status="pending"))
        await orchestrator.save_job(make_job_status("job-stats-02", status="failed"))
        stats = await orchestrator.collect_stats()
        assert stats["jobs_by_status"]["pending"] >= 1
        assert stats["jobs_by_status"]["failed"]  >= 1

    async def test_stream_length(self, orchestrator):
        await orchestrator.send_to_stage("ingest", {"job_id": "j"})
        stats = await orchestrator.collect_stats()
        assert stats["streams"]["ingest"]["queue_length"] >= 1


# ── _monitor_jobs ─────────────────────────────────────────────────────────────

class TestMonitorJobs:
    async def test_cancels_cleanly(self, orchestrator):
        task = asyncio.create_task(orchestrator._monitor_jobs())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task          # ← c'est ici que CancelledError est levé
        except asyncio.CancelledError:
            pass                # attendu, le test passe

    async def test_handles_exception_without_crash(self, orchestrator, monkeypatch):
        call_count = 0

        async def fast_sleep(n):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError
            raise RuntimeError("boom")

        monkeypatch.setattr("app.orchestrator.asyncio", asyncio)
        monkeypatch.setattr(asyncio, "sleep", fast_sleep)
        with pytest.raises(asyncio.CancelledError):
            await orchestrator._monitor_jobs()


# ── exceptions ────────────────────────────────────────────────────────────────

class TestExceptions:
    def test_job_not_found_message(self):
        exc = JobNotFoundError("job-xyz")
        assert "job-xyz" in str(exc)
        assert exc.job_id == "job-xyz"

    def test_orchestrator_error_is_base(self):
        assert issubclass(JobNotFoundError, OrchestratorError)


# ── conversions ───────────────────────────────────────────────────────────────

class TestConversions:
    def _make_record(self, job_id="job-conv-01", status=JobStatusEnum.pending):
        r               = MagicMock(spec=JobRecord)
        r.job_id        = job_id
        r.status        = status
        r.current_stage = "ingest"
        r.created_at    = datetime(2024, 1, 1, tzinfo=timezone.utc)
        r.updated_at    = datetime(2024, 1, 1, tzinfo=timezone.utc)
        r.metadata_json = json.dumps({"video_url": "s3://v.mp4"})
        r.results_json  = "{}"
        r.error         = None
        return r

    def test_record_to_status(self):
        r  = self._make_record()
        js = MediaOrchestrator._record_to_status(r)
        assert isinstance(js, JobStatus)
        assert js.job_id == "job-conv-01"
        assert js.metadata == {"video_url": "s3://v.mp4"}

    def test_record_to_dict(self):
        r = self._make_record(status=JobStatusEnum.failed)
        d = MediaOrchestrator._record_to_dict(r)
        assert d["status"] == "failed"
        assert isinstance(d["metadata"], dict)

    def test_record_to_status_none_dates(self):
        r            = self._make_record()
        r.created_at = None
        r.updated_at = None
        js = MediaOrchestrator._record_to_status(r)
        assert js.created_at == ""
        assert js.updated_at == ""