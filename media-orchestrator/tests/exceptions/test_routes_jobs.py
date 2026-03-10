"""
Tests des routes /jobs/* via client HTTP
"""

import pytest
from unittest.mock import AsyncMock, patch

from app.orchestrator import OrchestratorError
from tests.conftest import make_job_status


SUBMIT_PAYLOAD = {
    "video_url":   "s3://bucket/video.mp4",
    "resolutions": ["1080p", "720p"],
    "audio_tracks": ["en", "fr"],
}


class TestSubmitJob:
    async def test_success(self, client):
        r = await client.post("/jobs/submit", json=SUBMIT_PAYLOAD)
        assert r.status_code == 201
        data = r.json()
        assert data["success"] is True
        assert data["job_id"].startswith("job-")

    async def test_missing_video_url(self, client):
        r = await client.post("/jobs/submit", json={"resolutions": ["720p"]})
        assert r.status_code == 422

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.save_job_status = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.post("/jobs/submit", json=SUBMIT_PAYLOAD)
        assert r.status_code == 503

    async def test_unexpected_error_returns_500(self, client, orchestrator):
        orchestrator.save_job_status = AsyncMock(side_effect=RuntimeError("boom"))
        r = await client.post("/jobs/submit", json=SUBMIT_PAYLOAD)
        assert r.status_code == 500


class TestListJobs:
    async def test_empty(self, client):
        r = await client.get("/jobs/list")
        assert r.status_code == 200
        assert r.json()["total"] == 0

    async def test_with_jobs(self, client, orchestrator):
        await orchestrator.save_job_status(make_job_status("job-list-r-01"))
        r = await client.get("/jobs/list")
        assert r.json()["total"] >= 1

    async def test_filter_by_status(self, client, orchestrator):
        await orchestrator.save_job_status(make_job_status("job-list-f-01", status="failed"))
        r = await client.get("/jobs/list?status=failed")
        assert r.status_code == 200
        for job in r.json()["jobs"]:
            assert job["status"] == "failed"

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.list_jobs = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.get("/jobs/list")
        assert r.status_code == 503


class TestGetJobStatus:
    async def test_found(self, client, orchestrator):
        await orchestrator.save_job_status(make_job_status("job-get-01"))
        r = await client.get("/jobs/job-get-01/status")
        assert r.status_code == 200
        assert r.json()["job_id"] == "job-get-01"

    async def test_not_found(self, client):
        r = await client.get("/jobs/ghost/status")
        assert r.status_code == 404

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.load_job_status = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.get("/jobs/any/status")
        assert r.status_code == 503


class TestAdvanceJob:
    async def test_advance_to_next_stage(self, client, orchestrator):
        await orchestrator.save_job_status(make_job_status("job-adv-01"))
        r = await client.post("/jobs/job-adv-01/next", json={
            "stage":  "ingest",
            "result": {"output": "s3://out.mp4"},
        })
        assert r.status_code == 200
        data = r.json()
        assert data["current_stage"] == "metadata-extraction"
        assert data["status"]        == "processing"

    async def test_advance_last_stage_deletes_job(self, client, orchestrator):
        js = make_job_status("job-adv-last", current_stage="packaging")
        await orchestrator.save_job_status(js)
        r = await client.post("/jobs/job-adv-last/next", json={
            "stage":  "packaging",
            "result": {},
        })
        assert r.status_code == 200
        assert r.json()["status"] == "completed"
        # Job supprimé
        assert await orchestrator.load_job_status("job-adv-last") is None

    async def test_not_found(self, client):
        r = await client.post("/jobs/ghost/next", json={"stage": "ingest", "result": {}})
        assert r.status_code == 404

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.load_job_status = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.post("/jobs/any/next", json={"stage": "ingest", "result": {}})
        assert r.status_code == 503

    async def test_unexpected_error_returns_500(self, client, orchestrator):
        orchestrator.load_job_status = AsyncMock(side_effect=RuntimeError("boom"))
        r = await client.post("/jobs/any/next", json={"stage": "ingest", "result": {}})
        assert r.status_code == 500


class TestMarkJobFailed:
    async def test_mark_failed(self, client, orchestrator):
        await orchestrator.save_job_status(make_job_status("job-fail-01"))
        r = await client.post("/jobs/job-fail-01/failed", json={
            "stage": "transcoding",
            "error": "OOM",
        })
        assert r.status_code == 200
        assert r.json()["status"] == "failed"

        loaded = await orchestrator.load_job_status("job-fail-01")
        assert loaded.status == "failed"
        assert "OOM" in loaded.error

    async def test_not_found(self, client):
        r = await client.post("/jobs/ghost/failed", json={"error": "x", "stage": "y"})
        assert r.status_code == 404

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.load_job_status = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.post("/jobs/any/failed", json={"error": "x"})
        assert r.status_code == 503

    async def test_unexpected_error_returns_500(self, client, orchestrator):
        orchestrator.load_job_status = AsyncMock(side_effect=RuntimeError("boom"))
        r = await client.post("/jobs/any/failed", json={"error": "x"})
        assert r.status_code == 500


class TestDeleteJob:
    async def test_delete(self, client, orchestrator):
        await orchestrator.save_job_status(make_job_status("job-del-r-01"))
        r = await client.delete("/jobs/job-del-r-01")
        assert r.status_code == 200
        assert r.json()["success"] is True

    async def test_not_found(self, client):
        r = await client.delete("/jobs/ghost")
        assert r.status_code == 404

    async def test_orchestrator_error_returns_503(self, client, orchestrator):
        orchestrator.load_job_status = AsyncMock(side_effect=OrchestratorError("DB down"))
        r = await client.delete("/jobs/any")
        assert r.status_code == 503
