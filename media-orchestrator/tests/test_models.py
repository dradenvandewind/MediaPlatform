"""Tests des modèles Pydantic et dataclass JobStatus"""

import pytest
from app.models import AdvanceJobRequest, FailJobRequest, JobStatus, JobSubmitRequest


class TestJobSubmitRequest:
    def test_minimal(self):
        req = JobSubmitRequest(video_url="s3://bucket/v.mp4")
        assert req.video_url == "s3://bucket/v.mp4"
        assert req.profiles == ["1080p", "720p", "480p"]
        assert req.audio_tracks == ["en"]
        assert req.subtitles == []
        assert req.watermark_config is None
        assert req.drm_config is None

    def test_full(self):
        req = JobSubmitRequest(
            video_url        = "s3://b/v.mp4",
            profiles      = ["720p"],
            audio_tracks     = ["fr", "en"],
            subtitles        = ["fr"],
            watermark_config = {"text": "DEMO"},
            drm_config       = {"provider": "widevine"},
        )
        assert req.profiles == ["720p"]
        assert req.watermark_config == {"text": "DEMO"}

    def test_model_dump(self):
        req  = JobSubmitRequest(video_url="s3://b/v.mp4")
        data = req.model_dump()
        assert isinstance(data, dict)
        assert "video_url" in data


class TestAdvanceJobRequest:
    def test_defaults(self):
        req = AdvanceJobRequest(stage="ingest")
        assert req.stage == "ingest"
        assert req.result == {}

    def test_with_result(self):
        req = AdvanceJobRequest(stage="transcoding", result={"output": "s3://out.mp4"})
        assert req.result["output"] == "s3://out.mp4"


class TestFailJobRequest:
    def test_defaults(self):
        req = FailJobRequest()
        assert req.error == "Unknown error"
        assert req.stage is None

    def test_custom(self):
        req = FailJobRequest(error="OOM", stage="transcoding")
        assert req.error == "OOM"
        assert req.stage == "transcoding"


class TestJobStatus:
    def test_creation(self):
        js = JobStatus(
            job_id        = "job-abc",
            status        = "pending",
            current_stage = "ingest",
            created_at    = "2024-01-01T00:00:00",
            updated_at    = "2024-01-01T00:00:00",
            metadata      = {},
            results       = {},
        )
        assert js.job_id == "job-abc"
        assert js.error is None

    def test_with_error(self):
        js = JobStatus(
            job_id="j", status="failed", current_stage="muxing",
            created_at="", updated_at="", metadata={}, results={},
            error="disk full",
        )
        assert js.error == "disk full"
