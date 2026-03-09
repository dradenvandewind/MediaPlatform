from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

class JobSubmitRequest(BaseModel):
    video_url: str
    resolutions: List[str] = ["1080p", "720p", "480p"]
    audio_tracks: List[str] = ["en"]
    subtitles: List[str] = []
    watermark_config: Optional[Dict[str, Any]] = None
    drm_config: Optional[Dict[str, Any]] = None

class AdvanceJobRequest(BaseModel):
    stage: str
    result: Dict[str, Any] = {}

class FailJobRequest(BaseModel):
    error: str = "Unknown error"
    stage: Optional[str] = None

@dataclass
class JobStatus:
    job_id: str
    status: str
    current_stage: str
    created_at: str
    updated_at: str
    metadata: Dict[str, Any]
    results: Dict[str, Any]
    error: Optional[str] = None
