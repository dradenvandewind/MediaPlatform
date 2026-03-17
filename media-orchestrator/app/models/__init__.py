from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, validator

VALID_RESOLUTIONS = {"2160p", "1080p", "720p", "480p", "360p"}

class JobSubmitRequest(BaseModel):
    video_url: str
    title:       str = "untitled"
    profiles: List[str] = []
    audio_tracks: List[str] = ["en"]
    subtitles: List[str] = []
    watermark_config: Optional[Dict[str, Any]] = None
    drm_config: Optional[Dict[str, Any]] = None
    
    @validator("profiles")
    def check_profiles(cls, v):
        invalid = set(v) - VALID_RESOLUTIONS
        if invalid:
            raise ValueError(f" Invalid profiles : {invalid}")
        if not v:
            raise ValueError("At least one profile is required")
        return v

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
