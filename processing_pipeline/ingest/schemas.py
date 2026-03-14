"""Schémas Pydantic propres au nœud d'ingestion."""
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, model_validator


class IngestRequest(BaseModel):
    source_url:  HttpUrl | None = None
    source_path: str | None     = None
    video_url:   str | None     = None
    title:       str            = Field(default="untitled", min_length=1, max_length=255)  # ← default    
    profiles:    list[str]      = Field(default=["1080p", "720p", "480p"])
    metadata:    dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_source(self) -> "IngestRequest":

        if self.video_url and not self.source_url:
            self.source_url = self.video_url    
        if self.source_url is None and self.source_path is None:
            raise ValueError("Provide either source_url or source_path")
        if self.source_url and self.source_path:
            raise ValueError("Provide only one of source_url or source_path")
        return self


class IngestResponse(BaseModel):
    status:  str
    job_id:  str
    s3_uri:  str | None = None
    detail:  str | None = None
