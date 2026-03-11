"""Tests des settings pydantic"""

import os
import pytest
from app.settings import Settings


class TestSettings:
    def test_defaults(self):
        s = Settings(_env_file=None)
        assert s.redis_url    == "redis://localhost:6379"
        assert s.host         == "0.0.0.0"
        assert s.port         == 8000
        assert s.log_level    == "info"
        assert "postgresql"   in s.database_url

    def test_override_via_env(self, monkeypatch):
        monkeypatch.setenv("OTT_PORT", "9000")
        monkeypatch.setenv("OTT_LOG_LEVEL", "debug")
        s = Settings(_env_file=None)
        assert s.port      == 9000
        assert s.log_level == "debug"

    def test_redis_url_override(self, monkeypatch):
        monkeypatch.setenv("OTT_REDIS_URL", "redis://myhost:6380")
        s = Settings(_env_file=None)
        assert s.redis_url == "redis://myhost:6380"
