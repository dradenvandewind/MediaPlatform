"""Tests des settings pydantic"""


import pytest
from app.settings import Settings
import os
_OTT_VARS = ["OTT_REDIS_URL", "OTT_DATABASE_URL", "OTT_HOST", "OTT_PORT", "OTT_LOG_LEVEL"]


class TestSettings:
    def test_defaults(self,monkeypatch):
        for k in _OTT_VARS:
            monkeypatch.delenv(k, raising=False)
        s = Settings(_env_file=None, _env_ignore_empty=True)
        assert s.redis_url    == "redis://localhost:6379"
        assert s.host         == "0.0.0.0"
        assert s.port         == 8000
        assert s.log_level    == "info"
        assert "postgresql"   in s.database_url

    def test_override_via_env(self, monkeypatch):
        monkeypatch.setenv("OTT_PORT", "9000")
        monkeypatch.setenv("OTT_LOG_LEVEL", "debug")
        for k in _OTT_VARS:
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("OTT_PORT", "9000")
        monkeypatch.setenv("OTT_LOG_LEVEL", "debug")
        s = Settings(_env_file=None, _env_ignore_empty=True)
        assert s.port      == 9000
        assert s.log_level == "debug"

    def test_redis_url_override(self, monkeypatch):
        monkeypatch.setenv("OTT_REDIS_URL", "redis://myhost:6380")
        for k in _OTT_VARS:
            monkeypatch.delenv(k, raising=False)
        monkeypatch.setenv("OTT_REDIS_URL", "redis://myhost:6380")
        s = Settings(_env_file=None, _env_ignore_empty=True)
        assert s.redis_url == "redis://myhost:6380"
