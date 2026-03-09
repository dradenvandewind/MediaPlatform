"""
Configuration centralisée via variables d'environnement
"""
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    redis_url:    str = "redis://localhost:6379"
    database_url: str = "postgresql+asyncpg://ott:ott@localhost:5432/ott"
    host:         str = "0.0.0.0"
    port:         int = 8000
    log_level:    str = "info"

    model_config = SettingsConfigDict(
        env_prefix="OTT_",
        env_file=".env",
        env_file_encoding="utf-8",
    )