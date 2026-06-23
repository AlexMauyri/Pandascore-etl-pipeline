from functools import lru_cache
 
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
 
 
class PandaScoreSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PANDASCORE_", extra="ignore")
 
    token: str
    base_url: str = "https://api.pandascore.co"
    timeout_seconds: int = 10
 
 
class MinIOSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINIO_", extra="ignore")
 
    api_url: str = Field(..., alias="MINIO_API_URL")
    root_user: str = Field(..., alias="MINIO_ROOT_USER")
    root_password: str = Field(..., alias="MINIO_ROOT_PASSWORD")
    bucket: str = Field(..., alias="MINIO_BUCKET")
    secure: bool = Field(default=False, alias="MINIO_SECURE")
 
 
class ClickHouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_", extra="ignore")
 
    host: str = Field(..., alias="CLICKHOUSE_HOST")
    port: int = Field(default=8123, alias="CLICKHOUSE_PORT")
    user: str = Field(..., alias="CLICKHOUSE_USER")
    password: str = Field(..., alias="CLICKHOUSE_PASSWORD")
    db: str = Field(default="etl", alias="CLICKHOUSE_DB")
    insert_chunk_size: int = Field(default=50_000, alias="CLICKHOUSE_INSERT_CHUNK_SIZE")
 
 
@lru_cache
def get_pandascore_settings() -> PandaScoreSettings:
    return PandaScoreSettings() 
 
 
@lru_cache
def get_minio_settings() -> MinIOSettings:
    return MinIOSettings()
 
 
@lru_cache
def get_clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings() 
 
 
def clear_settings_cache() -> None:
    get_pandascore_settings.cache_clear()
    get_minio_settings.cache_clear()
    get_clickhouse_settings.cache_clear()
