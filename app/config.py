from functools import lru_cache

from pydantic import PositiveFloat, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://generation:generation@localhost:55432/generation"
    webhook_secret: str = "local-development-secret"
    alpha_url: str = "http://localhost:8001"
    beta_url: str = "http://localhost:8002"
    callback_base_url: str = "http://localhost:8000"
    provider_timeout: PositiveFloat = 60
    http_timeout: PositiveFloat = 5
    dispatch_lease: PositiveFloat = 10
    poll_interval: PositiveFloat = 1
    workers_enabled: bool = True
    provider_name: str = "alpha"

    @model_validator(mode="after")
    def validate_timeouts(self) -> "Settings":
        if not self.http_timeout < self.dispatch_lease < self.provider_timeout:
            raise ValueError("Expected HTTP_TIMEOUT < DISPATCH_LEASE < PROVIDER_TIMEOUT")
        if self.provider_name not in {"alpha", "beta"}:
            raise ValueError("PROVIDER_NAME must be alpha or beta")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
