"""Configuration. All secrets come from environment variables, never from code."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    app_env: str = "dev"
    log_level: str = "INFO"
    # Backend application role (app_rw). Required: no default on purpose.
    database_url: str
    # Read-only role used later by the SQL executor; optional in Phase 1.
    analytics_ro_url: str | None = None
    etl_url: str | None = None
    # Base directory that CSV/Parquet sources must live under (path traversal guard).
    data_dir: str = "/data"
    # Hosts that SQL source configs may point at (comma separated). Blocks arbitrary network targets.
    allowed_source_hosts: str = "postgres,localhost,127.0.0.1,mysql"
    # LLM provider: "ollama" or "none" (deterministic fallbacks only). Tests inject a mock client.
    llm_provider: str = "none"
    ollama_host: str = "http://ollama:11434"
    ollama_model: str = "qwen2.5-coder:3b"
    ollama_timeout_s: float = 60.0
    # Authentication. JWT_SECRET must be set (>= 32 chars); there is deliberately no default.
    jwt_secret: str | None = None
    jwt_ttl_minutes: int = 30
    allow_self_approval: bool = False        # separation of duties: requester cannot approve their own request
    # Integrations (documented mock adapters by default; any Slack-compatible webhook / REST ticket API works)
    alert_webhook_url: str | None = None
    ticket_api_url: str | None = None
    ticket_api_token: str | None = None
    integration_timeout_s: float = 3.0
    integration_retries: int = 2
    alert_max_attempts: int = 5
    # Observability
    pipeline_sla_hours: float = 26.0          # a pipeline with no success newer than this breaches its freshness SLA
    llm_prompt_cost_per_1k: float = 0.0       # USD; local Ollama is free, set these for a paid provider
    llm_completion_cost_per_1k: float = 0.0


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
