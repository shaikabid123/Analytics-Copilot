"""Validation of data-source configuration (stored in app.data_sources.config).

Rules (all enforced here, in code, not by convention):
  * No secrets in config. Passwords are referenced by env-var NAME (`password_env`),
    and that name must start with SRC_ so a config can never point at DATABASE_URL etc.
  * SQL hosts must be in ALLOWED_SOURCE_HOSTS (no arbitrary network targets).
  * File paths are relative to DATA_DIR, with no traversal, and have a fixed extension.
  * Unknown keys are rejected (so a stray "password" key fails loudly).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config import get_settings

SOURCE_TYPES = ("postgres", "mysql", "csv", "parquet")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_HOST = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")
_PASSWORD_ENV = re.compile(r"^SRC_[A-Z0-9_]{1,59}$")


class SourceConfigError(ValueError):
    """Raised for any invalid or unsafe source configuration."""


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SqlSourceConfig(_Base):
    host: str = Field(max_length=253)
    port: int = Field(ge=1, le=65535)
    database: str
    username: str
    password_env: str
    schemas: list[str] = Field(min_length=1, max_length=20)
    connect_timeout_s: int = Field(default=5, ge=1, le=30)

    @field_validator("host")
    @classmethod
    def _host_ok(cls, v: str) -> str:
        if not _HOST.match(v):
            raise ValueError("host contains invalid characters")
        allowed = {h.strip().lower() for h in get_settings().allowed_source_hosts.split(",") if h.strip()}
        if v.lower() not in allowed:
            raise ValueError(f"host '{v}' is not in ALLOWED_SOURCE_HOSTS")
        return v

    @field_validator("database", "username")
    @classmethod
    def _ident_ok(cls, v: str) -> str:
        if not _IDENT.match(v):
            raise ValueError("must be a plain identifier (letters, digits, underscore)")
        return v

    @field_validator("schemas")
    @classmethod
    def _schemas_ok(cls, v: list[str]) -> list[str]:
        for name in v:
            if not _IDENT.match(name):
                raise ValueError(f"invalid schema name: {name!r}")
        return v

    @field_validator("password_env")
    @classmethod
    def _pw_env_ok(cls, v: str) -> str:
        if not _PASSWORD_ENV.match(v):
            raise ValueError("password_env must be an env-var NAME starting with SRC_ (e.g. SRC_PG_PASSWORD)")
        return v


class FileSourceConfig(_Base):
    path: str = Field(min_length=1, max_length=300)
    delimiter: Literal[",", ";", "|", "\t"] = ","  # CSV only
    max_file_mb: int = Field(default=500, ge=1, le=2048)

    @field_validator("path")
    @classmethod
    def _path_ok(cls, v: str) -> str:
        p = Path(v)
        if p.is_absolute() or v.startswith(("/", "\\")) or re.match(r"^[A-Za-z]:", v):
            raise ValueError("path must be relative to DATA_DIR")
        if ".." in p.parts or "\x00" in v:
            raise ValueError("path must not contain '..'")
        return v


def validate_source_config(source_type: str, config: dict) -> SqlSourceConfig | FileSourceConfig:
    """Return the validated model or raise SourceConfigError with a readable message."""
    from pydantic import ValidationError

    if source_type not in SOURCE_TYPES:
        raise SourceConfigError(f"unsupported source_type '{source_type}' (allowed: {', '.join(SOURCE_TYPES)})")
    model = SqlSourceConfig if source_type in ("postgres", "mysql") else FileSourceConfig
    try:
        cfg = model.model_validate(config)
    except ValidationError as exc:
        msgs = "; ".join(f"{'.'.join(str(x) for x in e['loc']) or 'config'}: {e['msg']}" for e in exc.errors())
        raise SourceConfigError(msgs) from None
    if isinstance(cfg, FileSourceConfig):
        suffix = Path(cfg.path).suffix.lower()
        wanted = {".csv"} if source_type == "csv" else {".parquet", ".pq"}
        if suffix not in wanted:
            raise SourceConfigError(f"path: a {source_type} source must end with {' or '.join(sorted(wanted))}")
    return cfg


def resolve_data_path(relative: str) -> Path:
    """Resolve a validated relative path under DATA_DIR; reject escapes (including via symlinks)."""
    base = Path(get_settings().data_dir).resolve()
    target = (base / relative).resolve()
    if not target.is_relative_to(base):
        raise SourceConfigError("path escapes DATA_DIR")
    return target
