"""Reusable source-connector interface + Postgres / MySQL / CSV / Parquet implementations.

A connector only DISCOVERS and PREVIEWS. It never writes to a source. Loading data into
Bronze is done by the PySpark ETL (Phase 4). Credentials never appear in config: SQL
connectors read the password from the env var named by `password_env` at connect time.
"""
from __future__ import annotations

import csv
import os
import re
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, event, func, inspect, literal_column, pool, select, table
from sqlalchemy.engine import URL, Engine

from app.services.source_config import (
    FileSourceConfig, SourceConfigError, SqlSourceConfig, resolve_data_path, validate_source_config,
)

MAX_SAMPLE_ROWS = 20
INFER_ROWS = 5000
MAX_COLUMNS = 500


class ConnectorError(Exception):
    """Connection/discovery failure with a safe, user-presentable message (never contains secrets)."""


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool | None
    ordinal: int


@dataclass
class DatasetInfo:
    schema_name: str
    table_name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int | None = None


class SourceConnector(ABC):
    """Interface every source type implements."""

    @abstractmethod
    def test_connection(self) -> None:
        """Raise ConnectorError if the source cannot be reached/read."""

    @abstractmethod
    def list_datasets(self) -> list[DatasetInfo]:
        """Discover datasets with columns and exact row counts."""

    @abstractmethod
    def sample(self, schema_name: str, table_name: str, limit: int = 5) -> list[dict]:
        """Return up to `limit` (max 20) rows as JSON-safe dicts. Only for datasets this connector lists."""


def _clamp(limit: int) -> int:
    if not isinstance(limit, int) or limit < 1:
        raise ConnectorError("limit must be a positive integer")
    return min(limit, MAX_SAMPLE_ROWS)


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "<binary>"
    return str(v)


# --------------------------------------------------------------------------- SQL (Postgres / MySQL)
class SqlConnector(SourceConnector):
    DRIVER: str = ""

    def __init__(self, cfg: SqlSourceConfig):
        self.cfg = cfg

    def _password(self) -> str:
        value = os.environ.get(self.cfg.password_env)
        if not value:
            raise ConnectorError(f"environment variable {self.cfg.password_env} is not set")
        return value

    def _engine(self) -> Engine:
        url = URL.create(self.DRIVER, username=self.cfg.username, password=self._password(),
                         host=self.cfg.host, port=self.cfg.port, database=self.cfg.database)
        engine = create_engine(url, poolclass=pool.NullPool,
                               connect_args=self._connect_args())
        self._make_read_only(engine)
        return engine

    def _connect_args(self) -> dict:
        return {"connect_timeout": self.cfg.connect_timeout_s}

    def _make_read_only(self, engine: Engine) -> None:  # pragma: no cover - overridden
        pass

    @contextmanager
    def _connected(self) -> Iterator[Engine]:
        engine = self._engine()
        try:
            yield engine
        except ConnectorError:
            raise
        except Exception as exc:  # never leak driver messages (may echo host/user details)
            raise ConnectorError(f"{type(exc).__name__}: could not read from source "
                                 f"{self.cfg.host}:{self.cfg.port}/{self.cfg.database}") from None
        finally:
            engine.dispose()

    def test_connection(self) -> None:
        with self._connected() as engine, engine.connect() as c:
            c.exec_driver_sql("SELECT 1")

    def list_datasets(self) -> list[DatasetInfo]:
        out: list[DatasetInfo] = []
        with self._connected() as engine:
            insp = inspect(engine)
            for schema in self.cfg.schemas:
                names = sorted(set(insp.get_table_names(schema=schema)) | set(insp.get_view_names(schema=schema)))
                for name in names:
                    cols = [ColumnInfo(c["name"], self._type_name(c["type"]), bool(c.get("nullable", True)), i + 1)
                            for i, c in enumerate(insp.get_columns(name, schema=schema))]
                    with engine.connect() as conn:
                        count = conn.execute(select(func.count()).select_from(table(name, schema=schema))).scalar_one()
                    out.append(DatasetInfo(schema, name, cols, int(count)))
        return out

    @staticmethod
    def _type_name(t) -> str:
        try:
            return str(t)
        except Exception:  # exotic types without a compilable name
            return t.__class__.__name__

    def sample(self, schema_name: str, table_name: str, limit: int = 5) -> list[dict]:
        limit = _clamp(limit)
        if schema_name not in self.cfg.schemas:
            raise ConnectorError(f"schema '{schema_name}' is not configured for this source")
        with self._connected() as engine:
            if table_name not in inspect(engine).get_table_names(schema=schema_name) + \
                    inspect(engine).get_view_names(schema=schema_name):
                raise ConnectorError(f"unknown dataset {schema_name}.{table_name}")
            with engine.connect() as conn:
                rows = conn.execute(select(literal_column("*")).select_from(table(table_name, schema=schema_name)).limit(limit))
                return [{k: _jsonable(v) for k, v in r._mapping.items()} for r in rows]


class PostgresConnector(SqlConnector):
    DRIVER = "postgresql+psycopg"

    def _connect_args(self) -> dict:
        # Client-side read-only on top of the read-only DB role.
        return {"connect_timeout": self.cfg.connect_timeout_s, "options": "-c default_transaction_read_only=on"}


class MySQLConnector(SqlConnector):
    """Same interface over MySQL/MariaDB. NOTE: not integration-tested against a live server."""
    DRIVER = "mysql+pymysql"

    def _make_read_only(self, engine: Engine) -> None:
        @event.listens_for(engine, "connect")
        def _ro(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            cur.execute("SET SESSION TRANSACTION READ ONLY")
            cur.close()


# --------------------------------------------------------------------------- files
def _safe_table_name(path: Path) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", path.stem.lower()).strip("_") or "dataset"


def _check_file(cfg: FileSourceConfig) -> Path:
    path = resolve_data_path(cfg.path)
    if not path.is_file():
        raise ConnectorError(f"file not found under DATA_DIR: {cfg.path}")
    if path.stat().st_size > cfg.max_file_mb * 1024 * 1024:
        raise ConnectorError(f"file exceeds the {cfg.max_file_mb} MB limit")
    return path


_TS = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INT = re.compile(r"^[+-]?\d+$")
_FLOAT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")


def _infer_type(values: list[str]) -> str:
    non_empty = [v for v in values if v != ""]
    if not non_empty:
        return "text"
    if all(v.lower() in ("true", "false") for v in non_empty):
        return "boolean"
    if all(_INT.match(v) for v in non_empty):
        return "integer"
    if all(_FLOAT.match(v) for v in non_empty):
        return "numeric"
    if all(_DATE.match(v) for v in non_empty):
        return "date"
    if all(_TS.match(v) for v in non_empty):
        return "timestamp"
    return "text"


class CsvConnector(SourceConnector):
    def __init__(self, cfg: FileSourceConfig):
        self.cfg = cfg

    def _open(self):
        return open(_check_file(self.cfg), newline="", encoding="utf-8-sig")

    def _header(self, reader) -> list[str]:
        try:
            header = [h.strip() for h in next(reader)]
        except StopIteration:
            raise ConnectorError("CSV file is empty") from None
        if not header or any(h == "" for h in header):
            raise ConnectorError("CSV header has empty column names")
        if len(set(header)) != len(header):
            raise ConnectorError("CSV header has duplicate column names")
        if len(header) > MAX_COLUMNS:
            raise ConnectorError(f"CSV has more than {MAX_COLUMNS} columns")
        return header

    def test_connection(self) -> None:
        try:
            with self._open() as f:
                self._header(csv.reader(f, delimiter=self.cfg.delimiter))
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            raise ConnectorError(f"cannot read CSV: {type(exc).__name__}") from None

    def list_datasets(self) -> list[DatasetInfo]:
        try:
            with self._open() as f:
                reader = csv.reader(f, delimiter=self.cfg.delimiter)
                header = self._header(reader)
                samples: list[list[str]] = [[] for _ in header]
                count = 0
                for row in reader:
                    if not row:
                        continue
                    count += 1
                    if count <= INFER_ROWS:
                        for i in range(len(header)):
                            samples[i].append(row[i] if i < len(row) else "")
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            raise ConnectorError(f"cannot read CSV: {type(exc).__name__}") from None
        cols = [ColumnInfo(h, _infer_type(samples[i]), any(v == "" for v in samples[i]), i + 1)
                for i, h in enumerate(header)]
        return [DatasetInfo("file", _safe_table_name(Path(self.cfg.path)), cols, count)]

    def sample(self, schema_name: str, table_name: str, limit: int = 5) -> list[dict]:
        limit = _clamp(limit)
        ds = self.list_datasets()[0]
        if (schema_name, table_name) != (ds.schema_name, ds.table_name):
            raise ConnectorError(f"unknown dataset {schema_name}.{table_name}")
        out: list[dict] = []
        with self._open() as f:
            reader = csv.reader(f, delimiter=self.cfg.delimiter)
            header = self._header(reader)
            for row in reader:
                if row:
                    out.append({h: (row[i] if i < len(row) else None) for i, h in enumerate(header)})
                if len(out) >= limit:
                    break
        return out


class ParquetConnector(SourceConnector):
    def __init__(self, cfg: FileSourceConfig):
        self.cfg = cfg

    def _pf(self):
        import pyarrow.parquet as pq
        try:
            return pq.ParquetFile(_check_file(self.cfg))
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"cannot read Parquet: {type(exc).__name__}") from None

    def test_connection(self) -> None:
        self._pf()

    def list_datasets(self) -> list[DatasetInfo]:
        pf = self._pf()
        cols = [ColumnInfo(f.name, str(f.type), bool(f.nullable), i + 1) for i, f in enumerate(pf.schema_arrow)]
        if len(cols) > MAX_COLUMNS:
            raise ConnectorError(f"Parquet has more than {MAX_COLUMNS} columns")
        # Row count comes from file metadata: no full scan needed.
        return [DatasetInfo("file", _safe_table_name(Path(self.cfg.path)), cols, int(pf.metadata.num_rows))]

    def sample(self, schema_name: str, table_name: str, limit: int = 5) -> list[dict]:
        limit = _clamp(limit)
        ds = self.list_datasets()[0]
        if (schema_name, table_name) != (ds.schema_name, ds.table_name):
            raise ConnectorError(f"unknown dataset {schema_name}.{table_name}")
        for batch in self._pf().iter_batches(batch_size=limit):
            return [{k: _jsonable(v) for k, v in row.items()} for row in batch.to_pylist()[:limit]]
        return []


# --------------------------------------------------------------------------- factory
def build_connector(source_type: str, config: dict) -> SourceConnector:
    """Validate config (raises SourceConfigError) and return the right connector."""
    cfg = validate_source_config(source_type, config)
    if source_type == "postgres":
        return PostgresConnector(cfg)  # type: ignore[arg-type]
    if source_type == "mysql":
        return MySQLConnector(cfg)  # type: ignore[arg-type]
    if source_type == "csv":
        return CsvConnector(cfg)  # type: ignore[arg-type]
    return ParquetConnector(cfg)  # type: ignore[arg-type]


__all__ = ["ConnectorError", "SourceConfigError", "SourceConnector", "ColumnInfo", "DatasetInfo", "build_connector"]
