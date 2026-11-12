"""Read-only SQL execution with the analytics_ro role: EXPLAIN cost check, statement timeout, row cap.
The role itself can only SELECT from gold; this module adds limits on top (defence in depth)."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import create_engine, pool, text
from sqlalchemy.engine import Engine

from app.config import get_settings
from app.services.sql_guard import MAX_ROWS, validate_sql

MAX_COST = 100_000.0
TIMEOUT_MS = 5_000

_engine: Engine | None = None


class QueryError(Exception):
    """Execution-side failure with a safe, user-presentable message."""


class GuardRejected(QueryError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


class CostExceeded(QueryError):
    def __init__(self, cost: float):
        super().__init__(f"estimated cost {cost:,.0f} exceeds the limit of {MAX_COST:,.0f}")
        self.cost = cost


@dataclass
class QueryResult:
    sql: str
    columns: list[str]
    rows: list[list]
    row_count: int
    truncated: bool
    duration_ms: int
    estimated_cost: float


def get_ro_engine() -> Engine:
    global _engine
    url = get_settings().analytics_ro_url
    if not url:
        raise QueryError("ANALYTICS_RO_URL is not configured")
    if _engine is None:
        _engine = create_engine(url, poolclass=pool.NullPool,
                                connect_args={"options": "-c default_transaction_read_only=on"})
    return _engine


def _json_safe(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _db_message(exc: Exception) -> str:
    orig = getattr(exc, "orig", exc)
    first = str(orig).strip().splitlines()[0] if str(orig).strip() else type(exc).__name__
    return first[:200]


def explain_cost(sql: str, engine: Engine | None = None) -> tuple[float, float]:
    """(total cost, estimated rows) from EXPLAIN (no ANALYZE: the statement is not executed)."""
    eng = engine or get_ro_engine()
    try:
        with eng.connect() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql(f"SET LOCAL statement_timeout = {TIMEOUT_MS}")
            raw = conn.execute(text(f"EXPLAIN (FORMAT JSON) {sql}")).scalar()
    except Exception as exc:  # noqa: BLE001
        raise QueryError(f"query could not be planned: {_db_message(exc)}") from None
    plan = (json.loads(raw) if isinstance(raw, str) else raw)[0]["Plan"]
    return float(plan["Total Cost"]), float(plan["Plan Rows"])


def execute_readonly(sql: str, *, timeout_ms: int = TIMEOUT_MS, max_rows: int = MAX_ROWS,
                     engine: Engine | None = None) -> tuple[list[str], list[list], bool, int]:
    """Run ALREADY-VALIDATED SQL. Read-only transaction, statement timeout, row cap."""
    eng = engine or get_ro_engine()
    started = time.monotonic()
    try:
        with eng.connect() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            result = conn.execute(text(sql))
            cols = list(result.keys())
            fetched = result.fetchmany(max_rows + 1)
    except Exception as exc:  # noqa: BLE001
        msg = _db_message(exc)
        if "statement timeout" in msg:
            raise QueryError(f"query exceeded the {timeout_ms} ms time limit") from None
        raise QueryError(f"query failed: {msg}") from None
    truncated = len(fetched) > max_rows
    rows = [[_json_safe(v) for v in r] for r in fetched[:max_rows]]
    return cols, rows, truncated, int((time.monotonic() - started) * 1000)


def run_guarded_query(sql: str, role: str, *, max_cost: float = MAX_COST) -> QueryResult:
    """guard -> cost estimate -> execute. Raises GuardRejected / CostExceeded / QueryError."""
    guard = validate_sql(sql, role)
    if not guard.ok:
        raise GuardRejected(guard.errors)
    cost, _rows = explain_cost(guard.sql)  # type: ignore[arg-type]
    if cost > max_cost:
        raise CostExceeded(cost)
    cols, rows, truncated, ms = execute_readonly(guard.sql)  # type: ignore[arg-type]
    return QueryResult(guard.sql, cols, rows, len(rows), truncated, ms, cost)  # type: ignore[arg-type]
