"""Structured logging, request correlation and in-process API metrics.

  * every log line is ONE JSON object (timestamp, level, logger, message, request_id, + structured fields)
  * every request gets an id (client-supplied X-Request-ID is accepted only if it looks safe) that is echoed in the
    response and attached to every log line and error response
  * request bodies, query strings and Authorization headers are NEVER logged
  * unhandled exceptions are logged with a stack trace but the client only receives a generic message + request id
  * latency/error metrics are kept in memory per process (rolling window); they reset on restart - durable metrics
    (agent runs, LLM calls, SQL, pipelines, integrations) are computed from the database in services/metrics.py
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.logging_config import SAFE_REQUEST_ID, JsonFormatter, configure_logging, request_id_var  # noqa: F401 (re-exported)

# --------------------------------------------------------------------------------------- API metrics
class ApiMetrics:
    def __init__(self, maxlen: int = 5000):
        self._lock = threading.Lock()
        self._rows: deque = deque(maxlen=maxlen)      # (ts, method, route, status, ms)
        self.started_at = datetime.now(timezone.utc)

    def record(self, method: str, route: str, status: int, ms: float) -> None:
        with self._lock:
            self._rows.append((time.time(), method, route, status, ms))

    def reset(self) -> None:
        with self._lock:
            self._rows.clear()

    @staticmethod
    def _pct(sorted_vals: list[float], q: float) -> float | None:
        if not sorted_vals:
            return None
        return round(sorted_vals[min(len(sorted_vals) - 1, max(0, int(q * len(sorted_vals) + 0.999999) - 1))], 1)

    def summary(self) -> dict:
        with self._lock:
            rows = list(self._rows)
        def block(items):
            ms = sorted(r[4] for r in items)
            errs = sum(1 for r in items if r[3] >= 500)
            return {"requests": len(items), "errors_5xx": errs, "error_rate": round(errs / len(items), 4) if items else 0.0,
                    "client_errors_4xx": sum(1 for r in items if 400 <= r[3] < 500),
                    "p50_ms": self._pct(ms, 0.5), "p95_ms": self._pct(ms, 0.95)}
        by_route: dict[str, list] = {}
        for r in rows:
            by_route.setdefault(f"{r[1]} {r[2]}", []).append(r)
        return {"scope": "this API process since start (rolling window)", "since": self.started_at.isoformat(),
                "overall": block(rows), "routes": {k: block(v) for k, v in sorted(by_route.items())}}


API_METRICS = ApiMetrics()
_log = logging.getLogger("copilot.http")


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        incoming = request.headers.get("x-request-id", "")
        rid = incoming if SAFE_REQUEST_ID.match(incoming) else uuid.uuid4().hex
        token = request_id_var.set(rid)
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
        except Exception:  # noqa: BLE001 - last line of defence: log fully, answer generically
            _log.exception("unhandled_exception", extra={"method": request.method, "path": request.url.path})
            response = JSONResponse({"error": "internal_error", "message": "An internal error occurred.", "request_id": rid},
                                    status_code=500)
        ms = (time.perf_counter() - started) * 1000
        route = getattr(request.scope.get("route"), "path", None) or "unmatched"
        user = getattr(request.state, "username", None)
        API_METRICS.record(request.method, route, status, ms)
        level = logging.ERROR if status >= 500 else (logging.WARNING if status in (401, 403) else logging.INFO)
        _log.log(level, "http_request", extra={"method": request.method, "path": route if route != "unmatched" else request.url.path,
                                               "status": status, "duration_ms": round(ms, 1), "user": user})
        response.headers["X-Request-ID"] = rid
        request_id_var.reset(token)
        return response
