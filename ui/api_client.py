"""The ONLY way the UI talks to the system: HTTP calls to the secured API using the logged-in user's token.
The UI holds no database credentials and imports no backend code; hiding a page is a convenience, the API decides.

Every call returns a Result (never raises) with a human-readable error and the request id for support/log correlation.
Passwords and tokens are never logged."""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
import streamlit as st

log = logging.getLogger("copilot.ui")


class _JsonFormatter(logging.Formatter):
    _skip = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime", "taskName"}

    def format(self, record):
        out = {"ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"), "level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        out.update({k: v for k, v in record.__dict__.items() if k not in self._skip and not k.startswith("_")})
        return json.dumps(out, default=str)


def setup_logging() -> None:
    if not any(getattr(h, "_copilot_ui", False) for h in log.handlers):
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        h._copilot_ui = True
        log.addHandler(h)
        log.setLevel(logging.INFO)
        log.propagate = False


setup_logging()


def base_url() -> str:
    return os.environ.get("API_URL", "http://localhost:8000")


@dataclass
class Result:
    ok: bool
    status: int | None
    data: Any = None
    error: str | None = None
    kind: str = "ok"          # ok | unauthenticated | forbidden | not_found | conflict | invalid | server | unreachable | timeout | error
    request_id: str | None = None
    reasons: list[str] | None = None


def _detail(body: Any) -> str:
    if isinstance(body, dict):
        d = body.get("detail", body.get("message"))
        if isinstance(d, list):                                   # FastAPI validation errors
            return "; ".join(f"{'.'.join(str(x) for x in e.get('loc', [])[1:]) or 'input'}: {e.get('msg')}" for e in d)
        if isinstance(d, str):
            return d
        if body.get("reasons"):
            return "; ".join(str(r) for r in body["reasons"])
    return ""


class Api:
    def __init__(self, base: str | None = None, token: str | None = None, timeout: float = 30.0):
        self.base, self.token, self.timeout = (base or base_url()).rstrip("/"), token, timeout

    def request(self, method: str, path: str, *, json_body: Any = None, params: dict | None = None) -> Result:
        rid = uuid.uuid4().hex
        headers = {"X-Request-ID": rid}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        started = time.perf_counter()
        try:
            r = httpx.request(method, f"{self.base}{path}", json=json_body, params=params, headers=headers, timeout=self.timeout)
        except httpx.TimeoutException:
            return self._fail(None, "timeout", "The server took too long to respond. Please try again.", rid, method, path, started)
        except httpx.TransportError:
            return self._fail(None, "unreachable", f"Cannot reach the API at {self.base}. Check that it is running, then retry.", rid, method, path, started)
        rid = r.headers.get("x-request-id", rid)
        try:
            body = r.json()
        except ValueError:
            body = None
        ms = int((time.perf_counter() - started) * 1000)
        if 200 <= r.status_code < 300:
            log.info("api_call", extra={"method": method, "path": path, "status": r.status_code, "ms": ms, "request_id": rid})
            return Result(True, r.status_code, body, request_id=rid)
        detail = _detail(body)
        reasons = body.get("reasons") if isinstance(body, dict) and isinstance(body.get("reasons"), list) else None
        kinds = {401: "unauthenticated", 403: "forbidden", 404: "not_found", 409: "conflict", 422: "invalid"}
        kind = kinds.get(r.status_code, "server" if r.status_code >= 500 else "error")
        msg = {"unauthenticated": "Your session has expired or is invalid. Please log in again.",
               "forbidden": "Not permitted: " + (detail or "your role cannot perform this action."),
               "not_found": detail or "That item was not found.",
               "conflict": detail or "That action conflicts with the current state.",
               "invalid": "Please check your input: " + detail,
               "server": "The server hit an internal error. Please retry; if it persists give support this request id.",
               "error": detail or f"The request failed (HTTP {r.status_code})."}[kind]
        return self._fail(r.status_code, kind, msg, rid, method, path, started, reasons)

    def _fail(self, status, kind, msg, rid, method, path, started, reasons=None) -> Result:
        log.warning("api_error", extra={"method": method, "path": path, "status": status, "kind": kind, "request_id": rid,
                                        "ms": int((time.perf_counter() - started) * 1000)})
        return Result(False, status, None, msg, kind, rid, reasons)

    def get(self, path, **params):
        return self.request("GET", path, params={k: v for k, v in params.items() if v is not None})

    def post(self, path, body=None):
        return self.request("POST", path, json_body=body)

    def patch(self, path, body):
        return self.request("PATCH", path, json_body=body)

    def delete(self, path):
        return self.request("DELETE", path)

    def login(self, username: str, password: str) -> Result:
        return self.request("POST", "/auth/login", json_body={"username": username, "password": password})


# ------------------------------------------------------------------------------------------ Streamlit helpers
SESSION_KEYS = ("token", "username", "role")


def api() -> Api:
    return Api(token=st.session_state.get("token"))


def logout(notice: str | None = None) -> None:
    for k in SESSION_KEYS:
        st.session_state.pop(k, None)
    if notice:
        st.session_state["notice"] = notice


def require_login() -> None:
    if not st.session_state.get("token"):
        st.warning("Please log in first.")
        st.stop()


def render_error(res: Result) -> None:
    st.error(res.error)
    if res.reasons:
        for reason in res.reasons:
            st.caption(f"• {reason}")
    if res.request_id and res.kind in ("server", "unreachable", "timeout", "error"):
        st.caption(f"Request id: `{res.request_id}`")
    if res.kind == "unauthenticated":
        logout("Your session expired. Please log in again.")
        st.button("Back to login", on_click=lambda: None)


def need(res: Result, *, stop: bool = True):
    """Return the data of a successful call; otherwise show a clear error (and stop the page unless told not to)."""
    if res.ok:
        return res.data
    render_error(res)
    if stop:
        st.stop()
    return None


def df(columns, rows):
    import pandas as pd
    return pd.DataFrame(rows, columns=columns)
