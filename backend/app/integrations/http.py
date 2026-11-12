"""One careful HTTP caller for all integrations: timeout, bounded retry with backoff, and an outcome that is
always recorded (host only; never URLs with tokens, headers or payloads). It never raises."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import httpx
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_engine
from app.models import IntegrationCall


@dataclass
class CallResult:
    ok: bool
    status: str                       # ok | timeout | failed | rejected
    http_status: int | None = None
    attempts: int = 0
    latency_ms: int | None = None
    error: str | None = None
    data: dict = field(default_factory=dict)


def _host(url: str) -> str:
    u = urlparse(url)
    return f"{u.hostname}:{u.port}" if u.port else (u.hostname or "?")


def _record(integration: str, operation: str, url: str, r: CallResult, resource: tuple[str, str] | None) -> None:
    try:
        with Session(get_engine()) as s:
            s.add(IntegrationCall(integration=integration, operation=operation, target=_host(url)[:120], status=r.status,
                                  attempts=r.attempts, latency_ms=r.latency_ms, http_status=r.http_status,
                                  error=(r.error or None) and r.error[:300],
                                  resource_type=resource[0] if resource else None,
                                  resource_id=str(resource[1])[:64] if resource else None))
            s.commit()
    except Exception:  # noqa: BLE001 - bookkeeping must never break the caller
        pass


def call_json(integration: str, operation: str, url: str, payload: dict, *, headers: dict | None = None,
              timeout_s: float | None = None, retries: int | None = None, backoff_s: float = 0.3,
              resource: tuple[str, str] | None = None, sleep=time.sleep) -> CallResult:
    """POST JSON. Retries timeouts, connection errors and 5xx; never retries 4xx (retrying cannot fix them)."""
    cfg = get_settings()
    timeout_s = cfg.integration_timeout_s if timeout_s is None else timeout_s
    retries = cfg.integration_retries if retries is None else retries
    started = time.monotonic()
    result = CallResult(False, "failed")
    for attempt in range(1, retries + 2):
        result.attempts = attempt
        try:
            resp = httpx.post(url, json=payload, headers=headers or {}, timeout=timeout_s)
            result.http_status = resp.status_code
            if 200 <= resp.status_code < 300:
                try:
                    body = resp.json()
                except ValueError:
                    body = {"text": resp.text[:200]}
                result.ok, result.status, result.error = True, "ok", None
                result.data = body if isinstance(body, dict) else {"value": body}
                break
            if 400 <= resp.status_code < 500:
                result.status, result.error = "rejected", f"HTTP {resp.status_code}"
                break                                                    # not retried
            result.status, result.error = "failed", f"HTTP {resp.status_code}"
        except httpx.TimeoutException:
            result.status, result.error = "timeout", f"timed out after {timeout_s:g}s"
        except httpx.TransportError as exc:
            result.status, result.error = "failed", f"unreachable ({type(exc).__name__})"
        if attempt <= retries:
            sleep(backoff_s * (2 ** (attempt - 1)))
    result.latency_ms = int((time.monotonic() - started) * 1000)
    _record(integration, operation, url, result, resource)
    return result
