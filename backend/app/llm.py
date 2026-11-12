"""LLM access behind one small interface. The LLM only ever returns TEXT; callers must treat it as untrusted
input (SQL goes through the guard, proposals are rebuilt from deterministic rules, etc.).

Providers: OllamaClient (local/hosted Ollama HTTP API), NullLLM (always unavailable), MockLLM (tests).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Protocol

import httpx

from app.config import get_settings


class LLMUnavailable(Exception):
    """No usable answer from the model (not configured, down, timed out, or malformed reply)."""

    def __init__(self, message: str, *, timeout: bool = False):
        super().__init__(message)
        self.timeout = timeout


@dataclass
class LLMResponse:
    text: str
    provider: str
    model: str | None
    latency_ms: int
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class LLMClient(Protocol):
    provider: str
    model: str | None

    def chat(self, system: str, user: str, *, max_tokens: int = 512) -> LLMResponse: ...


class NullLLM:
    provider, model = "none", None

    def chat(self, system: str, user: str, *, max_tokens: int = 512) -> LLMResponse:
        raise LLMUnavailable("no LLM provider configured")


class OllamaClient:
    provider = "ollama"

    def __init__(self, host: str, model: str, timeout_s: float = 60.0, retries: int = 1,
                 backoff_s: float = 0.5, sleep: Callable[[float], None] = time.sleep):
        self.host, self.model = host.rstrip("/"), model
        self.timeout_s, self.retries, self.backoff_s, self._sleep = timeout_s, retries, backoff_s, sleep

    def chat(self, system: str, user: str, *, max_tokens: int = 512) -> LLMResponse:
        body = {"model": self.model, "stream": False,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "options": {"temperature": 0, "num_predict": max_tokens}}
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            started = time.monotonic()
            try:
                r = httpx.post(f"{self.host}/api/chat", json=body, timeout=self.timeout_s)
                if r.status_code >= 500:
                    raise LLMUnavailable(f"Ollama server error {r.status_code}")
                if r.status_code != 200:
                    raise LLMUnavailable(f"Ollama rejected the request ({r.status_code})")   # not retried
                data = r.json()
                text = ((data.get("message") or {}).get("content") or "").strip()
                if not text:
                    raise LLMUnavailable("Ollama returned an empty reply")
                return LLMResponse(text, self.provider, self.model, int((time.monotonic() - started) * 1000),
                                   data.get("prompt_eval_count"), data.get("eval_count"))
            except httpx.TimeoutException:
                last = LLMUnavailable(f"Ollama timed out after {self.timeout_s:g}s", timeout=True)
            except httpx.TransportError as exc:
                last = LLMUnavailable(f"Ollama unreachable ({type(exc).__name__})")
            except ValueError:
                last = LLMUnavailable("Ollama returned invalid JSON")
            except LLMUnavailable as exc:
                if "rejected" in str(exc):
                    raise
                last = exc
            if attempt < self.retries:
                self._sleep(self.backoff_s * (2 ** attempt))
        assert last is not None
        raise last


@dataclass
class MockLLM:
    """Scripted replies for tests. Each item is a string (returned) or an Exception (raised)."""
    script: list = field(default_factory=list)
    provider: str = "mock"
    model: str | None = "mock-1"
    calls: list = field(default_factory=list)

    def chat(self, system: str, user: str, *, max_tokens: int = 512) -> LLMResponse:
        self.calls.append({"system": system, "user": user})
        if not self.script:
            raise LLMUnavailable("mock script exhausted")
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return LLMResponse(str(item), self.provider, self.model, 1, len(user) // 4, len(str(item)) // 4)


def get_llm() -> LLMClient:
    s = get_settings()
    if s.llm_provider == "ollama":
        return OllamaClient(s.ollama_host, s.ollama_model, s.ollama_timeout_s)
    return NullLLM()
