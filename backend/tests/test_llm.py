"""Ollama client tested against a local FAKE Ollama server (no real model is involved: this proves the HTTP
contract, retries, timeouts and failure handling, NOT the quality of any model's SQL)."""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from sqlalchemy import text

from app.config import get_settings
from app.llm import LLMUnavailable, MockLLM, NullLLM, OllamaClient, get_llm

GOOD = "SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel"


class Fake:
    def __init__(self):
        self.script, self.requests = [], []

    def __enter__(self):
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # silence
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.requests.append({"path": self.path, "body": body})
                kind, payload = fake.script.pop(0) if fake.script else ("ok", "SELECT 1")
                if kind == "sleep":
                    time.sleep(payload)
                    kind, payload = "ok", "late"
                if kind == "status":
                    self.send_response(payload); self.end_headers(); return
                data = (b"not json" if kind == "garbage" else
                        json.dumps({"message": {"content": payload}, "prompt_eval_count": 12, "eval_count": 5}).encode())
                self.send_response(200); self.send_header("Content-Length", str(len(data))); self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.host = f"http://127.0.0.1:{self.server.server_port}"
        return self

    def __exit__(self, *a):
        self.server.shutdown()


def client(host, **kw):
    sleeps = []
    c = OllamaClient(host, "qwen2.5-coder:3b", timeout_s=kw.pop("timeout_s", 5), sleep=sleeps.append, **kw)
    c.sleeps = sleeps
    return c


def test_request_format_and_response_parsing():
    with Fake() as f:
        f.script = [("ok", "  SELECT 1  ")]
        r = client(f.host).chat("sys prompt", "user prompt", max_tokens=77)
    req = f.requests[0]
    assert req["path"] == "/api/chat" and req["body"]["model"] == "qwen2.5-coder:3b" and req["body"]["stream"] is False
    assert [m["role"] for m in req["body"]["messages"]] == ["system", "user"]
    assert req["body"]["options"] == {"temperature": 0, "num_predict": 77}
    assert (r.text, r.prompt_tokens, r.completion_tokens, r.provider) == ("SELECT 1", 12, 5, "ollama") and r.latency_ms >= 0


def test_server_errors_are_retried_with_backoff_then_succeed():
    with Fake() as f:
        f.script = [("status", 503), ("ok", "SELECT 2")]
        c = client(f.host, retries=1, backoff_s=0.5)
        assert c.chat("s", "u").text == "SELECT 2"
    assert len(f.requests) == 2 and c.sleeps == [0.5]


def test_retries_are_bounded():
    with Fake() as f:
        f.script = [("status", 500)] * 5
        c = client(f.host, retries=2)
        with pytest.raises(LLMUnavailable, match="server error"):
            c.chat("s", "u")
    assert len(f.requests) == 3 and c.sleeps == [0.5, 1.0]


def test_client_errors_are_not_retried():
    with Fake() as f:
        f.script = [("status", 404)]
        with pytest.raises(LLMUnavailable, match="rejected"):
            client(f.host, retries=3).chat("s", "u")
    assert len(f.requests) == 1                                  # e.g. model not pulled: retrying cannot help


def test_timeout_is_reported_as_timeout():
    with Fake() as f:
        f.script = [("sleep", 1.0), ("sleep", 1.0)]
        with pytest.raises(LLMUnavailable) as e:
            client(f.host, timeout_s=0.2, retries=1).chat("s", "u")
    assert e.value.timeout is True and "timed out" in str(e.value)


def test_unreachable_server_and_bad_replies():
    with pytest.raises(LLMUnavailable, match="unreachable"):
        client("http://127.0.0.1:1", retries=1).chat("s", "u")
    with Fake() as f:
        f.script = [("garbage", None)] * 2
        with pytest.raises(LLMUnavailable, match="invalid JSON"):
            client(f.host, retries=1).chat("s", "u")
    with Fake() as f:
        f.script = [("ok", "   ")] * 2
        with pytest.raises(LLMUnavailable, match="empty"):
            client(f.host, retries=1).chat("s", "u")


def test_provider_selection(monkeypatch):
    for value, cls in (("ollama", OllamaClient), ("none", NullLLM), ("something-else", NullLLM)):
        monkeypatch.setenv("LLM_PROVIDER", value)
        get_settings.cache_clear()
        assert isinstance(get_llm(), cls)
    monkeypatch.delenv("LLM_PROVIDER")
    get_settings.cache_clear()
    assert isinstance(get_llm(), NullLLM)                         # default: deterministic fallbacks only
    with pytest.raises(LLMUnavailable):
        NullLLM().chat("s", "u")
    with pytest.raises(LLMUnavailable, match="exhausted"):
        MockLLM([]).chat("s", "u")


def test_agent_run_through_the_real_ollama_client(app_engine):
    from app.agents import service
    with Fake() as f:
        f.script = [("ok", f"```sql\n{GOOD}\n```")]
        out = service.run_request("total orders by channel", "alice", "analyst", llm=client(f.host))
    assert out["status"] == "succeeded" and out["state"]["sql_source"] == "llm" and out["state"]["sql_result"]["row_count"] == 3
    assert "<question>total orders by channel</question>" in f.requests[0]["body"]["messages"][1]["content"]
    with app_engine.connect() as c:
        row = c.execute(text("SELECT provider, model, status, prompt_tokens, completion_tokens FROM app.llm_calls WHERE agent_run_id=:r"),
                        {"r": out["run_id"]}).one()
    assert tuple(row) == ("ollama", "qwen2.5-coder:3b", "ok", 12, 5)


def test_agent_run_survives_ollama_outage_and_records_it(app_engine):
    from app.agents import service
    with Fake() as f:
        f.script = [("status", 500), ("status", 500)]
        out = service.run_request("total orders by channel", "alice", "analyst", llm=client(f.host, retries=1))
    assert out["status"] == "succeeded" and out["state"]["sql_source"] == "template"
    assert "llm_unavailable:sql" in out["state"]["degraded_reasons"] and "deterministic fallback" in out["answer"]
    with app_engine.connect() as c:
        st = c.execute(text("SELECT status, error FROM app.llm_calls WHERE agent_run_id=:r"), {"r": out["run_id"]}).one()
    assert st[0] == "error" and "server error" in st[1]
