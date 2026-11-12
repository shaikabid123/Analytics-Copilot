"""Structured logging, request correlation, error handling and metrics."""
import io
import json
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.observability import API_METRICS, JsonFormatter, RequestContextMiddleware, configure_logging
from tests.conftest import PASSWORD


@pytest.fixture()
def logs():
    """Capture everything logged during a test as parsed JSON lines (through the real JsonFormatter)."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    old = root.level
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    class L:
        raw = property(lambda self: buf.getvalue())
        lines = property(lambda self: [json.loads(x) for x in buf.getvalue().splitlines() if x.strip()])
        def http(self): return [l for l in self.lines if l["msg"] == "http_request"]
    yield L()
    root.removeHandler(handler)
    root.setLevel(old)


# ------------------------------------------------------------------------------------ logging + correlation
def test_every_request_is_logged_as_one_json_object_with_a_correlation_id(api, login, logs):
    name, h = login("analyst")
    r = api.get("/catalog/datasets/1", headers=h)
    entry = [l for l in logs.http() if l["path"] == "/catalog/datasets/{dataset_id}"][-1]     # route TEMPLATE, not raw id
    assert entry["status"] == 200 and entry["method"] == "GET" and entry["user"] == name
    assert entry["request_id"] == r.headers["x-request-id"] and entry["duration_ms"] >= 0
    assert set(entry) >= {"ts", "level", "logger", "msg", "request_id", "method", "path", "status", "duration_ms", "user"}


def test_client_supplied_request_ids_are_accepted_only_when_safe(api):
    assert api.get("/health", headers={"X-Request-ID": "trace-1234-abcd"}).headers["x-request-id"] == "trace-1234-abcd"
    for bad in ("short", "has space in it!!", "x" * 100, "inj\\nected-line-aaaa", "<script>alert(1)</script>"):
        got = api.get("/health", headers={"X-Request-ID": bad}).headers["x-request-id"]
        assert got != bad and len(got) == 32                                      # replaced by a generated id


def test_auth_failures_are_logged_as_warnings_and_server_errors_as_errors(api, login, logs):
    api.get("/sources")
    _, h = login("analyst")
    api.get("/admin/users", headers=h)
    by_status = {l["status"]: l["level"] for l in logs.http() if l["path"] in ("/sources", "/admin/users")}
    assert by_status == {401: "WARNING", 403: "WARNING"}


def test_secrets_never_reach_the_logs(api, users, logs):
    name, pw = users("analyst")
    tok = api.post("/auth/login", json={"username": name, "password": pw}).json()["access_token"]
    api.post("/auth/login", json={"username": name, "password": "wrong-password-xyz"})
    api.post("/query/execute", json={"sql": "SELECT channel FROM gold.daily_sales"}, headers={"Authorization": f"Bearer {tok}"})
    raw = logs.raw
    for secret in (pw, "wrong-password-xyz", tok, "Bearer", "password", "SELECT channel"):
        assert secret not in raw, secret                                          # no credentials, tokens or request bodies


def test_integration_urls_are_not_logged_even_at_info_level(mock_integrations, logs):
    from app.integrations import slack
    configure_logging("INFO")                                                     # the real app configuration
    slack.send_alert("hello")
    assert "slack/webhook" not in logs.raw and str(mock_integrations.port) not in logs.raw   # a webhook URL is itself a secret
    assert logging.getLogger("httpx").level >= logging.WARNING


def test_unhandled_errors_return_a_generic_message_with_the_request_id_and_log_the_traceback(logs):
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)

    @app.get("/boom")
    def boom():
        raise RuntimeError("secret internal detail: db password is hunter2")

    r = TestClient(app, raise_server_exceptions=False).get("/boom", headers={"X-Request-ID": "trace-boom-0001"})
    assert r.status_code == 500 and r.json() == {"error": "internal_error", "message": "An internal error occurred.", "request_id": "trace-boom-0001"}
    assert "hunter2" not in r.text and "Traceback" not in r.text and r.headers["x-request-id"] == "trace-boom-0001"
    err = [l for l in logs.lines if l["msg"] == "unhandled_exception"][0]
    assert err["request_id"] == "trace-boom-0001" and "RuntimeError" in err["exc"] and err["level"] == "ERROR"
    assert [l for l in logs.http() if l["status"] == 500][0]["level"] == "ERROR"


def test_formatter_output_is_always_valid_json_even_for_awkward_messages():
    f = JsonFormatter()
    rec = logging.LogRecord("x", logging.INFO, "f", 1, 'quote " and \\ backslash\nnewline %s', ("é\u2028",), None)
    rec.extra_field = {"k": object()}
    out = json.loads(f.format(rec))
    assert out["msg"].startswith('quote " and') and "extra_field" in out


# ------------------------------------------------------------------------------------ in-process API metrics
def test_api_metrics_report_counts_error_classes_and_percentiles_by_route(api, login):
    API_METRICS.reset()
    _, h = login("engineer")
    for _ in range(5):
        api.get("/catalog/datasets", headers=h)
    api.get("/sources")                                        # 401
    api.get("/admin/users", headers=h)                         # 403
    m = api.get("/metrics/summary", headers=h).json()["api"]
    route = m["routes"]["GET /catalog/datasets"]
    assert route["requests"] == 5 and route["p50_ms"] > 0 and route["p95_ms"] >= route["p50_ms"] and route["errors_5xx"] == 0
    assert m["routes"]["GET /sources"]["client_errors_4xx"] == 1 and m["routes"]["GET /admin/users"]["client_errors_4xx"] == 1
    assert m["overall"]["requests"] >= 8 and m["overall"]["error_rate"] == 0.0 and "process" in m["scope"]


def test_percentile_helper_uses_nearest_rank_and_handles_empty():
    from app.observability import ApiMetrics
    assert ApiMetrics._pct([], 0.5) is None
    vals = [float(i) for i in range(1, 101)]
    assert ApiMetrics._pct(vals, 0.5) == 50.0 and ApiMetrics._pct(vals, 0.95) == 95.0 and ApiMetrics._pct([7.0], 0.95) == 7.0


# ------------------------------------------------------------------------------------ durable metrics
def test_metrics_summary_is_privileged_and_consistent_with_the_database(api, login, app_engine, monkeypatch):
    from app.agents import service
    from app.config import get_settings
    from app.llm import MockLLM
    monkeypatch.setenv("LLM_PROMPT_COST_PER_1K", "2.0")
    monkeypatch.setenv("LLM_COMPLETION_COST_PER_1K", "4.0")
    get_settings.cache_clear()
    _, analyst = login("analyst")
    _, eng = login("engineer")
    assert api.get("/metrics/summary", headers=analyst).status_code == 403
    assert api.get("/metrics/summary?days=0", headers=eng).status_code == 422
    out = service.run_request("total orders by channel", "metrics_tester", "analyst",
                              llm=MockLLM(["SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel"]))
    m = api.get("/metrics/summary?days=1", headers=eng).json()
    with app_engine.connect() as c:
        runs = c.execute(text("SELECT count(*) FROM app.agent_runs WHERE created_at >= now() - interval '1 day'")).scalar()
        calls, pt, ct, tasks = c.execute(text("SELECT count(*), coalesce(sum(prompt_tokens),0), coalesce(sum(completion_tokens),0), count(DISTINCT agent_run_id) "
                                              "FROM app.llm_calls WHERE created_at >= now() - interval '1 day'")).one()
        executed = c.execute(text("SELECT count(*) FROM app.query_audits WHERE status='executed' AND created_at >= now() - interval '1 day'")).scalar()
    assert m["agent_workflow"]["runs"] == runs and m["agent_workflow"]["latency_p50_ms"] > 0
    assert m["agent_workflow"]["latency_p95_ms"] >= m["agent_workflow"]["latency_p50_ms"]
    assert m["llm"]["calls"] == calls and m["llm"]["prompt_tokens"] == pt and m["llm"]["completion_tokens"] == ct
    assert m["llm"]["avg_tokens_per_task"] == round((pt + ct) / tasks, 1)
    assert m["llm"]["avg_cost_usd_per_task"] == round((pt * 2.0 + ct * 4.0) / 1000 / tasks, 6)       # cost is computed, not invented
    assert m["sql"]["by_status"]["executed"] == executed and m["sql"]["executed_p50_ms"] is not None
    assert any(i["intent"] == "sql" for i in m["agent_workflow"]["by_intent"]) and out["status"] == "succeeded"


def test_metrics_cover_pipeline_sla_and_integrations(api, login, app_engine, make_run, mock_integrations):
    from app.integrations import slack
    slack.send_alert("metric probe")
    _, eng = login("engineer")
    m = api.get("/metrics/summary?days=30", headers=eng).json()
    pipes = {p["pipeline"]: p for p in m["pipelines"]}
    assert set(pipes) == {"sales", "events"}
    for p in pipes.values():
        assert {"success_rate", "duration_p50_s", "duration_p95_s", "hours_since_success", "sla_hours", "sla_breached"} <= set(p)
        assert p["sla_breached"] == (p["hours_since_success"] is None or p["hours_since_success"] > p["sla_hours"])
    slack_m = next(i for i in m["integrations"] if i["integration"] == "slack")
    assert slack_m["calls"] >= 1 and 0.0 <= slack_m["success_rate"] <= 1.0 and slack_m["latency_p95_ms"] is not None
    assert isinstance(m["alerts"], dict)
