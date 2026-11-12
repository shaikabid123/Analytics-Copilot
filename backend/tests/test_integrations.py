"""Integrations against the bundled MOCK services on a real local port (Slack-style webhook, Jira-style tickets).
Covers success, retry/recovery, timeout, outage, rejected credentials, idempotency and graceful degradation.
Nothing here talks to real Slack or Jira."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from app.integrations import slack, tickets
from app.integrations.dispatch import dispatch_pending_alerts
from app.integrations.http import call_json
from tests.conftest import REJECT_FAIL


def calls(app_engine, integration=None):
    with app_engine.connect() as c:
        sql = "SELECT integration, operation, target, status, attempts, http_status, error, latency_ms FROM app.integration_calls"
        sql += " WHERE integration=:i" if integration else ""
        return c.execute(text(sql + " ORDER BY id DESC LIMIT 20"), {"i": integration} if integration else {}).all()


@pytest.fixture()
def alerts(app_engine, make_run):
    """Factory: create dq_alerts for a synthetic run. Returns (run_id, [alert ids])."""
    def make(severities=("high",)):
        _, rid = make_run(checks=[REJECT_FAIL])
        ids = []
        with app_engine.begin() as c:
            for i, sev in enumerate(severities):
                ids.append(c.execute(text("INSERT INTO app.dq_alerts (run_id, severity, title, detail) VALUES (:r, :s, :t, CAST(:d AS jsonb)) RETURNING id"),
                                     {"r": rid, "s": sev, "t": f"[t] customers.reject_rate failed #{i}", "d": '{"observed": 4.0, "threshold": 2.0}'}).scalar())
        return rid, ids
    return make


def alert_rows(app_engine, ids):
    with app_engine.connect() as c:
        return c.execute(text("SELECT id, status, attempts, sent_at IS NOT NULL, ticket_key, last_error, next_attempt_at FROM app.dq_alerts "
                              "WHERE id = ANY(:i) ORDER BY id"), {"i": ids}).all()


# ============================================================================= adapters: success / failure / timeout
def test_webhook_success_is_delivered_and_recorded_without_secrets(mock_integrations, app_engine):
    r = slack.send_alert("hello team", severity="high", resource=("dq_alert", "1"))
    assert r.ok and r.status == "ok" and r.attempts == 1 and r.http_status == 200
    assert mock_integrations.state()["slack"]["received"][0]["text"] == "hello team"
    row = calls(app_engine, "slack")[0]
    assert row[:6] == ("slack", "send_alert", f"127.0.0.1:{mock_integrations.port}", "ok", 1, 200)
    assert mock_integrations.token not in str(row)


def test_ticket_creation_is_authenticated_and_idempotent(mock_integrations, app_engine):
    a = tickets.create_ticket("Gate failed", "details", ["data-quality"], "key-1")
    b = tickets.create_ticket("Gate failed", "details", ["data-quality"], "key-1")          # e.g. a retry after a lost response
    c = tickets.create_ticket("Other", "details", ["data-quality"], "key-2")
    assert a.ok and a.data["key"] == b.data["key"] == "OPS-1" and c.data["key"] == "OPS-2"
    assert len(mock_integrations.state()["tickets"]["issues"]) == 2                         # no duplicate for the repeated key


def test_bad_credentials_are_rejected_once_and_never_retried(mock_integrations, monkeypatch, app_engine):
    from app.config import get_settings
    monkeypatch.setenv("TICKET_API_TOKEN", "wrong-token")
    get_settings.cache_clear()
    r = tickets.create_ticket("s", "d", [], "k")
    assert not r.ok and r.status == "rejected" and r.http_status == 401 and r.attempts == 1       # 4xx: retrying cannot help
    assert calls(app_engine, "tickets")[0][3] == "rejected"


def test_server_errors_are_retried_a_bounded_number_of_times(mock_integrations, app_engine):
    mock_integrations.control("slack", "fail")
    r = slack.send_alert("x")
    assert not r.ok and r.status == "failed" and r.attempts == 2 and r.http_status == 500       # 1 try + 1 retry (INTEGRATION_RETRIES=1)
    assert mock_integrations.state()["slack"]["requests"] == 2
    assert calls(app_engine, "slack")[0][3:5] == ("failed", 2)


def test_transient_failure_recovers_on_retry(mock_integrations):
    mock_integrations.control("slack", "flaky", fail_first=1)
    r = slack.send_alert("recovers")
    assert r.ok and r.attempts == 2
    assert [m["text"] for m in mock_integrations.state()["slack"]["received"]] == ["recovers"]


def test_slow_service_times_out_instead_of_hanging(mock_integrations, app_engine):
    mock_integrations.control("slack", "slow", sleep_s=3)
    r = slack.send_alert("slow", retries=0, timeout_s=0.3)
    assert not r.ok and r.status == "timeout" and r.attempts == 1 and r.latency_ms < 2000
    assert "timed out" in r.error and calls(app_engine, "slack")[0][3] == "timeout"


def test_unreachable_service_is_reported_not_raised(monkeypatch, app_engine):
    from app.config import get_settings
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:1/hook")
    monkeypatch.setenv("INTEGRATION_RETRIES", "0")
    get_settings.cache_clear()
    r = slack.send_alert("nobody home")
    assert not r.ok and r.status == "failed" and "unreachable" in r.error


def test_unconfigured_integrations_degrade_to_a_clear_result(monkeypatch):
    from app.config import get_settings
    for var in ("ALERT_WEBHOOK_URL", "TICKET_API_URL", "TICKET_API_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    get_settings.cache_clear()
    assert slack.send_alert("x").error == "alert webhook is not configured"
    assert tickets.create_ticket("s", "d", [], "k").error == "ticket API is not configured"
    assert dispatch_pending_alerts()["configured"] is False and slack.notify_safely("x") is False


def test_call_json_never_raises_on_garbage_urls():
    for url in ("", "not a url", "ftp://x", "http://"):
        r = call_json("slack", "t", url, {"a": 1}, timeout_s=0.2, retries=0)
        assert r.ok is False


# ============================================================================= alert dispatch
def test_dispatch_delivers_slack_for_every_alert_and_tickets_for_high_only(mock_integrations, alerts, app_engine):
    rid, ids = alerts(("high", "medium"))
    out = dispatch_pending_alerts(only_runs=[rid])
    assert out["sent"] == 2 and out["retry_later"] == 0 and not out["degraded"]
    rows = alert_rows(app_engine, ids)
    assert [(r[1], r[2], r[3]) for r in rows] == [("sent", 0, True), ("sent", 0, True)]
    assert rows[0][4] == "OPS-1" and rows[1][4] is None                          # ticket only for HIGH severity
    st = mock_integrations.state()
    assert len(st["slack"]["received"]) == 2 and len(st["tickets"]["issues"]) == 1
    assert "[HIGH]" in st["slack"]["received"][0]["text"] and "observed=4.0" in st["slack"]["received"][0]["text"]
    assert dispatch_pending_alerts(only_runs=[rid])["sent"] == 0                 # nothing left to do


def test_slack_outage_keeps_alerts_queued_with_backoff_then_recovers(mock_integrations, alerts, app_engine):
    rid, ids = alerts(("medium",))
    mock_integrations.control("slack", "fail")
    out = dispatch_pending_alerts(only_runs=[rid])
    assert out["retry_later"] == 1 and out["sent"] == 0 and out["degraded"]
    (_, status, attempts, sent, _, err, nxt), = alert_rows(app_engine, ids)
    assert (status, attempts, sent) == ("pending", 1, False) and "slack: failed" in err and nxt > datetime.now(timezone.utc)
    assert dispatch_pending_alerts(only_runs=[rid])["retry_later"] == 0          # not due yet: not hammered
    mock_integrations.control("slack", "ok")
    later = datetime.now(timezone.utc) + timedelta(hours=2)
    assert dispatch_pending_alerts(only_runs=[rid], now=later)["sent"] == 1
    assert alert_rows(app_engine, ids)[0][1] == "sent"


def test_dispatch_stops_early_when_the_integration_is_clearly_down(mock_integrations, alerts, app_engine):
    rid, ids = alerts(("medium",) * 5)
    mock_integrations.control("slack", "fail")
    out = dispatch_pending_alerts(only_runs=[rid])
    assert out["stopped_early"] and out["retry_later"] == 3
    rows = alert_rows(app_engine, ids)
    assert [r[2] for r in rows] == [1, 1, 1, 0, 0]                               # the rest were not even attempted


def test_alerts_give_up_after_a_bounded_number_of_attempts(mock_integrations, alerts, app_engine, monkeypatch):
    from app.config import get_settings
    monkeypatch.setenv("ALERT_MAX_ATTEMPTS", "2")
    get_settings.cache_clear()
    rid, ids = alerts(("medium",))
    mock_integrations.control("slack", "fail")
    now = datetime.now(timezone.utc)
    dispatch_pending_alerts(only_runs=[rid], now=now)
    out = dispatch_pending_alerts(only_runs=[rid], now=now + timedelta(hours=2))
    assert out["failed"] == 1 and alert_rows(app_engine, ids)[0][1] == "failed"
    mock_integrations.control("slack", "ok")
    assert dispatch_pending_alerts(only_runs=[rid], now=now + timedelta(hours=9))["sent"] == 0       # terminal: not retried forever


def test_ticket_outage_does_not_resend_slack_and_completes_after_recovery(mock_integrations, alerts, app_engine):
    rid, ids = alerts(("high",))
    mock_integrations.control("tickets", "fail")
    out = dispatch_pending_alerts(only_runs=[rid])
    assert out["retry_later"] == 1
    row = alert_rows(app_engine, ids)[0]
    assert row[1] == "pending" and row[3] is True and row[4] is None and "tickets: failed" in row[5]
    mock_integrations.control("tickets", "ok")
    assert dispatch_pending_alerts(only_runs=[rid], now=datetime.now(timezone.utc) + timedelta(hours=2))["sent"] == 1
    st = mock_integrations.state()
    assert len(st["slack"]["received"]) == 1 and len(st["tickets"]["issues"]) == 1       # Slack not duplicated, one ticket
    assert alert_rows(app_engine, ids)[0][4] == "OPS-1"


def test_ticket_timeout_then_recovery_never_creates_duplicates(mock_integrations, alerts, app_engine):
    rid, ids = alerts(("high",))
    mock_integrations.control("tickets", "slow", sleep_s=2)
    out = dispatch_pending_alerts(only_runs=[rid])
    assert out["retry_later"] == 1 and "tickets: timeout" in alert_rows(app_engine, ids)[0][5]
    mock_integrations.control("tickets", "ok")
    assert dispatch_pending_alerts(only_runs=[rid], now=datetime.now(timezone.utc) + timedelta(hours=2))["sent"] == 1
    assert len(mock_integrations.state()["tickets"]["issues"]) == 1                      # idempotency key held across retries


# ============================================================================= workflow behaviour
def test_dispatch_over_http_is_privileged_and_audited(api, login, mock_integrations, alerts, app_engine):
    rid, ids = alerts(("medium",))
    _, analyst = login("analyst")
    name, eng = login("engineer")
    assert api.post("/integrations/dispatch", headers=analyst).status_code == 403
    r = api.post("/integrations/dispatch", headers=eng)
    assert r.status_code == 200 and r.json()["sent"] >= 1 and alert_rows(app_engine, ids)[0][1] == "sent"
    assert any(a["id"] == ids[0] and a["status"] == "sent" for a in api.get("/integrations/alerts?status=sent&limit=200", headers=eng).json())
    hist = api.get("/integrations/calls", headers=eng).json()
    assert hist and all("token" not in str(h).lower() for h in hist)
    with app_engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM app.audit_events WHERE action='integrations.dispatch' AND actor_label=:a"), {"a": name}).scalar() == 1


def test_approval_workflow_notifies_and_survives_a_webhook_outage(mock_integrations, make_run, login, app_engine):
    from app.agents import service
    from app.llm import NullLLM
    requester, _ = login("engineer")
    reviewer, _ = login("engineer")
    _, rid = make_run(checks=[REJECT_FAIL], error="DQGateError: gate failed")
    out = service.run_request(f"why did run {rid} fail", requester, "engineer", llm=NullLLM())
    service.resume_approval(out["approval_id"], "rejected", reviewer, "engineer", llm=NullLLM())
    texts = [m["text"] for m in mock_integrations.state()["slack"]["received"]]
    assert any(f"Approval #{out['approval_id']} requested by {requester}" in t for t in texts)
    assert any(f"Approval #{out['approval_id']} rejected by {reviewer}" in t for t in texts)

    mock_integrations.control("slack", "fail")                                   # webhook down: the workflow must still work
    _, rid2 = make_run(checks=[REJECT_FAIL], error="DQGateError: gate failed")
    out2 = service.run_request(f"why did run {rid2} fail", requester, "engineer", llm=NullLLM())
    done = service.resume_approval(out2["approval_id"], "approved", reviewer, "engineer", llm=NullLLM())
    assert out2["status"] == "awaiting_approval" and done["status"] == "succeeded"
    assert {c[3] for c in calls(app_engine, "slack")[:2]} == {"failed"}          # the outage is visible in the call history
