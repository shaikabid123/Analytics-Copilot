"""Clear error handling, recovery and log hygiene in the UI's API client and pages."""
import io
import json
import logging
import threading
import time
from unittest import mock

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

import api_client as ac
from app.observability import JsonFormatter, RequestContextMiddleware
from tests_ui.conftest import PASSWORD, home, run_view, texts


@pytest.fixture()
def captured_logs():
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.addHandler(handler)
    old = root.level
    root.setLevel(logging.INFO)
    ui_log = logging.getLogger("copilot.ui")
    ui_log.propagate = True                                    # let the UI's records reach the capture handler too
    yield lambda: [json.loads(x) for x in buf.getvalue().splitlines() if x.strip()], lambda: buf.getvalue()
    ui_log.propagate = False
    root.removeHandler(handler)
    root.setLevel(old)


def test_every_failure_class_maps_to_a_distinct_clear_message(live_api, ui_user):
    anon = ac.Api(live_api)
    res = anon.get("/sources")
    assert (res.kind, res.status) == ("unauthenticated", 401) and "log in again" in res.error
    _, analyst = ui_user("analyst")
    _, eng = ui_user("engineer")
    _, admin = ui_user("admin")
    res = ac.Api(live_api, analyst).get("/admin/users")
    assert res.kind == "forbidden" and res.error.startswith("Not permitted:")
    res = ac.Api(live_api, eng).get("/approvals/987654321")
    assert res.kind == "not_found" and "not found" in res.error
    body = {"username": "t_dup_user_zz", "password": "Strong-Passphrase-7", "role": "analyst"}
    a = ac.Api(live_api, admin)
    try:
        assert a.post("/admin/users", body).ok
        dup = a.post("/admin/users", body)
        assert dup.kind == "conflict" and "already exists" in dup.error
    finally:
        from sqlalchemy import create_engine, text
        import os
        with create_engine(os.environ["DATABASE_URL"]).begin() as c:
            c.execute(text("DELETE FROM app.users WHERE username = 't_dup_user_zz'"))
    bad = ac.Api(live_api, eng).post("/approvals/1/decision", {"decision": "maybe"})
    assert bad.kind == "invalid" and bad.error.startswith("Please check your input") and "decision" in bad.error


def test_server_errors_show_a_generic_message_plus_the_request_id():
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)

    @app.get("/boom")
    def boom():
        raise RuntimeError("db password is hunter2")

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="critical"))
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)
    try:
        base = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
        res = ac.Api(base).get("/boom")
    finally:
        server.should_exit = True
    assert res.kind == "server" and res.status == 500 and "internal error" in res.error and "hunter2" not in res.error
    assert res.request_id and len(res.request_id) == 32


def test_timeouts_and_outages_are_reported_not_raised():
    with mock.patch("httpx.request", side_effect=httpx.ReadTimeout("slow")):
        t = ac.Api("http://x").get("/y")
    assert t.kind == "timeout" and "too long" in t.error and t.request_id
    with mock.patch("httpx.request", side_effect=httpx.ConnectError("refused")):
        u = ac.Api("http://x").get("/y")
    assert u.kind == "unreachable" and "Cannot reach the API" in u.error


def test_pages_show_server_errors_with_a_request_id_and_do_not_crash(ui_user):
    _, tok = ui_user("analyst")
    boom = ac.Result(False, 500, None, "The server hit an internal error. Please retry; if it persists give support this request id.", "server", "abc123req")
    with mock.patch.object(ac.Api, "request", return_value=boom):
        at = run_view("catalog", tok)
    assert not at.exception and "internal error" in texts(at) and "abc123req" in texts(at)


def test_a_page_survives_an_api_outage_and_recovers_when_the_service_returns(live_api, ui_user, monkeypatch):
    _, tok = ui_user("analyst")
    at = run_view("query", tok)
    monkeypatch.setenv("API_URL", "http://127.0.0.1:1")
    at.text_area(key="ask_q").set_value("total orders by channel")
    at.button(key="ask_go").click().run()
    assert not at.exception and "Cannot reach the API" in texts(at) and not at.code
    monkeypatch.setenv("API_URL", live_api)                                         # the operator fixed it: just click again
    at.button(key="ask_go").click().run()
    assert not at.exception and "Cannot reach the API" not in texts(at) and any("gold.daily_sales" in c.value for c in at.code)


def test_an_expired_session_mid_page_returns_the_user_to_login_with_an_explanation(ui_user):
    at = run_view("catalog", "expired.or.forged")
    assert "session has expired" in texts(at) and "token" not in at.session_state.to_dict()


def test_ui_and_api_logs_share_the_request_id_and_contain_no_secrets(live_api, users, captured_logs):
    lines, raw = captured_logs
    name, pw = users("analyst")
    ac.Api(live_api).login(name, "wrong-password-xyz")
    tok = ac.Api(live_api).login(name, pw).data["access_token"]
    res = ac.Api(live_api, tok).get("/catalog/datasets")
    assert res.ok
    entries = lines()
    ui = [e for e in entries if e["logger"] == "copilot.ui" and e.get("path") == "/catalog/datasets"][0]
    srv = [e for e in entries if e["logger"] == "copilot.http" and e.get("path") == "/catalog/datasets"][0]
    assert ui["request_id"] == srv["request_id"] == res.request_id                  # one id follows the request across both processes
    assert any(e["logger"] == "copilot.ui" and e.get("kind") == "unauthenticated" for e in entries)   # the wrong password was logged as an event
    for secret in (pw, "wrong-password-xyz", tok, "Bearer"):
        assert secret not in raw(), secret
