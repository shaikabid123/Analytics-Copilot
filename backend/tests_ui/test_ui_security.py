"""The UI cannot bypass backend authorization: hidden pages are cosmetic, every action is enforced by the API, the UI
holds no database access, and nothing it sends can assert a role or identity."""
import json
import re
from pathlib import Path
from unittest import mock

import httpx
import pytest
import yaml

import api_client as ac
from tests_ui.conftest import REPO, UI, home, run_view, texts

PRIVILEGED_VIEWS = ["pipelines", "quality", "approvals", "monitoring"]
ACTION_BUTTON = re.compile(r"^(run_|approved_|rejected_|act_|revert_|q_dispatch|mu_|nu_)")


@pytest.mark.parametrize("view", PRIVILEGED_VIEWS + ["admin"])
def test_privileged_pages_rendered_for_an_analyst_show_the_backend_refusal_and_no_data_or_actions(ui_user, view):
    name, tok = ui_user("analyst")
    at = run_view(view, tok, role="admin", username="root")                  # forged session claims on top of a real analyst token
    assert not at.exception
    assert "Not permitted" in texts(at), texts(at)[-300:]
    assert len(at.dataframe) == 0 and len(at.metric) == 0
    assert not [b.key for b in at.button if b.key and ACTION_BUTTON.match(b.key)]


def test_engineers_are_refused_on_the_admin_page(ui_user):
    _, tok = ui_user("engineer")
    at = run_view("admin", tok, role="admin")
    assert "Not permitted" in texts(at) and len(at.dataframe) == 0 and not [b for b in at.button if b.key and b.key.startswith(("mu_", "nu_"))]


def test_forged_session_role_does_not_change_the_navigation_offered(ui_user):
    _, tok = ui_user("analyst")
    captured = {}
    with mock.patch("streamlit.navigation") as nav:
        nav.side_effect = lambda pages, **kw: (captured.update(pages=pages), mock.MagicMock())[1]
        at = __import__("streamlit.testing.v1", fromlist=["AppTest"]).AppTest.from_file(str(UI / "Home.py"), default_timeout=60)
        at.session_state["token"], at.session_state["role"] = tok, "admin"        # attacker edits the session role
        at.run()
    assert list(captured["pages"]) == ["Explore"]
    assert at.session_state["role"] == "analyst"                                   # overwritten with what the backend says


@pytest.mark.parametrize("method,path,body", [
    ("GET", "/approvals", None), ("POST", "/approvals/1/decision", {"decision": "approved"}), ("GET", "/pipelines", None),
    ("POST", "/pipelines/events/runs", None), ("POST", "/pipelines/runs/1/retry", None), ("POST", "/integrations/dispatch", None),
    ("GET", "/metrics/summary", None), ("GET", "/admin/users", None), ("GET", "/audit/events", None),
    ("DELETE", "/pipelines/sales/overrides/max_reject_pct", None), ("POST", "/pipelines/runs/1/abandon", None),
])
def test_the_ui_client_gains_no_privileges_over_the_raw_api(live_api, ui_user, method, path, body):
    _, tok = ui_user("analyst")
    res = ac.Api(live_api, tok).request(method, path, json_body=body)
    assert not res.ok and res.kind == "forbidden" and res.status == 403 and res.error.startswith("Not permitted")


def test_every_ui_request_carries_the_users_own_token_and_never_asserts_identity(live_api, ui_user, make_run):
    from app.agents import service
    from app.llm import NullLLM
    _, tok = ui_user("engineer")
    seen = []
    real = httpx.request

    def spy(method, url, **kw):
        seen.append((method, url, kw.get("headers") or {}, kw.get("json"), kw.get("params")))
        return real(method, url, **kw)

    with mock.patch("httpx.request", side_effect=spy):
        for view in ("catalog", "sources", "query", "analytics", "pipelines", "quality", "approvals", "monitoring"):
            run_view(view, tok)
        at = run_view("query", tok)
        at.text_area(key="ask_q").set_value("total orders by channel")
        at.button(key="ask_go").click().run()
    assert len(seen) > 20
    for method, url, headers, body, params in seen:
        assert url.startswith(live_api + "/"), url                                  # only ever the configured API host
        assert headers.get("Authorization") == f"Bearer {tok}" and re.fullmatch(r"[0-9a-f]{32}", headers.get("X-Request-ID", "")), url
        for payload in (body or {}, params or {}):
            assert not ({"role", "actor", "user", "username", "user_id", "is_admin"} & set(payload)), (url, payload)
    asks = [b for m, u, h, b, p in seen if u.endswith("/agents/ask")]
    assert asks == [{"question": "total orders by channel"}]                        # only the question: identity comes from the token


def test_login_sends_credentials_only_to_the_login_endpoint(live_api, users):
    name, pw = users("analyst")
    seen = []
    real = httpx.request
    with mock.patch("httpx.request", side_effect=lambda m, u, **kw: (seen.append((u, kw.get("json"))), real(m, u, **kw))[1]):
        at = home()
        at.text_input(key="login_user").set_value(name)
        at.text_input(key="login_pw").set_value(pw)
        at.button[0].click().run()
    with_pw = [u for u, body in seen if body and "password" in body]
    assert with_pw == [live_api + "/auth/login"]


@pytest.mark.parametrize("view", ["catalog", "sources", "query", "analytics", "pipelines", "quality", "approvals", "monitoring"])
def test_the_session_token_is_never_rendered_on_any_page(ui_user, view):
    _, tok = ui_user("engineer")
    at = run_view(view, tok)
    blob = texts(at) + str([d.value.to_dict() for d in at.dataframe]) + str([c.value for c in at.code])
    assert tok not in blob and tok.split(".")[1] not in blob


# ------------------------------------------------------------------------------------------ the UI has no way to the database
UI_SOURCES = sorted(p for p in UI.rglob("*.py"))


def test_ui_source_has_no_database_or_backend_access():
    assert len(UI_SOURCES) >= 11
    banned_imports = re.compile(r"^\s*(?:from|import)\s+(sqlalchemy|psycopg\w*|app\b|alembic|pyspark|langgraph|etl\b)", re.M)
    banned_strings = ("postgresql://", "postgresql+psycopg", "DATABASE_URL", "ETL_URL", "ANALYTICS_RO", "SRC_PG", "SRC_GOLD", "JWT_SECRET",
                      "app_rw", "etl_rw", "analytics_ro", "source_ro", "password_hash")
    for path in UI_SOURCES:
        src = path.read_text()
        assert not banned_imports.search(src), path
        for s in banned_strings:
            assert s not in src, (path.name, s)


def test_only_the_api_client_talks_http():
    for path in UI_SOURCES:
        if path.name == "api_client.py":
            continue
        assert not re.search(r"^\s*(import httpx|from httpx|import requests|import urllib)", path.read_text(), re.M), path


def test_ui_dependencies_and_container_have_no_database_drivers_or_credentials():
    pkgs = {re.split(r"[=<>\[ ]", line.strip())[0].lower() for line in (UI / "requirements.txt").read_text().splitlines() if line.strip() and not line.startswith("#")}
    assert pkgs <= {"streamlit", "httpx", "pandas"}, pkgs
    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text())["services"]["ui"]
    env = compose.get("environment") or {}
    env_keys = set(env) if isinstance(env, dict) else {e.split("=")[0] for e in env}
    assert env_keys == {"API_URL"} and "env_file" not in compose
    assert set(compose.get("depends_on", {})) <= {"api"}
    assert all(str(v).startswith("./ui:") for v in compose.get("volumes", []))     # nothing but its own code
    dockerfile = (REPO / "docker" / "ui.Dockerfile").read_text()
    assert "COPY ui/" in dockerfile and "backend" not in dockerfile and "etl" not in dockerfile
