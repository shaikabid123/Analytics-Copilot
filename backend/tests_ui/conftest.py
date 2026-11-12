"""UI workflow tests: real Streamlit pages (AppTest) -> real HTTP -> live API server (in a thread) -> real database.
Needs streamlit: pip install -r requirements-uitest.txt (a missing package fails loudly)."""
import os
import sys
import threading
import time
import uuid
from pathlib import Path

import httpx
import pytest
import uvicorn
from sqlalchemy import text
from streamlit.testing.v1 import AppTest

REPO = Path(os.environ.get("REPO_ROOT") or Path(__file__).resolve().parents[2])
UI = REPO / "ui"
if not UI.is_dir():
    pytest.fail(f"UI directory not found at {UI} (set REPO_ROOT)", pytrace=False)
sys.path.insert(0, str(UI))

# shared fixtures and constants from the backend test suite
from tests.conftest import (  # noqa: E402,F401
    CONTRACT_FAIL, DRIFT_FAIL, PASSWORD, REJECT_FAIL, _fresh_settings_after_each_test, app_engine, etl_engine, make_run,
    mock_integrations, ro_engine, users,
)


@pytest.fixture(scope="session")
def live_api():
    from app.main import app
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=0, log_level="critical"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    base = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    os.environ["API_URL"] = base
    yield base
    server.should_exit = True


@pytest.fixture()
def ui_user(live_api, users):
    """ui_user(role) -> (username, token) using the real login endpoint."""
    def make(role="analyst"):
        name, pw = users(role)
        r = httpx.post(f"{live_api}/auth/login", json={"username": name, "password": pw})
        assert r.status_code == 200, r.text
        return name, r.json()["access_token"]
    return make


def run_view(name: str, token: str | None = None, **state) -> AppTest:
    at = AppTest.from_file(str(UI / "views" / f"{name}.py"), default_timeout=60)
    if token:
        at.session_state["token"] = token
    for k, v in state.items():
        at.session_state[k] = v
    return at.run()


def home(token: str | None = None) -> AppTest:
    at = AppTest.from_file(str(UI / "Home.py"), default_timeout=60)
    if token:
        at.session_state["token"] = token
    return at.run()


def texts(at: AppTest) -> str:
    """Every visible message on the page, for substring assertions."""
    parts = []
    for coll in (at.markdown, at.error, at.warning, at.info, at.success, at.caption, at.title, at.subheader, at.code):
        parts += [str(e.value) for e in coll]
    return "\n".join(parts)


def has_token(at: AppTest) -> bool:
    try:
        return bool(at.session_state["token"])
    except KeyError:
        return False


def select_by_label(at: AppTest, key: str, fragment: str):
    sb = at.selectbox(key=key)
    idx = next(i for i, o in enumerate(sb.options) if fragment in str(o))
    sb.select_index(idx)
    return at


@pytest.fixture()
def pipe(make_run, monkeypatch, app_engine):
    """A synthetic pipeline (patched into the supported list) with one FAILED run; real pipelines are never touched."""
    from app.routers import pipelines as router_mod
    name, failed_run = make_run(checks=[REJECT_FAIL, DRIFT_FAIL], error="DQGateError: gate failed")
    monkeypatch.setattr(router_mod, "SUPPORTED", (*router_mod.SUPPORTED, name))

    class P: pass
    p = P(); p.name, p.failed_run = name, failed_run
    yield p
    with app_engine.begin() as c:
        c.execute(text("DELETE FROM app.dq_alerts WHERE run_id IN (SELECT id FROM app.pipeline_runs WHERE pipeline_id=(SELECT id FROM app.pipelines WHERE name=:n))"), {"n": name})
        c.execute(text("DELETE FROM app.pipeline_runs WHERE pipeline_id = (SELECT id FROM app.pipelines WHERE name=:n)"), {"n": name})
