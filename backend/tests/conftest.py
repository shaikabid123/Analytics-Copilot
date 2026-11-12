"""DB tests need a migrated Postgres (docker compose up -d postgres; alembic upgrade head).
If DATABASE_URL is missing they FAIL loudly, they are never silently skipped."""
import os

import pytest
from sqlalchemy import create_engine


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.fail(f"Environment variable {name} is required for these tests", pytrace=False)
    return value


@pytest.fixture(scope="session")
def app_engine():
    return create_engine(_require("DATABASE_URL"))


@pytest.fixture(scope="session")
def etl_engine():
    return create_engine(_require("ETL_URL"))


@pytest.fixture(scope="session")
def ro_engine():
    return create_engine(_require("ANALYTICS_RO_URL"))


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient
    from app.main import app
    return TestClient(app)


# ----------------------------------------------------------------------------- agent test helpers
import json
import uuid

from sqlalchemy import text as _text


@pytest.fixture()
def make_run(app_engine):
    """Insert a synthetic pipeline + run + DQ results; cleaned up afterwards (audit rows stay: insert-only)."""
    created = {"pipelines": [], "runs": []}

    def _make(status="failed", error=None, checks=(), retryable=None, params=None):
        name = f"agent_t_{uuid.uuid4().hex[:8]}"
        details = {"retryable": retryable} if retryable is not None else {}
        with app_engine.begin() as c:
            pid = c.execute(_text("INSERT INTO app.pipelines (name, params) VALUES (:n, CAST(:p AS jsonb)) RETURNING id"),
                            {"n": name, "p": json.dumps(params or {})}).scalar()
            rid = c.execute(_text("INSERT INTO app.pipeline_runs (pipeline_id, status, trigger, attempt, error_message, details, "
                                  "started_at, finished_at) VALUES (:p, :s, 'manual', 1, :e, CAST(:d AS jsonb), now(), now()) "
                                  "RETURNING id"), {"p": pid, "s": status, "e": error, "d": json.dumps(details)}).scalar()
            for ds, chk, typ, sev, st, obs, thr, det in checks:
                c.execute(_text("INSERT INTO app.dq_results (run_id, dataset, check_name, check_type, severity, status, observed, "
                                "threshold, detail) VALUES (:r,:d,:c,:t,:s,:st,:o,:th,CAST(:de AS jsonb))"),
                          {"r": rid, "d": ds, "c": chk, "t": typ, "s": sev, "st": st, "o": obs, "th": thr, "de": json.dumps(det)})
        created["pipelines"].append(pid)
        created["runs"].append(rid)
        return name, rid

    yield _make
    with app_engine.begin() as c:
        for rid in created["runs"]:
            c.execute(_text("DELETE FROM app.approvals WHERE pipeline_run_id = :r"), {"r": rid})
            c.execute(_text("DELETE FROM app.pipeline_runs WHERE id = :r"), {"r": rid})
        for pid in created["pipelines"]:
            c.execute(_text("DELETE FROM app.pipelines WHERE id = :p"), {"p": pid})


REJECT_FAIL = ("customers", "reject_rate", "validity", "error", "failed", 4.0, 2.0, {"reasons": {"missing_email": 8, "invalid_segment": 2}})
CONTRACT_FAIL = ("src.orders", "schema_contract", "contract", "error", "failed", 1.0, 0.0,
                 {"missing_columns": ["channel"], "type_mismatch": {}, "unexpected_columns": []})
DRIFT_FAIL = ("orders", "drift_total_amount", "drift", "warn", "failed", 0.6, 0.25, {"psi": 0.6})


# ----------------------------------------------------------------------------- auth / API helpers
import os as _os
import threading as _threading
import time as _time

from sqlalchemy.orm import Session as _Session

_os.environ.setdefault("JWT_SECRET", "test-only-secret-" + "x" * 32)
PASSWORD = "Correct-Horse-Battery-9"


@pytest.fixture()
def api(app_engine):
    from fastapi.testclient import TestClient
    from app.main import app
    return TestClient(app)


@pytest.fixture()
def users(app_engine):
    """Factory for throw-away users (cleaned up afterwards). Returns (username, password)."""
    from app.services.users import create_user
    created: list[str] = []

    def make(role="analyst", password=PASSWORD):
        name = f"t_{role}_{uuid.uuid4().hex[:8]}"
        with _Session(app_engine) as s:
            create_user(s, name, password, role, actor="pytest")
        created.append(name)
        return name, password

    yield make
    with app_engine.begin() as c:
        for n in created:
            c.execute(_text("DELETE FROM app.users WHERE username = :n"), {"n": n})


@pytest.fixture()
def login(api, users):
    """login(role) -> (username, headers). Creates a user and logs in through the real endpoint."""
    def go(role="analyst"):
        name, pw = users(role)
        r = api.post("/auth/login", json={"username": name, "password": pw})
        assert r.status_code == 200, r.text
        return name, {"Authorization": f"Bearer {r.json()['access_token']}"}
    return go


class MockServer:
    """The bundled mock integrations running on a real local port."""

    def __init__(self):
        import uvicorn
        from mock_services.app import create_app
        _os.environ.setdefault("MOCK_TICKET_TOKEN", "mock-token")
        self.token = _os.environ["MOCK_TICKET_TOKEN"]
        self.server = uvicorn.Server(uvicorn.Config(create_app(), host="127.0.0.1", port=0, log_level="error"))
        self.thread = _threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        for _ in range(100):
            if self.server.started:
                break
            _time.sleep(0.05)
        self.port = self.server.servers[0].sockets[0].getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"

    def control(self, service, mode="ok", fail_first=0, sleep_s=5.0):
        import httpx
        httpx.post(f"{self.base}/control", json={"service": service, "mode": mode, "fail_first": fail_first, "sleep_s": sleep_s}).raise_for_status()

    def state(self):
        import httpx
        return httpx.get(f"{self.base}/control/state").json()

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=5)


@pytest.fixture()
def mock_integrations(monkeypatch):
    from app.config import get_settings
    m = MockServer()
    monkeypatch.setenv("ALERT_WEBHOOK_URL", f"{m.base}/slack/webhook")
    monkeypatch.setenv("TICKET_API_URL", f"{m.base}/tickets/api")
    monkeypatch.setenv("TICKET_API_TOKEN", m.token)
    monkeypatch.setenv("INTEGRATION_TIMEOUT_S", "0.5")
    monkeypatch.setenv("INTEGRATION_RETRIES", "1")
    get_settings.cache_clear()
    yield m
    m.stop()
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _fresh_settings_after_each_test():
    """Tests may monkeypatch the environment; never let a cached Settings object leak into the next test."""
    yield
    from app.config import get_settings
    get_settings.cache_clear()
