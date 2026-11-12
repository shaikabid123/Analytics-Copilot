from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import get_session
from app.main import app


def test_liveness_does_not_need_database(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_readiness_ok_with_migrated_database(client):
    r = client.get("/health/ready")
    assert r.status_code == 200
    body = r.json()
    assert body["database"] == "up"
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    cfg = Config(str(__import__("pathlib").Path(__file__).resolve().parents[1] / "alembic.ini"))
    cfg.set_main_option("script_location", str(__import__("pathlib").Path(__file__).resolve().parents[1] / "alembic"))
    head = ScriptDirectory.from_config(cfg).get_current_head()
    assert body["schema_revision"] == head


def test_readiness_returns_503_when_database_is_down(client):
    # Point the dependency at a port nothing listens on.
    bad = create_engine("postgresql+psycopg://x:y@127.0.0.1:1/none", connect_args={"connect_timeout": 1})

    def broken_session():
        with sessionmaker(bind=bad)() as s:
            yield s

    app.dependency_overrides[get_session] = broken_session
    try:
        r = client.get("/health/ready")
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 503
    assert r.json()["database"] == "down"
    assert "password" not in r.text.lower()  # no connection details leaked


def test_openapi_docs_available(client):
    assert client.get("/openapi.json").status_code == 200


