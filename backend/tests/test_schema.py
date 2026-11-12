import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

EXPECTED_TABLES = {
    "roles", "users", "data_sources", "catalog_datasets", "catalog_columns",
    "glossary_terms", "glossary_term_columns", "pipelines", "pipeline_runs",
    "agent_runs", "approvals", "audit_events", "alembic_version",
}


def test_all_expected_tables_exist(app_engine):
    with app_engine.connect() as c:
        rows = c.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='app'")).all()
    assert EXPECTED_TABLES <= {r[0] for r in rows}


def test_three_roles_seeded(app_engine):
    with app_engine.connect() as c:
        names = {r[0] for r in c.execute(text("SELECT name FROM app.roles"))}
    assert names == {"analyst", "engineer", "admin"}


def test_audit_insert_works_but_update_delete_truncate_are_blocked(app_engine):
    with app_engine.begin() as c:
        eid = c.execute(text(
            "INSERT INTO app.audit_events (actor_label, action) "
            "VALUES ('pytest', 'test.audit') RETURNING id")).scalar_one()
    for stmt in (
        f"UPDATE app.audit_events SET action='tampered' WHERE id={eid}",
        f"DELETE FROM app.audit_events WHERE id={eid}",
        "TRUNCATE app.audit_events",
    ):
        with pytest.raises(DBAPIError, match="insert-only"):
            with app_engine.begin() as c:
                c.execute(text(stmt))
    with app_engine.connect() as c:
        action = c.execute(text("SELECT action FROM app.audit_events WHERE id=:i"), {"i": eid}).scalar_one()
    assert action == "test.audit"  # row survived all three attempts


def test_invalid_approval_status_rejected(app_engine):
    with pytest.raises(IntegrityError):
        with app_engine.begin() as c:
            c.execute(text(
                "INSERT INTO app.approvals (request_type, status, summary) "
                "VALUES ('etl_fix', 'maybe', 'x')"))


def test_decided_approval_must_record_decider(app_engine):
    with pytest.raises(IntegrityError, match="ck_approvals_decision_recorded"):
        with app_engine.begin() as c:
            c.execute(text(
                "INSERT INTO app.approvals (request_type, status, summary) "
                "VALUES ('etl_fix', 'approved', 'no decider recorded')"))


def test_invalid_pipeline_run_status_rejected(app_engine):
    with pytest.raises(IntegrityError):
        with app_engine.begin() as c:
            c.execute(text("INSERT INTO app.pipelines (name) VALUES ('p_status_test') "
                           "ON CONFLICT DO NOTHING"))
            pid = c.execute(text("SELECT id FROM app.pipelines WHERE name='p_status_test'")).scalar_one()
            c.execute(text("INSERT INTO app.pipeline_runs (pipeline_id, status) VALUES (:p, 'weird')"),
                      {"p": pid})
    with app_engine.begin() as c:  # clean up the helper pipeline row
        c.execute(text("DELETE FROM app.pipelines WHERE name='p_status_test'"))


def test_duplicate_username_rejected(app_engine):
    with app_engine.begin() as c:
        rid = c.execute(text("SELECT id FROM app.roles WHERE name='analyst'")).scalar_one()
        c.execute(text("INSERT INTO app.users (username, password_hash, role_id) "
                       "VALUES ('dup_test', 'x', :r)"), {"r": rid})
    try:
        with pytest.raises(IntegrityError):
            with app_engine.begin() as c:
                c.execute(text("INSERT INTO app.users (username, password_hash, role_id) "
                               "VALUES ('dup_test', 'x', :r)"), {"r": rid})
    finally:
        with app_engine.begin() as c:
            c.execute(text("DELETE FROM app.users WHERE username='dup_test'"))
