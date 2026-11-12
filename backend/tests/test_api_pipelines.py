"""Pipeline execution + recovery controls through the API. Uses SYNTHETIC pipelines (patched into the supported list)
so real sales/events runs are never touched."""
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text

from app.routers import pipelines as router_mod
from tests.conftest import DRIFT_FAIL, REJECT_FAIL


@pytest.fixture()
def pipe(make_run, monkeypatch, app_engine):
    """A synthetic supported pipeline that already has one FAILED run (so retry has something to retry)."""
    name, failed_run = make_run(checks=[REJECT_FAIL, DRIFT_FAIL], error="DQGateError: gate failed")
    monkeypatch.setattr(router_mod, "SUPPORTED", (*router_mod.SUPPORTED, name))
    class P: pass
    p = P(); p.name, p.failed_run = name, failed_run
    yield p
    with app_engine.begin() as c:                                   # runs created through the API
        c.execute(text("DELETE FROM app.pipeline_runs WHERE pipeline_id = (SELECT id FROM app.pipelines WHERE name=:n)"), {"n": name})


def run_row(app_engine, rid):
    with app_engine.connect() as c:
        return c.execute(text("SELECT status, trigger, retry_of_run_id, triggered_by, error_message, finished_at IS NOT NULL FROM app.pipeline_runs WHERE id=:i"), {"i": rid}).one()


def audit_count(app_engine, action, actor):
    with app_engine.connect() as c:
        return c.execute(text("SELECT count(*) FROM app.audit_events WHERE action=:a AND actor_label=:u"), {"a": action, "u": actor}).scalar()


def test_overview_shows_sla_freshness_and_is_privileged(api, login, pipe):
    _, analyst = login("analyst")
    assert api.get("/pipelines", headers=analyst).status_code == 403
    _, eng = login("engineer")
    rows = api.get("/pipelines", headers=eng).json()
    names = {r["name"] for r in rows}
    assert {"sales", "events", pipe.name} <= names
    mine = next(r for r in rows if r["name"] == pipe.name)
    assert mine["last_run"]["status"] == "failed" and mine["last_success_at"] is None and mine["sla_breached"] is True
    assert mine["active_run"] is None and mine["sla_hours"] == 26.0 and mine["max_retries"] == 3


def test_start_enqueues_one_run_and_blocks_duplicates(api, login, pipe, app_engine):
    name, eng = login("engineer")
    r = api.post(f"/pipelines/{pipe.name}/runs", headers=eng)
    assert r.status_code == 202 and r.json()["status"] == "queued"
    rid = r.json()["run_id"]
    status, trigger, retry_of, by, err, finished = run_row(app_engine, rid)
    assert (status, trigger, retry_of, err, finished) == ("queued", "manual", None, None, False) and by is not None
    dup = api.post(f"/pipelines/{pipe.name}/runs", headers=eng)
    assert dup.status_code == 409 and f"run {rid} is already queued" in dup.json()["detail"]
    assert api.get("/pipelines", headers=eng).json()[0]["name"] and \
        next(p for p in api.get("/pipelines", headers=eng).json() if p["name"] == pipe.name)["active_run"] == {"id": rid, "status": "queued"}
    assert audit_count(app_engine, "pipeline.run.requested", name) == 1


def test_unknown_or_unsupported_pipelines_cannot_be_started(api, login):
    _, eng = login("engineer")
    for bad in ("no_such_pipeline", "app_users", "x"):
        assert api.post(f"/pipelines/{bad}/runs", headers=eng).status_code == 404
    assert api.post("/pipelines/Sales;DROP/runs", headers=eng).status_code in (404, 422)


def test_concurrent_start_requests_have_exactly_one_winner(login, pipe):
    from fastapi.testclient import TestClient
    from app.main import app
    _, h1 = login("engineer")
    _, h2 = login("admin")
    go = lambda h: TestClient(app).post(f"/pipelines/{pipe.name}/runs", headers=h).status_code   # noqa: E731
    with ThreadPoolExecutor(2) as ex:
        codes = sorted([ex.submit(go, h1).result(), ex.submit(go, h2).result()])
    assert codes == [202, 409]                                      # enforced by the database, not by luck


def test_retry_creates_a_linked_queued_run_only_for_failed_runs(api, login, pipe, app_engine):
    _, eng = login("engineer")
    r = api.post(f"/pipelines/runs/{pipe.failed_run}/retry", headers=eng)
    assert r.status_code == 202 and r.json()["retry_of"] == pipe.failed_run
    status, trigger, retry_of, *_ = run_row(app_engine, r.json()["run_id"])
    assert (status, trigger, retry_of) == ("queued", "retry", pipe.failed_run)
    assert api.post(f"/pipelines/runs/{pipe.failed_run}/retry", headers=eng).status_code == 409        # a run is already queued
    assert api.post(f"/pipelines/runs/{r.json()['run_id']}/retry", headers=eng).status_code == 409      # queued runs cannot be retried
    assert api.post("/pipelines/runs/987654321/retry", headers=eng).status_code == 404


def test_cancel_only_applies_to_queued_runs_and_frees_the_slot(api, login, pipe, app_engine):
    name, eng = login("engineer")
    rid = api.post(f"/pipelines/{pipe.name}/runs", headers=eng).json()["run_id"]
    assert api.post(f"/pipelines/runs/{rid}/cancel", headers=eng).status_code == 200
    status, _, _, _, err, finished = run_row(app_engine, rid)
    assert status == "failed" and finished and f"cancelled by {name}" in err
    assert api.post(f"/pipelines/runs/{rid}/cancel", headers=eng).status_code == 409                    # already terminal
    assert api.post(f"/pipelines/runs/{pipe.failed_run}/cancel", headers=eng).status_code == 409
    assert api.post(f"/pipelines/{pipe.name}/runs", headers=eng).status_code == 202                      # slot is free again
    assert audit_count(app_engine, "pipeline.run.cancelled", name) == 1


def test_abandon_is_admin_only_and_only_for_running_runs(api, login, pipe, app_engine):
    _, eng = login("engineer")
    admin, adm = login("admin")
    rid = api.post(f"/pipelines/{pipe.name}/runs", headers=eng).json()["run_id"]
    assert api.post(f"/pipelines/runs/{rid}/abandon", headers=adm).status_code == 409                    # still queued, not running
    with app_engine.begin() as c:                                                                         # a worker claims it, then dies
        c.execute(text("UPDATE app.pipeline_runs SET status='running', started_at=now() WHERE id=:i"), {"i": rid})
    assert api.post(f"/pipelines/{pipe.name}/runs", headers=eng).status_code == 409                       # blocked by the stuck run
    assert api.post(f"/pipelines/runs/{rid}/abandon", headers=eng).status_code == 403
    assert run_row(app_engine, rid)[0] == "running"
    assert api.post(f"/pipelines/runs/{rid}/abandon", headers=adm).status_code == 200
    assert "abandoned by admin" in run_row(app_engine, rid)[4]
    assert api.post(f"/pipelines/{pipe.name}/runs", headers=eng).status_code == 202                       # recovered
    assert audit_count(app_engine, "pipeline.run.abandoned", admin) == 1


def test_overrides_can_be_reverted_by_admins_only(api, login, pipe, app_engine):
    _, eng = login("engineer")
    admin, adm = login("admin")
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.pipelines SET params = CAST('{\"max_reject_pct\": 6.0, \"reset_baseline_next_run\": true}' AS jsonb) WHERE name=:n"), {"n": pipe.name})
    url = f"/pipelines/{pipe.name}/overrides/max_reject_pct"
    assert api.delete(url, headers=eng).status_code == 403
    assert next(p for p in api.get("/pipelines", headers=eng).json() if p["name"] == pipe.name)["overrides"]["max_reject_pct"] == 6.0
    r = api.delete(url, headers=adm)
    assert r.status_code == 200 and r.json()["overrides"] == {"reset_baseline_next_run": True}
    assert api.delete(url, headers=adm).status_code == 404
    assert api.delete(f"/pipelines/{pipe.name}/overrides/max_retries", headers=adm).status_code == 422  # only known overrides
    assert audit_count(app_engine, "pipeline.override.reverted", admin) == 1


def test_quality_summary_gives_reject_rates_and_drift_per_run(api, login, pipe):
    _, eng = login("engineer")
    s = api.get(f"/pipelines/quality/summary?pipeline={pipe.name}", headers=eng).json()
    assert s["pipeline"] == pipe.name and len(s["runs"]) == 1
    run = s["runs"][0]
    assert run["reject_rate"] == {"customers": 4.0} and run["drift_psi"] == {"orders.total_amount": 0.6}
    assert run["failed"] == 2 and run["status"] == "failed"
    assert api.get("/pipelines/quality/summary?pipeline=no_such", headers=eng).status_code == 404
    assert api.get("/pipelines/quality/summary", headers=eng).status_code == 422


def test_run_listing_filters_and_checks(api, login, pipe):
    _, eng = login("engineer")
    rows = api.get(f"/pipelines/runs?pipeline={pipe.name}&status=failed", headers=eng).json()
    assert [r["id"] for r in rows] == [pipe.failed_run] and rows[0]["error"].startswith("DQGateError")
    assert api.get("/pipelines/runs?status=bogus", headers=eng).status_code == 422
    assert api.get("/pipelines/runs?pipeline=BAD NAME", headers=eng).status_code == 422
    checks = api.get(f"/pipelines/runs/{pipe.failed_run}/checks", headers=eng).json()
    assert {c["check"] for c in checks} == {"reject_rate", "drift_total_amount"}
