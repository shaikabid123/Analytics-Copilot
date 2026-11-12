"""Persisted approval queue over HTTP: who can see it, who can decide, single-use decisions, separation of duties."""
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text

from app.agents import service
from app.config import get_settings
from app.llm import NullLLM
from tests.conftest import REJECT_FAIL


@pytest.fixture()
def pending(make_run, login, app_engine):
    """A pending approval requested by an engineer. Returns a namespace with ids and the requester's headers."""
    class P: pass

    def create():
        p = P()
        p.requester, p.req_headers = login("engineer")
        p.pipeline, p.run_id = make_run(checks=[REJECT_FAIL], error="DQGateError: gate failed")
        out = service.run_request(f"why did run {p.run_id} fail", p.requester, "engineer", llm=NullLLM())
        assert out["status"] == "awaiting_approval"
        p.approval_id, p.agent_run = out["approval_id"], out["run_id"]
        return p
    return create


def params(app_engine, name):
    with app_engine.connect() as c:
        return c.execute(text("SELECT params FROM app.pipelines WHERE name=:n"), {"n": name}).scalar()


def test_queue_is_visible_only_to_engineers_and_admins_and_shows_the_proposal(api, login, pending):
    p = pending()
    _, analyst = login("analyst")
    assert api.get("/approvals", headers=analyst).status_code == 403
    assert api.get(f"/approvals/{p.approval_id}", headers=analyst).status_code == 403
    _, reviewer = login("engineer")
    listing = api.get("/approvals", headers=reviewer).json()
    item = next(a for a in listing if a["id"] == p.approval_id)
    assert item["status"] == "pending" and item["requested_by"] == p.requester and item["pipeline_run_id"] == p.run_id
    assert item["proposal"]["action"] == "set_max_reject_pct" and item["proposal"]["params"] == {"value": 5.0}
    assert item["proposal"]["pipeline"] == p.pipeline and item["decided_by"] is None and item["applied_at"] is None
    assert api.get(f"/approvals/{p.approval_id}", headers=reviewer).json()["id"] == p.approval_id
    assert api.get("/approvals?status=bogus", headers=reviewer).status_code == 422
    assert api.get("/approvals/987654321", headers=reviewer).status_code == 404


def test_analyst_cannot_decide_and_nothing_changes(api, login, pending, app_engine):
    p = pending()
    _, analyst = login("analyst")
    r = api.post(f"/approvals/{p.approval_id}/decision", json={"decision": "approved"}, headers=analyst)
    assert r.status_code == 403
    assert api.get(f"/approvals/{p.approval_id}", headers=p.req_headers).json()["status"] == "pending"
    assert params(app_engine, p.pipeline) == {}


def test_requester_cannot_approve_their_own_request(api, pending, app_engine):
    p = pending()
    r = api.post(f"/approvals/{p.approval_id}/decision", json={"decision": "approved"}, headers=p.req_headers)
    assert r.status_code == 403 and "requested" in r.json()["detail"]
    assert params(app_engine, p.pipeline) == {}
    assert api.get(f"/approvals/{p.approval_id}", headers=p.req_headers).json()["status"] == "pending"
    with app_engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM app.audit_events WHERE action='approval.self_decision_denied' AND actor_label=:a"),
                         {"a": p.requester}).scalar() == 1


def test_second_engineer_approves_and_the_change_is_applied_attributed_and_audited(api, login, pending, app_engine):
    p = pending()
    reviewer, rh = login("engineer")
    r = api.post(f"/approvals/{p.approval_id}/decision", json={"decision": "approved", "note": "looks right"}, headers=rh)
    assert r.status_code == 200 and r.json()["run_status"] == "succeeded" and "was approved and the change was applied" in r.json()["answer"]
    assert params(app_engine, p.pipeline) == {"max_reject_pct": 5.0}
    item = api.get(f"/approvals/{p.approval_id}", headers=rh).json()
    assert item["status"] == "approved" and item["decided_by"] == reviewer and item["note"] == "looks right" and item["applied_at"]
    with app_engine.connect() as c:
        applied = c.execute(text("SELECT actor_label FROM app.audit_events WHERE action='pipeline.change.applied' AND detail->>'approval_id'=:i"),
                            {"i": str(p.approval_id)}).scalar()
        decided = c.execute(text("SELECT actor_label, detail->>'role' FROM app.audit_events WHERE action='approval.decided' AND resource_id=:i"),
                            {"i": str(p.approval_id)}).one()
    assert applied == reviewer and tuple(decided) == (reviewer, "engineer")


def test_decisions_are_single_use_and_inputs_are_validated(api, login, pending, app_engine):
    p = pending()
    _, rh = login("admin")
    url = f"/approvals/{p.approval_id}/decision"
    assert api.post(url, json={"decision": "maybe"}, headers=rh).status_code == 422
    assert api.post(url, json={"decision": "approved", "extra": 1}, headers=rh).status_code == 422
    assert api.post(url, json={"decision": "approved", "note": "x" * 501}, headers=rh).status_code == 422
    assert api.post(url, json={"decision": "rejected"}, headers=rh).status_code == 200
    assert api.post(url, json={"decision": "approved"}, headers=rh).status_code == 409          # cannot flip a decision
    assert api.post("/approvals/987654321/decision", json={"decision": "approved"}, headers=rh).status_code == 404
    assert params(app_engine, p.pipeline) == {}                                                   # rejection changed nothing


def test_rejected_and_approved_history_stays_queryable(api, login, pending):
    p = pending()
    _, rh = login("engineer")
    api.post(f"/approvals/{p.approval_id}/decision", json={"decision": "rejected", "note": "not now"}, headers=rh)
    assert all(a["id"] != p.approval_id for a in api.get("/approvals", headers=rh).json())        # no longer pending
    rejected = api.get("/approvals?status=rejected", headers=rh).json()
    assert any(a["id"] == p.approval_id and a["note"] == "not now" for a in rejected)
    assert any(a["id"] == p.approval_id for a in api.get("/approvals?status=all&limit=200", headers=rh).json())


def test_self_approval_can_be_enabled_explicitly_for_single_operator_setups(api, pending, monkeypatch, app_engine):
    p = pending()
    monkeypatch.setenv("ALLOW_SELF_APPROVAL", "true")
    get_settings.cache_clear()
    r = api.post(f"/approvals/{p.approval_id}/decision", json={"decision": "approved"}, headers=p.req_headers)
    assert r.status_code == 200 and params(app_engine, p.pipeline) == {"max_reject_pct": 5.0}


def test_tampered_proposal_is_refused_even_after_a_valid_approval(api, login, pending, app_engine):
    p = pending()
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.approvals SET proposed_change = jsonb_set(proposed_change, '{proposal,params,value}', '9.5') WHERE id=:i"),
                  {"i": p.approval_id})
    _, rh = login("engineer")
    r = api.post(f"/approvals/{p.approval_id}/decision", json={"decision": "approved"}, headers=rh)
    assert r.status_code == 200 and "Change not applied" in r.json()["answer"] and "modified" in r.json()["answer"]
    assert params(app_engine, p.pipeline) == {}


def test_concurrent_decisions_have_exactly_one_winner(login, pending, app_engine):
    from fastapi.testclient import TestClient
    from app.main import app
    p = pending()
    _, h1 = login("engineer")
    _, h2 = login("admin")

    def decide(headers, decision):
        return TestClient(app).post(f"/approvals/{p.approval_id}/decision", json={"decision": decision}, headers=headers).status_code

    with ThreadPoolExecutor(2) as ex:
        a, b = ex.submit(decide, h1, "approved"), ex.submit(decide, h2, "rejected")
        codes = sorted([a.result(), b.result()])
    assert codes == [200, 409]
    with app_engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM app.audit_events WHERE action='approval.decided' AND resource_id=:i"),
                         {"i": str(p.approval_id)}).scalar() == 1
