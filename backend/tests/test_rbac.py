"""RBAC enforced by the backend. The route table below is checked against the live OpenAPI document, so a new
route cannot be added without being classified here, and every classified route is exercised for every role."""
import pytest
from sqlalchemy import text

PUBLIC = {("GET", "/health"), ("GET", "/health/ready"), ("POST", "/auth/login")}
ANY, PRIV, ADMIN = {"analyst", "engineer", "admin"}, {"engineer", "admin"}, {"admin"}
UUID0 = "00000000-0000-0000-0000-000000000000"
NO_APPROVAL = 987654321

# (METHOD, openapi template) -> (allowed roles, concrete path, json body)
ROUTES = {
    ("GET", "/auth/me"): (ANY, "/auth/me", None),
    ("GET", "/sources"): (ANY, "/sources", None),
    ("GET", "/catalog/datasets"): (ANY, "/catalog/datasets", None),
    ("GET", "/catalog/datasets/{dataset_id}"): (ANY, "/catalog/datasets/1", None),
    ("GET", "/catalog/search"): (ANY, "/catalog/search?q=order", None),
    ("GET", "/glossary"): (ANY, "/glossary", None),
    ("GET", "/glossary/{term_id}"): (ANY, "/glossary/1", None),
    ("POST", "/query/validate"): (ANY, "/query/validate", {"sql": "SELECT channel FROM gold.daily_sales"}),
    ("POST", "/query/execute"): (ANY, "/query/execute", {"sql": "SELECT channel FROM gold.daily_sales LIMIT 1"}),
    ("POST", "/agents/ask"): (ANY, "/agents/ask", {"question": "hello there"}),
    ("GET", "/agents/runs/{run_id}"): (ANY, f"/agents/runs/{UUID0}", None),
    ("GET", "/approvals"): (PRIV, "/approvals", None),
    ("GET", "/approvals/{approval_id}"): (PRIV, f"/approvals/{NO_APPROVAL}", None),
    ("POST", "/approvals/{approval_id}/decision"): (PRIV, f"/approvals/{NO_APPROVAL}/decision", {"decision": "approved"}),
    ("GET", "/pipelines"): (PRIV, "/pipelines", None),
    ("POST", "/pipelines/{name}/runs"): (PRIV, "/pipelines/no_such_pipeline/runs", None),
    ("POST", "/pipelines/runs/{run_id}/retry"): (PRIV, "/pipelines/runs/987654321/retry", None),
    ("POST", "/pipelines/runs/{run_id}/cancel"): (PRIV, "/pipelines/runs/987654321/cancel", None),
    ("POST", "/pipelines/runs/{run_id}/abandon"): (ADMIN, "/pipelines/runs/987654321/abandon", None),
    ("DELETE", "/pipelines/{name}/overrides/{param}"): (ADMIN, "/pipelines/no_such_pipeline/overrides/max_reject_pct", None),
    ("GET", "/pipelines/quality/summary"): (PRIV, "/pipelines/quality/summary?pipeline=no_such_pipeline", None),
    ("GET", "/metrics/summary"): (PRIV, "/metrics/summary", None),
    ("GET", "/pipelines/runs"): (PRIV, "/pipelines/runs", None),
    ("GET", "/pipelines/runs/{run_id}/checks"): (PRIV, "/pipelines/runs/987654321/checks", None),
    ("POST", "/integrations/dispatch"): (PRIV, "/integrations/dispatch", None),
    ("GET", "/integrations/alerts"): (PRIV, "/integrations/alerts", None),
    ("GET", "/integrations/calls"): (PRIV, "/integrations/calls", None),
    ("GET", "/admin/users"): (ADMIN, "/admin/users", None),
    ("POST", "/admin/users"): (ADMIN, "/admin/users", {"username": "x", "password": "y", "role": "z"}),
    ("PATCH", "/admin/users/{username}"): (ADMIN, "/admin/users/no_such_user_zz", {"is_active": True}),
    ("GET", "/audit/events"): (ADMIN, "/audit/events", None),
    ("GET", "/audit/queries"): (ADMIN, "/audit/queries", None),
}


def live_operations(api):
    ops = set()
    for path, item in api.get("/openapi.json").json()["paths"].items():
        for method in item:
            if method in ("get", "post", "put", "patch", "delete"):
                ops.add((method.upper(), path))
    return ops


def test_route_table_covers_every_operation_in_the_live_api(api):
    live = live_operations(api)
    assert live == set(ROUTES) | PUBLIC, {"unclassified": sorted(live - set(ROUTES) - PUBLIC), "stale": sorted((set(ROUTES) | PUBLIC) - live)}


@pytest.mark.parametrize("key", sorted(ROUTES))
def test_unauthenticated_and_garbage_credentials_get_401(api, key):
    _, path, body = ROUTES[key]
    for hdrs in ({}, {"Authorization": "Bearer garbage"}, {"Authorization": "Bearer "}, {"Authorization": "Token abc"}):
        r = api.request(key[0], path, json=body, headers=hdrs)
        assert r.status_code == 401, (key, hdrs, r.status_code)
        assert r.headers.get("www-authenticate") == "Bearer"


@pytest.mark.parametrize("key", sorted(ROUTES))
def test_every_role_gets_exactly_the_access_it_should(api, login, key):
    allowed, path, body = ROUTES[key]
    for role in ("analyst", "engineer", "admin"):
        _, hdrs = login(role)
        r = api.request(key[0], path, json=body, headers=hdrs)
        if role in allowed:
            assert r.status_code not in (401, 403), (key, role, r.status_code, r.text[:120])
        else:
            assert r.status_code == 403, (key, role, r.status_code)


def test_public_routes_stay_public_and_leak_nothing(api):
    assert api.get("/health").json()["status"] == "ok"
    ready = api.get("/health/ready")
    assert "password" not in ready.text.lower() and "postgresql" not in ready.text.lower()


def test_forbidden_attempts_are_audited_with_actor_and_endpoint(api, login, app_engine):
    name, hdrs = login("analyst")
    assert api.get("/audit/events", headers=hdrs).status_code == 403
    assert api.post("/approvals/1/decision", json={"decision": "approved"}, headers=hdrs).status_code == 403
    with app_engine.connect() as c:
        rows = c.execute(text("SELECT resource_id, outcome, detail FROM app.audit_events WHERE actor_label=:a AND action='authz.denied' ORDER BY id"),
                         {"a": name}).all()
    assert [r[0] for r in rows] == ["GET /audit/events", "POST /approvals/1/decision"]
    assert all(r[1] == "denied" and r[2]["role"] == "analyst" for r in rows)


def test_role_cannot_be_supplied_by_the_client(api, login):
    _, hdrs = login("analyst")
    r = api.post("/agents/ask", json={"question": "hello", "role": "admin"}, headers=hdrs)
    assert r.status_code == 422                                           # unknown fields are refused, not ignored
    assert api.get("/admin/users?role=admin", headers={**hdrs, "X-Role": "admin", "X-User": "root"}).status_code == 403
