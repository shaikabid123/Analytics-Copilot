"""Authentication: password hashing, JWT handling (incl. classic attacks), lockout, immediate revocation."""
import base64
import json
from datetime import datetime, timedelta, timezone

import jwt
import pytest
from sqlalchemy import text

from app.config import get_settings
from app.security import ALGORITHM, ISSUER, create_token, decode_token, hash_password, verify_password
from tests.conftest import PASSWORD


def secret():
    return get_settings().jwt_secret


def headers(token):
    return {"Authorization": f"Bearer {token}"}


def audit_reasons(app_engine, username):
    with app_engine.connect() as c:
        return c.execute(text("SELECT detail->>'reason' FROM app.audit_events WHERE actor_label=:u AND action='auth.login' "
                              "AND outcome='failure' ORDER BY id"), {"u": username}).scalars().all()


# ---------------------------------------------------------------- hashing
def test_password_hashing_uses_unique_salts_and_verifies():
    a, b = hash_password(PASSWORD), hash_password(PASSWORD)
    assert a != b and a.startswith("scrypt$") and PASSWORD not in a
    assert verify_password(PASSWORD, a) and verify_password(PASSWORD, b)
    assert not verify_password(PASSWORD + "x", a) and not verify_password("", a)


@pytest.mark.parametrize("stored", ["", "garbage", "scrypt$1$2$3", "bcrypt$x$y$z$a$b", "scrypt$16384$8$1$!!!$???", None])
def test_malformed_stored_hashes_fail_closed_without_raising(stored):
    assert verify_password(PASSWORD, stored) is False


# ---------------------------------------------------------------- login
def test_login_success_returns_a_short_lived_signed_token(api, users):
    name, pw = users("engineer")
    r = api.post("/auth/login", json={"username": name, "password": pw})
    assert r.status_code == 200 and r.json()["token_type"] == "bearer" and r.json()["expires_in"] == 30 * 60
    claims = decode_token(r.json()["access_token"])
    assert claims["sub"] == name and claims["iss"] == ISSUER and claims["role"] == "engineer"
    assert 0 < claims["exp"] - claims["iat"] <= 1800
    assert api.get("/auth/me", headers=headers(r.json()["access_token"])).json() == {"username": name, "role": "engineer"}


def test_wrong_password_and_unknown_user_are_indistinguishable(api, users):
    name, _ = users()
    bad = api.post("/auth/login", json={"username": name, "password": "wrong-password-123"})
    ghost = api.post("/auth/login", json={"username": "nobody_here", "password": "wrong-password-123"})
    assert bad.status_code == ghost.status_code == 401 and bad.json() == ghost.json() == {"detail": "invalid credentials"}
    assert bad.headers["www-authenticate"] == "Bearer"


def test_login_requires_well_formed_input(api):
    assert api.post("/auth/login", json={"username": "a"}).status_code == 422
    assert api.post("/auth/login", json={"username": "a", "password": "b", "role": "admin"}).status_code == 422   # extra field refused
    assert api.post("/auth/login", json={"username": "x" * 65, "password": "b"}).status_code == 422


def test_lockout_after_repeated_failures_blocks_even_the_correct_password(api, users, app_engine):
    name, pw = users()
    for _ in range(5):
        assert api.post("/auth/login", json={"username": name, "password": "wrong-password-123"}).status_code == 401
    assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 401          # locked
    assert audit_reasons(app_engine, name)[-2:] == ["locked_now", "locked"] or "locked" in audit_reasons(app_engine, name)
    with app_engine.begin() as c:                                                                         # time passes
        c.execute(text("UPDATE app.users SET locked_until = now() - interval '1 minute' WHERE username=:n"), {"n": name})
    assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 200


def test_successful_login_resets_the_failure_counter(api, users, app_engine):
    name, pw = users()
    for _ in range(3):
        api.post("/auth/login", json={"username": name, "password": "wrong-password-123"})
    assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 200
    with app_engine.connect() as c:
        assert c.execute(text("SELECT failed_attempts FROM app.users WHERE username=:n"), {"n": name}).scalar() == 0
    for _ in range(3):                                                                                   # 3 more: still below 5
        api.post("/auth/login", json={"username": name, "password": "wrong-password-123"})
    assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 200


def test_login_attempts_are_audited_without_secrets(api, users, app_engine):
    name, pw = users()
    api.post("/auth/login", json={"username": name, "password": "wrong-password-123"})
    api.post("/auth/login", json={"username": name, "password": pw})
    with app_engine.connect() as c:
        rows = c.execute(text("SELECT outcome, detail::text FROM app.audit_events WHERE actor_label=:u AND action='auth.login' ORDER BY id"),
                         {"u": name}).all()
    assert [r[0] for r in rows] == ["failure", "success"]
    assert all("wrong-password" not in r[1] and pw not in r[1] for r in rows)


# ---------------------------------------------------------------- token verification
def test_missing_or_malformed_credentials_get_401(api):
    assert api.get("/auth/me").status_code == 401
    assert api.get("/auth/me", headers={"Authorization": "Basic YWRtaW46YWRtaW4="}).status_code == 401
    assert api.get("/auth/me", headers=headers("not.a.jwt")).status_code == 401
    assert api.get("/auth/me", headers=headers("")).status_code == 401
    assert api.get("/auth/me").headers["www-authenticate"] == "Bearer"


def test_expired_future_and_incomplete_tokens_are_rejected(api, users):
    name, _ = users()
    now = datetime.now(timezone.utc)
    base = {"sub": name, "role": "analyst", "iss": ISSUER, "iat": now, "exp": now + timedelta(minutes=5)}
    assert api.get("/auth/me", headers=headers(jwt.encode(base, secret(), ALGORITHM))).status_code == 200
    expired = jwt.encode({**base, "iat": now - timedelta(hours=2), "exp": now - timedelta(hours=1)}, secret(), ALGORITHM)
    assert api.get("/auth/me", headers=headers(expired)).status_code == 401
    for missing in ("exp", "iat", "sub", "iss"):
        claims = {k: v for k, v in base.items() if k != missing}
        assert api.get("/auth/me", headers=headers(jwt.encode(claims, secret(), ALGORITHM))).status_code == 401, missing
    assert api.get("/auth/me", headers=headers(jwt.encode({**base, "iss": "someone-else"}, secret(), ALGORITHM))).status_code == 401


def test_tokens_signed_with_another_key_or_tampered_are_rejected(api, users):
    name, pw = users("analyst")
    good = api.post("/auth/login", json={"username": name, "password": pw}).json()["access_token"]
    now = datetime.now(timezone.utc)
    claims = {"sub": name, "role": "admin", "iss": ISSUER, "iat": now, "exp": now + timedelta(minutes=5)}
    assert api.get("/auth/me", headers=headers(jwt.encode(claims, "a-different-secret-" + "y" * 32, ALGORITHM))).status_code == 401
    head, payload, sig = good.split(".")
    forged = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    forged["role"] = "admin"
    new_payload = base64.urlsafe_b64encode(json.dumps(forged).encode()).rstrip(b"=").decode()
    assert api.get("/auth/me", headers=headers(f"{head}.{new_payload}.{sig}")).status_code == 401      # signature no longer matches


def test_alg_none_and_algorithm_confusion_tokens_are_rejected(api, users):
    name, _ = users()
    now = datetime.now(timezone.utc)
    claims = {"sub": name, "role": "admin", "iss": ISSUER, "iat": int(now.timestamp()), "exp": int((now + timedelta(minutes=5)).timestamp())}
    enc = lambda o: base64.urlsafe_b64encode(json.dumps(o).encode()).rstrip(b"=").decode()   # noqa: E731
    unsigned = f"{enc({'alg': 'none', 'typ': 'JWT'})}.{enc(claims)}."
    assert api.get("/auth/me", headers=headers(unsigned)).status_code == 401
    hs512 = jwt.encode(claims, secret(), "HS512")                   # right key, wrong (unpinned) algorithm
    assert api.get("/auth/me", headers=headers(hs512)).status_code == 401


def test_role_claim_in_the_token_is_never_trusted(api, users):
    name, _ = users("analyst")
    token, _ = create_token(name, "admin")                           # validly signed, but claims admin
    assert api.get("/auth/me", headers=headers(token)).json()["role"] == "analyst"
    assert api.get("/admin/users", headers=headers(token)).status_code == 403


def test_deactivation_and_role_changes_take_effect_immediately(api, users, login, app_engine):
    admin, ah = login("admin")
    name, pw = users("analyst")
    tok = api.post("/auth/login", json={"username": name, "password": pw}).json()["access_token"]
    assert api.get("/approvals", headers=headers(tok)).status_code == 403
    assert api.patch(f"/admin/users/{name}", json={"role": "engineer"}, headers=ah).status_code == 200
    assert api.get("/approvals", headers=headers(tok)).status_code == 200            # same token, new DB role
    assert api.patch(f"/admin/users/{name}", json={"role": "analyst"}, headers=ah).status_code == 200
    assert api.get("/approvals", headers=headers(tok)).status_code == 403
    assert api.patch(f"/admin/users/{name}", json={"is_active": False}, headers=ah).status_code == 200
    assert api.get("/auth/me", headers=headers(tok)).status_code == 401              # revoked without waiting for expiry
    assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 401
    with app_engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM app.audit_events WHERE action='user.update' AND actor_label=:a"), {"a": admin}).scalar() >= 3


def test_deleted_user_token_is_rejected(api, users, app_engine):
    name, pw = users()
    tok = api.post("/auth/login", json={"username": name, "password": pw}).json()["access_token"]
    with app_engine.begin() as c:
        c.execute(text("DELETE FROM app.users WHERE username=:n"), {"n": name})
    assert api.get("/auth/me", headers=headers(tok)).status_code == 401


def test_missing_or_weak_secret_disables_auth_instead_of_falling_back(api, users, monkeypatch):
    name, pw = users()
    for bad in ("", "short"):
        monkeypatch.setenv("JWT_SECRET", bad)
        get_settings.cache_clear()
        assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 503
        assert api.get("/auth/me", headers=headers("x.y.z")).status_code in (401, 503)
    monkeypatch.delenv("JWT_SECRET")
    get_settings.cache_clear()
    assert api.post("/auth/login", json={"username": name, "password": pw}).status_code == 503


# ---------------------------------------------------------------- user administration
def test_only_admins_manage_users_and_policy_is_enforced(api, login, users):
    _, ah = login("admin")
    _, eh = login("engineer")
    body = {"username": "t_new_user_1", "password": PASSWORD, "role": "analyst"}
    assert api.post("/admin/users", json=body, headers=eh).status_code == 403
    try:
        assert api.post("/admin/users", json={**body, "password": "short"}, headers=ah).status_code == 422
        assert api.post("/admin/users", json={**body, "password": "xxxxxxxxxxxxxx"}, headers=ah).status_code == 422       # repetitive
        assert api.post("/admin/users", json={**body, "password": "t_new_user_1-abcdef"}, headers=ah).status_code == 422   # contains username
        assert api.post("/admin/users", json={**body, "role": "root"}, headers=ah).status_code == 422
        assert api.post("/admin/users", json={**body, "username": "Bad Name"}, headers=ah).status_code == 422
        assert api.post("/admin/users", json=body, headers=ah).status_code == 201
        assert api.post("/admin/users", json=body, headers=ah).status_code == 409
        listing = api.get("/admin/users", headers=ah).json()
        assert "password" not in json.dumps(listing) and any(u["username"] == "t_new_user_1" for u in listing)
    finally:
        from sqlalchemy import create_engine
        import os
        with create_engine(os.environ["DATABASE_URL"]).begin() as c:
            c.execute(text("DELETE FROM app.users WHERE username = 't_new_user_1'"))


def test_admin_cannot_lock_themselves_out(api, login):
    name, ah = login("admin")
    assert api.patch(f"/admin/users/{name}", json={"is_active": False}, headers=ah).status_code == 409
    assert api.patch(f"/admin/users/{name}", json={"role": "analyst"}, headers=ah).status_code == 409
