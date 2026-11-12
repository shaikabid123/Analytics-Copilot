"""Authentication and authorization primitives.

  * passwords: scrypt (stdlib) with a per-user random salt, constant-time comparison
  * tokens: HS256 JWT; the algorithm is pinned on decode (so `alg: none` / algorithm-confusion tokens are rejected),
            exp/iat/sub/iss are required
  * every request re-reads the user from the database: deactivation and role changes take effect immediately,
    and the role used for authorization is the DATABASE role, never the one inside the token
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import audit
from app.config import get_settings
from app.db import get_engine, get_session
from app.models import User

ISSUER = "copilot-api"
ALGORITHM = "HS256"
MIN_SECRET_LEN = 32
_N, _R, _P = 2 ** 14, 8, 1


class AuthConfigError(Exception):
    pass


class InvalidToken(Exception):
    pass


# ------------------------------------------------------------------------------------------ passwords
def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P, dklen=32)
    return f"scrypt${_N}${_R}${_P}${_b64(salt)}${_b64(dk)}"


def verify_password(password: str, stored: str) -> bool:
    """Constant-time check; malformed stored hashes simply fail (never raise)."""
    try:
        scheme, n, r, p, salt, dk = stored.split("$")
        if scheme != "scrypt":
            return False
        calc = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p), dklen=32)
        return hmac.compare_digest(calc, base64.b64decode(dk))
    except Exception:  # noqa: BLE001
        return False


_DUMMY = None


def dummy_verify(password: str) -> None:
    """Burn the same CPU as a real check so unknown usernames are not distinguishable by timing."""
    global _DUMMY
    if _DUMMY is None:
        _DUMMY = hash_password(secrets.token_hex(8))
    verify_password(password, _DUMMY)


# ------------------------------------------------------------------------------------------ tokens
def _secret() -> str:
    s = get_settings().jwt_secret
    if not s or len(s) < MIN_SECRET_LEN:
        raise AuthConfigError(f"JWT_SECRET must be set to at least {MIN_SECRET_LEN} characters")
    return s


def create_token(username: str, role: str, ttl: timedelta | None = None) -> tuple[str, int]:
    ttl = ttl or timedelta(minutes=get_settings().jwt_ttl_minutes)
    now = datetime.now(timezone.utc)
    payload = {"sub": username, "role": role, "iss": ISSUER, "iat": now, "exp": now + ttl}
    return jwt.encode(payload, _secret(), algorithm=ALGORITHM), int(ttl.total_seconds())


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, _secret(), algorithms=[ALGORITHM], issuer=ISSUER,
                          options={"require": ["exp", "iat", "sub", "iss"]})
    except jwt.PyJWTError as exc:
        raise InvalidToken(type(exc).__name__) from None


# ------------------------------------------------------------------------------------------ dependencies
@dataclass(frozen=True)
class Principal:
    user_id: int
    username: str
    role: str


_bearer = HTTPBearer(auto_error=False, description="JWT from POST /auth/login")
_UNAUTH = {"WWW-Authenticate": "Bearer"}


def current_user(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
                 session: Session = Depends(get_session)) -> Principal:
    if creds is None or creds.scheme.lower() != "bearer":
        raise HTTPException(401, "authentication required", headers=_UNAUTH)
    try:
        payload = decode_token(creds.credentials)
    except InvalidToken:
        raise HTTPException(401, "invalid or expired token", headers=_UNAUTH) from None
    except AuthConfigError:
        raise HTTPException(503, "authentication is not configured") from None
    user = session.scalar(select(User).where(User.username == payload["sub"]))
    if user is None or not user.is_active:
        raise HTTPException(401, "invalid or expired token", headers=_UNAUTH)
    request.state.username = user.username                          # for the access log only
    return Principal(user.id, user.username, user.role.name)       # role from the database, not the token


def require_roles(*roles: str):
    """Dependency factory: authenticated AND holding one of `roles`. Denials are audited."""
    allowed = frozenset(roles)

    def dep(request: Request, user: Principal = Depends(current_user)) -> Principal:
        if user.role not in allowed:
            from sqlalchemy.orm import Session as S
            with S(get_engine()) as s:
                audit.record(s, actor=user.username, action="authz.denied", resource_type="endpoint",
                             resource_id=f"{request.method} {request.url.path}"[:64], outcome="denied",
                             detail={"role": user.role, "required": sorted(allowed)})
                s.commit()
            raise HTTPException(403, "your role is not permitted to perform this operation")
        return user

    dep.__name__ = f"require_{'_'.join(sorted(allowed))}"
    return dep


ANY_USER = Depends(current_user)
