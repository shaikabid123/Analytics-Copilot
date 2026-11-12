"""User management and login with lockout. Messages never reveal whether a username exists."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app import audit
from app.models import ROLE_NAMES, Role, User
from app.security import dummy_verify, hash_password, verify_password

MAX_FAILED = 5
LOCK_FOR = timedelta(minutes=15)
_USERNAME = re.compile(r"^[a-z][a-z0-9_.-]{2,31}$")


class UserError(ValueError):
    pass


def validate_password(username: str, password: str) -> None:
    if len(password) < 12:
        raise UserError("password must be at least 12 characters")
    if len(password) > 128:
        raise UserError("password must be at most 128 characters")
    if username.lower() in password.lower():
        raise UserError("password must not contain the username")
    if len(set(password)) < 5:
        raise UserError("password is too repetitive")


def create_user(session: Session, username: str, password: str, role: str, *, actor: str = "system",
                email: str | None = None) -> User:
    if not _USERNAME.match(username):
        raise UserError("username must be 3-32 chars: lowercase letters, digits, . _ - (starting with a letter)")
    if role not in ROLE_NAMES:
        raise UserError(f"role must be one of {ROLE_NAMES}")
    validate_password(username, password)
    role_row = session.scalar(select(Role).where(Role.name == role))
    user = User(username=username, password_hash=hash_password(password), role_id=role_row.id, email=email)
    session.add(user)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        raise UserError("username already exists") from None
    audit.record(session, actor=actor, action="user.create", resource_type="user", resource_id=user.id,
                 detail={"username": username, "role": role})
    session.commit()
    return user


def authenticate(session: Session, username: str, password: str) -> User | None:
    """Return the user on success, None on any failure (unknown user, bad password, inactive, locked)."""
    now = datetime.now(timezone.utc)
    user = session.scalar(select(User).where(User.username == username))
    if user is None:
        dummy_verify(password)
        audit.record(session, actor=username[:64] or "unknown", action="auth.login", resource_type="user",
                     outcome="failure", detail={"reason": "unknown_user"})
        session.commit()
        return None
    if user.locked_until and user.locked_until > now:
        dummy_verify(password)
        audit.record(session, actor=username, action="auth.login", resource_type="user", resource_id=user.id,
                     outcome="failure", detail={"reason": "locked"})
        session.commit()
        return None
    ok = verify_password(password, user.password_hash) and user.is_active
    if not ok:
        user.failed_attempts = (user.failed_attempts or 0) + 1
        reason = "inactive" if not user.is_active else "bad_password"
        if user.failed_attempts >= MAX_FAILED:
            user.locked_until, user.failed_attempts, reason = now + LOCK_FOR, 0, "locked_now"
        audit.record(session, actor=username, action="auth.login", resource_type="user", resource_id=user.id,
                     outcome="failure", detail={"reason": reason})
        session.commit()
        return None
    user.failed_attempts, user.locked_until, user.last_login_at = 0, None, now
    audit.record(session, actor=username, action="auth.login", resource_type="user", resource_id=user.id)
    session.commit()
    return user
