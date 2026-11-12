from sqlalchemy.orm import Session

from app.models import AuditEvent


def record(session: Session, *, actor: str, action: str, resource_type: str | None = None,
           resource_id: str | int | None = None, outcome: str = "success", detail: dict | None = None,
           actor_user_id: int | None = None) -> None:
    """Append one audit row (the table is insert-only at the database level). Caller commits."""
    session.add(AuditEvent(
        actor_label=actor, actor_user_id=actor_user_id, action=action, resource_type=resource_type,
        resource_id=None if resource_id is None else str(resource_id), outcome=outcome, detail=detail or {}))
