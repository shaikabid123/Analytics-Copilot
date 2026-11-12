"""Ticketing API (Jira-style REST: POST {base}/issues with a bearer token). Creation is idempotent via an
Idempotency-Key header, so a retry after a timeout cannot create a duplicate ticket."""
from app.config import get_settings
from app.integrations.http import CallResult, call_json


def create_ticket(summary: str, description: str, labels: list[str], idempotency_key: str, *,
                  resource: tuple[str, str] | None = None, sleep=None) -> CallResult:
    cfg = get_settings()
    if not cfg.ticket_api_url or not cfg.ticket_api_token:
        return CallResult(False, "failed", error="ticket API is not configured", attempts=0)
    kwargs = {"sleep": sleep} if sleep else {}
    return call_json("tickets", "create_ticket", cfg.ticket_api_url.rstrip("/") + "/issues",
                     {"summary": summary[:200], "description": description[:4000], "labels": labels},
                     headers={"Authorization": f"Bearer {cfg.ticket_api_token}", "Idempotency-Key": idempotency_key},
                     resource=resource, **kwargs)
