"""Deliver pending data-quality alerts: Slack message for every alert, plus a ticket for HIGH severity.
An alert is 'sent' only when every channel it needs has succeeded. Failures keep it queued with exponential
back-off and a bounded number of attempts; if the integration is down the loop stops early instead of hammering it."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_engine
from app.integrations import slack, tickets
from app.models import DqAlert

STOP_AFTER_CONSECUTIVE_FAILURES = 3


def format_alert(a: DqAlert) -> str:
    d = a.detail or {}
    return f"[{a.severity.upper()}] {a.title} (observed={d.get('observed')}, threshold={d.get('threshold')}, alert #{a.id}, run {a.run_id})"


def dispatch_pending_alerts(limit: int = 50, *, now: datetime | None = None, sleep=None,
                            only_runs: list[int] | None = None) -> dict:
    cfg = get_settings()
    summary = {"configured": bool(cfg.alert_webhook_url), "sent": 0, "retry_later": 0, "failed": 0, "stopped_early": False,
               "degraded": False}
    if not cfg.alert_webhook_url:
        return summary
    now = now or datetime.now(timezone.utc)
    consecutive = 0
    with Session(get_engine()) as s:
        stmt = select(DqAlert).where(DqAlert.status == "pending",
                                     (DqAlert.next_attempt_at.is_(None)) | (DqAlert.next_attempt_at <= now))
        if only_runs is not None:
            stmt = stmt.where(DqAlert.run_id.in_(only_runs))
        alerts = s.scalars(stmt.order_by(DqAlert.id).limit(limit)).all()
        for a in alerts:
            if consecutive >= STOP_AFTER_CONSECUTIVE_FAILURES:
                summary["stopped_early"] = summary["degraded"] = True
                break
            problems: list[str] = []
            if a.sent_at is None:
                r = slack.send_alert(format_alert(a), severity=a.severity, resource=("dq_alert", str(a.id)), sleep=sleep)
                if r.ok:
                    a.sent_at = now
                else:
                    problems.append(f"slack: {r.status} {r.error}")
            if a.severity == "high" and a.ticket_key is None and cfg.ticket_api_url:
                r = tickets.create_ticket(a.title, format_alert(a), ["data-quality", a.severity], f"dq-alert-{a.id}",
                                          resource=("dq_alert", str(a.id)), sleep=sleep)
                if r.ok and r.data.get("key"):
                    a.ticket_key = str(r.data["key"])[:64]
                else:
                    problems.append(f"tickets: {r.status} {r.error}")
            if not problems:
                a.status, a.last_error = "sent", None
                summary["sent"] += 1
                consecutive = 0
            else:
                a.attempts += 1
                a.last_error = "; ".join(problems)[:300]
                consecutive += 1
                summary["degraded"] = True
                if a.attempts >= cfg.alert_max_attempts:
                    a.status = "failed"
                    summary["failed"] += 1
                else:
                    a.next_attempt_at = now + timedelta(minutes=min(60, 2 ** a.attempts))
                    summary["retry_later"] += 1
            s.commit()
    return summary
