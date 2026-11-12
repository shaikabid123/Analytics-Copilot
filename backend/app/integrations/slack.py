"""Alert webhook (Slack-style incoming webhook: POST {"text": ...}). Works with the bundled mock and with any
Slack-compatible webhook URL. NOTE: only the mock has been exercised in this project."""
from app.config import get_settings
from app.integrations.http import CallResult, call_json


def send_alert(text: str, *, severity: str = "info", resource: tuple[str, str] | None = None,
               timeout_s: float | None = None, retries: int | None = None, sleep=None) -> CallResult:
    url = get_settings().alert_webhook_url
    if not url:
        return CallResult(False, "failed", error="alert webhook is not configured", attempts=0)
    kwargs = {"sleep": sleep} if sleep else {}
    return call_json("slack", "send_alert", url, {"text": text[:3000], "severity": severity, "username": "copilot"},
                     timeout_s=timeout_s, retries=retries, resource=resource, **kwargs)


def notify_safely(text: str, *, resource: tuple[str, str] | None = None) -> bool:
    """Best-effort notification for workflow events: short timeout, no retries, never raises or blocks the workflow."""
    try:
        return send_alert(text, severity="info", resource=resource, timeout_s=2.0, retries=0).ok
    except Exception:  # noqa: BLE001
        return False
