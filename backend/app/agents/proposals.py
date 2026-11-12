"""The ONLY changes an approved proposal may make. Both the ETL agent (when proposing) and apply_change
(when executing) validate against this table, so a tampered or invented action cannot run."""
from __future__ import annotations

import hashlib
import json

# action -> (requires_approval, description)
ACTIONS: dict[str, tuple[bool, str]] = {
    "set_max_reject_pct": (True, "Raise the per-dataset quarantine threshold that blocks Gold publication"),
    "set_max_retries": (True, "Change how many times a transient failure is retried"),
    "reset_baseline_next_run": (True, "Re-learn drift/volume baselines from the next successful run"),
    "advice_only": (False, "Recommendation for an engineer; nothing is changed automatically"),
}
MAX_REJECT_PCT_CAP = 10.0
MAX_RETRIES_CAP = 5


def validate_proposal(p: object) -> list[str]:
    if not isinstance(p, dict):
        return ["proposal must be an object"]
    errors: list[str] = []
    action, params = p.get("action"), p.get("params", {})
    if action not in ACTIONS:
        return [f"action '{action}' is not allowed"]
    if not isinstance(params, dict):
        return ["params must be an object"]
    if p.get("requires_approval") != ACTIONS[action][0]:
        errors.append("requires_approval does not match the action's policy")
    if not isinstance(p.get("pipeline"), str) or not p["pipeline"]:
        errors.append("pipeline is required")
    v = params.get("value")
    if action == "set_max_reject_pct":
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not 0 < v <= MAX_REJECT_PCT_CAP:
            errors.append(f"value must be a number in (0, {MAX_REJECT_PCT_CAP}]")
    elif action == "set_max_retries":
        if not isinstance(v, int) or isinstance(v, bool) or not 0 <= v <= MAX_RETRIES_CAP:
            errors.append(f"value must be an integer in [0, {MAX_RETRIES_CAP}]")
    elif action == "reset_baseline_next_run" and params:
        errors.append("this action takes no parameters")
    extra = set(params) - {"value", "steps"}
    if extra:
        errors.append(f"unexpected params: {sorted(extra)}")
    return errors


def proposal_hash(p: dict) -> str:
    return hashlib.sha256(json.dumps(p, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
