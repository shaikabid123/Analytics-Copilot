"""Typed shared state for the single LangGraph workflow. Every node reads and writes only these keys.
Values are plain JSON-compatible types so the whole state can be checkpointed to Postgres."""
from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict

INTENTS = ("metadata", "sql", "dq", "etl", "unknown")


class CopilotState(TypedDict, total=False):
    # request
    run_id: str
    actor: str
    role: str
    request_text: str
    pipeline: str | None
    target_run_id: int | None
    # routing
    intent: str
    intent_source: str                       # rules | llm | default
    denied: str | None                       # permission message (deterministic)
    error: str | None
    steps: Annotated[list[str], operator.add]              # audit-friendly trail of nodes visited
    degraded_reasons: Annotated[list[str], operator.add]   # why a fallback path was used
    # metadata agent
    metadata_context: dict[str, Any]
    # sql planner
    sql_draft: str
    sql_source: str                          # llm | template
    sql_attempts: int
    template_tried: bool
    guard_errors: list[str]
    sql_validated: str
    sql_result: dict[str, Any]
    # data quality + etl recommendation
    dq_report: dict[str, Any]
    diagnosis: dict[str, Any]
    proposal: dict[str, Any] | None
    approval_id: int | None
    approval_status: str | None
    applied: dict[str, Any] | None
    # output
    answer: str
    verified: bool
    verification_notes: list[str]
