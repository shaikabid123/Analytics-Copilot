"""Permissioned tool registry. Agents never touch the database, LLM or executor directly: every capability is a
named tool, each tool lists which agents may call it and which roles may use it, and every call (allowed or
denied) is written to the audit trail. The check is deterministic code, not a prompt."""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app import audit
from app.agents.proposals import ACTIONS, proposal_hash, validate_proposal
from app.db import get_engine
from app.llm import LLMClient, LLMUnavailable
from app.models import (
    AgentRun, Approval, CatalogColumn, CatalogDataset, DqAlert, DqResult, GlossaryTerm, GlossaryTermColumn,
    LlmCall, Pipeline, PipelineRun, QueryAudit,
)
from app.services import catalog as catalog_svc
from app.services.sql_exec import CostExceeded, GuardRejected, QueryError, run_guarded_query
from app.services.sql_guard import ROLE_TABLES, validate_sql

ROLES = ("analyst", "engineer", "admin")
PRIVILEGED = frozenset({"engineer", "admin"})
EVERYONE = frozenset(ROLES)

# agent names (also used in audit rows)
ROUTER, METADATA, SQL_PLANNER, DATA_QUALITY, ETL_RECO, EXECUTOR = (
    "router", "metadata_discovery", "sql_planner", "data_quality", "etl_recommendation", "approval_executor")


class PermissionDenied(Exception):
    pass


class ApplyRefused(Exception):
    pass


@dataclass(frozen=True)
class ToolContext:
    run_id: str
    actor: str
    role: str
    llm: LLMClient
    agent: str = ""
    question: str | None = None


@dataclass(frozen=True)
class ToolSpec:
    name: str
    fn: Callable[..., Any]
    agents: frozenset[str]
    roles: frozenset[str]


def _session() -> Session:
    return Session(get_engine())


def _short(v: Any) -> Any:
    return v[:200] if isinstance(v, str) else (v if isinstance(v, (int, float, bool, type(None))) else type(v).__name__)


# ------------------------------------------------------------------------------------------ metadata tools
def catalog_search(ctx: ToolContext, q: str, limit: int = 8) -> list[dict]:
    with _session() as s:
        return catalog_svc.search(s, q, limit)


def dataset_columns(ctx: ToolContext, dataset_id: int) -> dict:
    with _session() as s:
        ds = s.get(CatalogDataset, dataset_id)
        if ds is None:
            return {}
        cols = s.scalars(select(CatalogColumn).where(CatalogColumn.dataset_id == dataset_id).order_by(CatalogColumn.ordinal))
        return {"id": ds.id, "name": f"{ds.schema_name}.{ds.table_name}", "layer": ds.layer, "owner": ds.owner_name,
                "description": ds.description, "row_count": ds.row_count,
                "columns": [{"name": c.name, "type": c.data_type, "description": c.description, "pii": c.is_pii}
                            for c in cols if not c.name.startswith("_")]}


def glossary_lookup(ctx: ToolContext, q: str) -> list[dict]:
    tsq = catalog_svc.build_prefix_tsquery(q)
    if not tsq:
        return []
    from sqlalchemy import func
    with _session() as s:
        terms = s.scalars(select(GlossaryTerm).where(GlossaryTerm.search_vector.op("@@")(func.to_tsquery("english", tsq)))
                          .order_by(GlossaryTerm.term).limit(5)).all()
        out = []
        for t in terms:
            cols = s.execute(select(CatalogDataset.schema_name, CatalogDataset.table_name, CatalogColumn.name)
                             .join(CatalogColumn, CatalogColumn.dataset_id == CatalogDataset.id)
                             .join(GlossaryTermColumn, GlossaryTermColumn.column_id == CatalogColumn.id)
                             .where(GlossaryTermColumn.term_id == t.id)).all()
            out.append({"term": t.term, "definition": t.definition,
                        "columns": [f"{a}.{b}.{c}" for a, b, c in cols]})
        return out


def gold_schema(ctx: ToolContext) -> list[dict]:
    """Gold tables this ROLE may query, with column descriptions (the SQL planner's only schema context)."""
    allowed = ROLE_TABLES.get(ctx.role, frozenset())
    with _session() as s:
        rows = s.scalars(select(CatalogDataset).where(CatalogDataset.layer == "gold").order_by(CatalogDataset.table_name)).all()
        ids = [d.id for d in rows if d.table_name in allowed]
    return [dataset_columns(ctx, i) for i in ids]


# ------------------------------------------------------------------------------------------ LLM tool
def llm_complete(ctx: ToolContext, system: str, user: str, max_tokens: int = 512):
    try:
        resp = ctx.llm.chat(system, user, max_tokens=max_tokens)
    except LLMUnavailable as exc:
        if ctx.llm.provider != "none":              # an unconfigured provider is not a failed call
            _record_llm(ctx, "timeout" if exc.timeout else "error", None, str(exc))
        raise
    _record_llm(ctx, "ok", resp, None)
    return resp


def _record_llm(ctx: ToolContext, status: str, resp, error: str | None) -> None:
    with _session() as s:
        run = s.get(AgentRun, ctx.run_id) if ctx.run_id else None
        s.add(LlmCall(agent_run_id=run.id if run else None, agent=ctx.agent, provider=ctx.llm.provider,
                      model=getattr(resp, "model", None) or ctx.llm.model, status=status,
                      latency_ms=getattr(resp, "latency_ms", None), prompt_tokens=getattr(resp, "prompt_tokens", None),
                      completion_tokens=getattr(resp, "completion_tokens", None), error=(error or None) and error[:300]))
        s.commit()


# ------------------------------------------------------------------------------------------ SQL tools
def _query_audit(ctx: ToolContext, *, sql: str, executed: str | None, source: str, status: str,
                 reasons: list[str] | None = None, cost: float | None = None, rows: int | None = None,
                 ms: int | None = None) -> None:
    with _session() as s:
        run = s.get(AgentRun, ctx.run_id) if ctx.run_id else None
        s.add(QueryAudit(agent_run_id=run.id if run else None, actor=ctx.actor, role=ctx.role, question=ctx.question,
                         generated_sql=sql[:4000], executed_sql=executed, source=source, status=status,
                         block_reasons=reasons or [], estimated_cost=cost, row_count=rows, duration_ms=ms))
        s.commit()


def sql_validate(ctx: ToolContext, sql: str, source: str = "llm") -> dict:
    g = validate_sql(sql, ctx.role)
    if not g.ok:
        _query_audit(ctx, sql=str(sql), executed=None, source=source, status="blocked", reasons=g.errors)
    return {"ok": g.ok, "sql": g.sql, "errors": g.errors, "tables": sorted(g.tables)}


def sql_execute(ctx: ToolContext, sql: str, source: str = "llm") -> dict:
    try:
        res = run_guarded_query(sql, ctx.role)
    except (GuardRejected, CostExceeded) as exc:
        reasons = exc.errors if isinstance(exc, GuardRejected) else [str(exc)]
        _query_audit(ctx, sql=sql, executed=None, source=source, status="blocked", reasons=reasons)
        return {"ok": False, "kind": "blocked", "errors": reasons}
    except QueryError as exc:
        _query_audit(ctx, sql=sql, executed=None, source=source, status="failed", reasons=[str(exc)])
        return {"ok": False, "kind": "failed", "errors": [str(exc)]}
    _query_audit(ctx, sql=sql, executed=res.sql, source=source, status="executed", cost=res.estimated_cost,
                 rows=res.row_count, ms=res.duration_ms)
    return {"ok": True, "sql": res.sql, "columns": res.columns, "rows": res.rows, "row_count": res.row_count,
            "truncated": res.truncated, "duration_ms": res.duration_ms, "estimated_cost": res.estimated_cost}


# ------------------------------------------------------------------------------------------ pipeline/DQ tools
def _run_row(s: Session, run: PipelineRun) -> dict:
    p = s.get(Pipeline, run.pipeline_id)
    d = run.details or {}
    return {"id": run.id, "pipeline": p.name, "status": run.status, "attempt": run.attempt, "trigger": run.trigger,
            "error": run.error_message, "retryable": d.get("retryable"), "rows_in": run.rows_in, "rows_out": run.rows_out,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "max_retries": d.get("max_retries"), "params": p.params or {}}


def dq_latest_run(ctx: ToolContext, pipeline: str) -> dict | None:
    with _session() as s:
        p = s.scalar(select(Pipeline).where(Pipeline.name == pipeline))
        if p is None:
            return None
        run = s.scalar(select(PipelineRun).where(PipelineRun.pipeline_id == p.id).order_by(PipelineRun.id.desc()).limit(1))
        return _run_row(s, run) if run else None


def run_details(ctx: ToolContext, run_id: int) -> dict | None:
    with _session() as s:
        run = s.get(PipelineRun, run_id)
        return _run_row(s, run) if run else None


def dq_results(ctx: ToolContext, run_id: int) -> list[dict]:
    with _session() as s:
        rows = s.scalars(select(DqResult).where(DqResult.run_id == run_id).order_by(DqResult.id)).all()
        return [{"dataset": r.dataset, "check": r.check_name, "type": r.check_type, "severity": r.severity,
                 "status": r.status, "observed": r.observed, "threshold": r.threshold, "detail": r.detail} for r in rows]


def dq_alerts(ctx: ToolContext, run_id: int) -> list[dict]:
    with _session() as s:
        rows = s.scalars(select(DqAlert).where(DqAlert.run_id == run_id)).all()
        return [{"severity": a.severity, "title": a.title, "status": a.status} for a in rows]


# ------------------------------------------------------------------------------------------ approval tools
def create_approval(ctx: ToolContext, proposal: dict, summary: str, pipeline_run_id: int | None) -> int:
    errors = validate_proposal(proposal)
    if errors or not ACTIONS[proposal["action"]][0]:
        raise PermissionDenied(f"cannot request approval for this proposal: {errors or 'action does not need approval'}")
    with _session() as s:
        run = s.get(AgentRun, ctx.run_id)
        ap = Approval(request_type="etl_change", status="pending", agent_run_id=run.id if run else None,
                      pipeline_run_id=pipeline_run_id, summary=summary[:1000], requested_by_label=ctx.actor,
                      proposed_change={"proposal": proposal, "proposal_hash": proposal_hash(proposal)})
        s.add(ap)
        s.flush()
        audit.record(s, actor=ctx.actor, action="approval.requested", resource_type="approval", resource_id=ap.id,
                     detail={"action": proposal["action"], "pipeline": proposal["pipeline"], "agent": ctx.agent})
        s.commit()
        approval_id = ap.id
    from app.integrations.slack import notify_safely
    notify_safely(f"Approval #{approval_id} requested by {ctx.actor}: {summary[:300]}", resource=("approval", str(approval_id)))
    return approval_id


def apply_change(ctx: ToolContext, approval_id: int) -> dict:
    """The ONLY code path that changes pipeline configuration. Requires an approval recorded in the database as
    approved by an engineer/admin, an untampered proposal, and an action from the allowlist."""
    with _session() as s:
        ap = s.execute(select(Approval).where(Approval.id == approval_id).with_for_update()).scalar_one_or_none()
        if ap is None:
            raise ApplyRefused("approval not found")
        if ap.status != "approved":
            raise ApplyRefused(f"approval is '{ap.status}', not approved")
        change = dict(ap.proposed_change or {})
        proposal = change.get("proposal") or {}
        if change.get("applied_at"):
            return {"already_applied": True, "action": proposal.get("action")}
        if proposal_hash(proposal) != change.get("proposal_hash"):
            audit.record(s, actor=ctx.actor, action="approval.tamper_detected", resource_type="approval",
                         resource_id=ap.id, outcome="failure")
            s.commit()
            raise ApplyRefused("proposal was modified after it was requested; refusing to apply")
        errors = validate_proposal(proposal)
        if errors:
            raise ApplyRefused(f"proposal is not valid: {errors}")
        pipe = s.scalar(select(Pipeline).where(Pipeline.name == proposal["pipeline"]).with_for_update())
        if pipe is None:
            raise ApplyRefused("pipeline not found")
        before = {"params": dict(pipe.params or {}), "max_retries": pipe.max_retries}
        params = dict(pipe.params or {})
        action, value = proposal["action"], (proposal.get("params") or {}).get("value")
        if action == "set_max_reject_pct":
            params["max_reject_pct"] = float(value)
            pipe.params = params
        elif action == "reset_baseline_next_run":
            params["reset_baseline_next_run"] = True
            pipe.params = params
        elif action == "set_max_retries":
            pipe.max_retries = int(value)
        else:
            raise ApplyRefused(f"action '{action}' makes no automatic change")
        change["applied_at"] = datetime.now(timezone.utc).isoformat()
        ap.proposed_change = change
        audit.record(s, actor=ap.decided_by_label or "unknown", action="pipeline.change.applied",
                     resource_type="pipeline", resource_id=pipe.id,
                     detail={"approval_id": ap.id, "action": action, "value": value, "pipeline": pipe.name,
                             "before": before, "after": {"params": params, "max_retries": pipe.max_retries}})
        s.commit()
        return {"action": action, "value": value, "pipeline": pipe.name, "before": before}


# ------------------------------------------------------------------------------------------ registry
def _spec(name, fn, agents, roles):
    return ToolSpec(name, fn, frozenset(agents), frozenset(roles))


TOOLS: dict[str, ToolSpec] = {t.name: t for t in (
    _spec("catalog_search", catalog_search, {METADATA}, EVERYONE),
    _spec("dataset_columns", dataset_columns, {METADATA}, EVERYONE),
    _spec("glossary_lookup", glossary_lookup, {METADATA}, EVERYONE),
    _spec("gold_schema", gold_schema, {METADATA}, EVERYONE),
    _spec("llm_complete", llm_complete, {ROUTER, SQL_PLANNER, DATA_QUALITY, ETL_RECO}, EVERYONE),
    _spec("sql_validate", sql_validate, {SQL_PLANNER}, EVERYONE),
    _spec("sql_execute", sql_execute, {SQL_PLANNER}, EVERYONE),
    _spec("dq_latest_run", dq_latest_run, {DATA_QUALITY}, PRIVILEGED),
    _spec("dq_results", dq_results, {DATA_QUALITY, ETL_RECO}, PRIVILEGED),
    _spec("dq_alerts", dq_alerts, {DATA_QUALITY}, PRIVILEGED),
    _spec("run_details", run_details, {DATA_QUALITY, ETL_RECO}, PRIVILEGED),
    _spec("create_approval", create_approval, {ETL_RECO}, PRIVILEGED),
    _spec("apply_change", apply_change, {EXECUTOR}, PRIVILEGED),
)}


def call_tool(agent: str, name: str, ctx: ToolContext, **kwargs) -> Any:
    """Single entry point for every tool call. Raises PermissionDenied (and audits it) when the agent is not
    allowed to use the tool or the caller's role is too low."""
    spec = TOOLS.get(name)
    detail = {"agent": agent, "tool": name, "args": {k: _short(v) for k, v in kwargs.items()}}
    denial = None
    if spec is None:
        denial = f"unknown tool '{name}'"
    elif agent not in spec.agents:
        denial = f"agent '{agent}' is not permitted to use tool '{name}'"
    elif ctx.role not in spec.roles:
        denial = f"role '{ctx.role}' is not permitted to use tool '{name}'"
    if denial:
        with _session() as s:
            audit.record(s, actor=ctx.actor, action="agent.tool.denied", resource_type="agent_run",
                         resource_id=ctx.run_id, outcome="denied", detail={**detail, "reason": denial})
            s.commit()
        raise PermissionDenied(denial)
    ctx = dataclasses.replace(ctx, agent=agent)
    try:
        result = spec.fn(ctx, **kwargs)  # type: ignore[union-attr]
    except Exception as exc:
        outcome = "failure"
        with _session() as s:
            audit.record(s, actor=ctx.actor, action="agent.tool.call", resource_type="agent_run",
                         resource_id=ctx.run_id, outcome=outcome, detail={**detail, "error": type(exc).__name__})
            s.commit()
        raise
    with _session() as s:
        audit.record(s, actor=ctx.actor, action="agent.tool.call", resource_type="agent_run",
                     resource_id=ctx.run_id, detail=detail)
        s.commit()
    return result
