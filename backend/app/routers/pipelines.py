"""Pipeline status, execution and recovery controls.

Execution model: the API never runs Spark. `POST .../runs` ENQUEUES a run (status 'queued'); the ETL worker claims it.
A partial unique index guarantees at most one queued-or-running run per pipeline, even under concurrent requests.
Viewing/starting/retrying/cancelling: engineer or admin. Abandoning a stuck run and reverting overrides: admin only."""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app import audit
from app.config import get_settings
from app.db import get_session
from app.models import DqResult, Pipeline, PipelineRun
from app.security import Principal, require_roles

router = APIRouter(prefix="/pipelines", tags=["pipelines"], dependencies=[Depends(require_roles("engineer", "admin"))])
OPERATOR = require_roles("engineer", "admin")
ADMIN = require_roles("admin")
SUPPORTED = ("sales", "events")                      # pipelines that have an implementation the worker can run
OVERRIDABLE = ("max_reject_pct", "reset_baseline_next_run")
NAME = "^[a-z][a-z0-9_]{1,63}$"


def _now():
    return datetime.now(timezone.utc)


def _run_dict(r: PipelineRun, name: str) -> dict:
    return {"id": r.id, "pipeline": name, "status": r.status, "attempt": r.attempt, "trigger": r.trigger,
            "retry_of": r.retry_of_run_id, "rows_in": r.rows_in, "rows_out": r.rows_out, "error": r.error_message,
            "queued_at": r.queued_at.isoformat() if r.queued_at else None,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None}


def _pipeline(session: Session, name: str) -> Pipeline:
    p = session.scalar(select(Pipeline).where(Pipeline.name == name))
    if p is None or name not in SUPPORTED:
        raise HTTPException(404, "pipeline not found")
    return p


def _enqueue(session: Session, p: Pipeline, user: Principal, trigger: str, retry_of: int | None) -> PipelineRun:
    run = PipelineRun(pipeline_id=p.id, status="queued", trigger=trigger, attempt=1, retry_of_run_id=retry_of,
                      triggered_by=user.user_id)
    session.add(run)
    try:
        session.flush()
    except IntegrityError:                           # the partial unique index: another run is queued or running
        session.rollback()
        active = session.execute(select(PipelineRun.id, PipelineRun.status).where(
            PipelineRun.pipeline_id == p.id, PipelineRun.status.in_(("queued", "running")))).first()
        detail = f"run {active[0]} is already {active[1]}" if active else "another run is active"
        raise HTTPException(409, f"{p.name}: {detail}") from None
    audit.record(session, actor=user.username, action="pipeline.run.requested", resource_type="pipeline_run",
                 resource_id=run.id, detail={"pipeline": p.name, "trigger": trigger, "retry_of": retry_of})
    session.commit()
    return run


@router.get("")
def overview(session: Session = Depends(get_session)):
    sla = get_settings().pipeline_sla_hours
    out = []
    for p in session.scalars(select(Pipeline).where(Pipeline.name.in_(SUPPORTED)).order_by(Pipeline.name)):
        last = session.scalar(select(PipelineRun).where(PipelineRun.pipeline_id == p.id).order_by(PipelineRun.id.desc()).limit(1))
        ok = session.scalar(select(PipelineRun.finished_at).where(PipelineRun.pipeline_id == p.id, PipelineRun.status == "succeeded")
                            .order_by(PipelineRun.id.desc()).limit(1))
        active = session.execute(select(PipelineRun.id, PipelineRun.status).where(
            PipelineRun.pipeline_id == p.id, PipelineRun.status.in_(("queued", "running")))).first()
        hours = (_now() - ok).total_seconds() / 3600 if ok else None
        out.append({"name": p.name, "description": p.description, "schedule_cron": p.schedule_cron, "max_retries": p.max_retries,
                    "overrides": p.params or {}, "last_run": _run_dict(last, p.name) if last else None,
                    "last_success_at": ok.isoformat() if ok else None,
                    "hours_since_success": round(hours, 2) if hours is not None else None, "sla_hours": sla,
                    "sla_breached": hours is None or hours > sla,
                    "active_run": {"id": active[0], "status": active[1]} if active else None})
    return out


@router.get("/runs")
def runs(limit: int = Query(20, ge=1, le=100), pipeline: str | None = Query(None, pattern=NAME),
         status: str | None = Query(None, pattern="^(queued|running|succeeded|failed|retrying)$"),
         session: Session = Depends(get_session)):
    stmt = select(PipelineRun, Pipeline.name).join(Pipeline, Pipeline.id == PipelineRun.pipeline_id).order_by(PipelineRun.id.desc()).limit(limit)
    if pipeline:
        stmt = stmt.where(Pipeline.name == pipeline)
    if status:
        stmt = stmt.where(PipelineRun.status == status)
    return [_run_dict(r, n) for r, n in session.execute(stmt)]


@router.get("/runs/{run_id}/checks")
def checks(run_id: int, session: Session = Depends(get_session)):
    if session.get(PipelineRun, run_id) is None:
        raise HTTPException(404, "run not found")
    return [{"dataset": c.dataset, "check": c.check_name, "type": c.check_type, "severity": c.severity, "status": c.status,
             "observed": c.observed, "threshold": c.threshold, "detail": c.detail}
            for c in session.scalars(select(DqResult).where(DqResult.run_id == run_id).order_by(DqResult.id))]


@router.get("/quality/summary")
def quality_summary(pipeline: str = Query(..., pattern=NAME), limit: int = Query(20, ge=1, le=100),
                    session: Session = Depends(get_session)):
    """Per-run quality trend (oldest first): check counts, reject rate per dataset, drift PSI per column."""
    p = _pipeline(session, pipeline)
    run_rows = session.scalars(select(PipelineRun).where(PipelineRun.pipeline_id == p.id, PipelineRun.finished_at.is_not(None),
                                                         PipelineRun.status.in_(("succeeded", "failed")))
                               .order_by(PipelineRun.id.desc()).limit(limit)).all()
    out = []
    for r in reversed(run_rows):
        res = session.scalars(select(DqResult).where(DqResult.run_id == r.id)).all()
        out.append({"run_id": r.id, "status": r.status, "finished_at": r.finished_at.isoformat(), "checks": len(res),
                    "failed": sum(1 for c in res if c.status == "failed"), "warnings": sum(1 for c in res if c.status == "warning"),
                    "reject_rate": {c.dataset: c.observed for c in res if c.check_name == "reject_rate" and c.observed is not None},
                    "drift_psi": {f"{c.dataset}.{c.check_name[6:]}": c.observed for c in res
                                  if c.check_type == "drift" and c.observed is not None}})
    return {"pipeline": pipeline, "runs": out}


@router.post("/{name}/runs", status_code=202)
def start(name: str, user: Principal = Depends(OPERATOR), session: Session = Depends(get_session)):
    p = _pipeline(session, name)
    run = _enqueue(session, p, user, "manual", None)
    return {"run_id": run.id, "status": "queued", "pipeline": name}


@router.post("/runs/{run_id}/retry", status_code=202)
def retry(run_id: int, user: Principal = Depends(OPERATOR), session: Session = Depends(get_session)):
    r = session.get(PipelineRun, run_id)
    if r is None:
        raise HTTPException(404, "run not found")
    if r.status not in ("failed", "retrying"):
        raise HTTPException(409, f"only failed runs can be retried (this run is {r.status})")
    p = session.get(Pipeline, r.pipeline_id)
    _pipeline(session, p.name)
    run = _enqueue(session, p, user, "retry", run_id)
    return {"run_id": run.id, "status": "queued", "pipeline": p.name, "retry_of": run_id}


@router.post("/runs/{run_id}/cancel")
def cancel(run_id: int, user: Principal = Depends(OPERATOR), session: Session = Depends(get_session)):
    r = session.get(PipelineRun, run_id, with_for_update=True)
    if r is None:
        raise HTTPException(404, "run not found")
    if r.status != "queued":
        raise HTTPException(409, f"only queued runs can be cancelled (this run is {r.status})")
    r.status, r.finished_at, r.error_message = "failed", _now(), f"cancelled by {user.username} before it started"
    audit.record(session, actor=user.username, action="pipeline.run.cancelled", resource_type="pipeline_run", resource_id=r.id)
    session.commit()
    return {"run_id": r.id, "status": r.status}


@router.post("/runs/{run_id}/abandon")
def abandon(run_id: int, user: Principal = Depends(ADMIN), session: Session = Depends(get_session)):
    """Recovery for a run stuck in 'running' after a crashed worker. If a worker is genuinely still running it, that
    worker will record its own final status; use this only for runs known to be dead."""
    r = session.get(PipelineRun, run_id, with_for_update=True)
    if r is None:
        raise HTTPException(404, "run not found")
    if r.status != "running":
        raise HTTPException(409, f"only running runs can be abandoned (this run is {r.status})")
    r.status, r.finished_at, r.error_message = "failed", _now(), f"abandoned by admin {user.username}"
    audit.record(session, actor=user.username, action="pipeline.run.abandoned", resource_type="pipeline_run",
                 resource_id=r.id, outcome="failure")
    session.commit()
    return {"run_id": r.id, "status": r.status}


@router.delete("/{name}/overrides/{param}")
def revert_override(name: str, param: str, user: Principal = Depends(ADMIN), session: Session = Depends(get_session)):
    p = _pipeline(session, name)
    if param not in OVERRIDABLE:
        raise HTTPException(422, f"param must be one of {OVERRIDABLE}")
    params = dict(p.params or {})
    if param not in params:
        raise HTTPException(404, "that override is not set")
    before = params.pop(param)
    p.params = params
    audit.record(session, actor=user.username, action="pipeline.override.reverted", resource_type="pipeline", resource_id=p.id,
                 detail={"pipeline": name, "param": param, "was": before})
    session.commit()
    return {"pipeline": name, "overrides": params}
