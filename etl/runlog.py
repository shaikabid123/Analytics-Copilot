"""Run metadata in the app schema (written as app_rw): runs, DQ results, alerts, baselines, audit."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app import audit
from app.db import get_engine
from app.models import DqAlert, DqResult, DriftBaseline, Pipeline, PipelineRun
from etl.dq import CheckResult


def session() -> Session:
    return Session(get_engine())


def ensure_pipeline(name: str) -> Pipeline:
    with session() as s:
        p = s.scalar(select(Pipeline).where(Pipeline.name == name))
        if p is None:
            p = Pipeline(name=name, description="(registered by runner)")
            s.add(p)
            s.commit()
        s.refresh(p)
        s.expunge(p)
        return p


def clear_param(pipeline_id: int, key: str) -> None:
    with session() as s:
        p = s.get(Pipeline, pipeline_id)
        params = dict(p.params or {})
        if params.pop(key, None) is not None:
            p.params = params
            audit.record(s, actor="etl", action="pipeline.param.consumed", resource_type="pipeline",
                         resource_id=pipeline_id, detail={"param": key})
            s.commit()


STALE_AFTER = timedelta(minutes=60)


def claim_pipeline(pipeline_id: int, name: str, stale_after: timedelta = STALE_AFTER,
                   exclude_run_id: int | None = None) -> None:
    """Concurrency guard. Runs stuck in 'running' longer than `stale_after` are assumed to have crashed
    and are marked failed (abandoned); if a genuinely active run remains, refuse to start another."""
    from etl.common import PipelineError
    now = datetime.now(timezone.utc)
    with session() as s:
        reaped = s.execute(update(PipelineRun).where(
            PipelineRun.pipeline_id == pipeline_id, PipelineRun.status == "running",
            PipelineRun.started_at < now - stale_after)
            .values(status="failed", finished_at=now,
                    error_message="abandoned: runner stopped without finishing (reaped by a later start)")
            .returning(PipelineRun.id)).scalars().all()
        for rid in reaped:
            audit.record(s, actor="etl", action="pipeline.run.abandoned", resource_type="pipeline_run",
                         resource_id=rid, outcome="failure", detail={"pipeline": name})
        stmt = select(PipelineRun.id).where(PipelineRun.pipeline_id == pipeline_id, PipelineRun.status == "running")
        if exclude_run_id is not None:               # the run this worker just claimed is not "another" run
            stmt = stmt.where(PipelineRun.id != exclude_run_id)
        active = s.scalar(stmt)
        s.commit()
    if active is not None:
        raise PipelineError(f"pipeline '{name}' is already running (run {active}); refusing to start a second run")


def claim_queued_run(only_pipelines: list[str] | None = None) -> dict | None:
    """Atomically take the oldest queued run (FOR UPDATE SKIP LOCKED: concurrent workers never get the same one)."""
    from sqlalchemy import text
    with session() as s:
        scope = "AND pipeline_id IN (SELECT id FROM app.pipelines WHERE name = ANY(:names)) " if only_pipelines is not None else ""
        stmt = text("UPDATE app.pipeline_runs SET status = 'running', started_at = now() WHERE id = ("
                    f"  SELECT id FROM app.pipeline_runs WHERE status = 'queued' {scope}ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) "
                    "RETURNING id, pipeline_id, trigger, retry_of_run_id")
        row = s.execute(stmt, {"names": only_pipelines} if only_pipelines is not None else {}).first()
        if row is None:
            s.commit()
            return None
        name = s.get(Pipeline, row.pipeline_id).name
        audit.record(s, actor="etl-worker", action="pipeline.run.start", resource_type="pipeline_run", resource_id=row.id,
                     detail={"pipeline": name, "trigger": row.trigger, "queued": True})
        s.commit()
        return {"id": row.id, "pipeline_id": row.pipeline_id, "pipeline": name, "trigger": row.trigger, "retry_of": row.retry_of_run_id}


def start_run(pipeline_id: int, name: str, trigger: str, attempt: int, retry_of: int | None) -> int:
    with session() as s:
        run = PipelineRun(pipeline_id=pipeline_id, status="running", trigger=trigger, attempt=attempt,
                          retry_of_run_id=retry_of, started_at=datetime.now(timezone.utc))
        s.add(run)
        s.flush()
        audit.record(s, actor="etl", action="pipeline.run.start", resource_type="pipeline_run",
                     resource_id=run.id, detail={"pipeline": name, "attempt": attempt, "trigger": trigger})
        s.commit()
        return run.id


def finish_run(run_id: int, status: str, *, rows_in: int | None = None, rows_out: int | None = None,
               error: str | None = None, details: dict | None = None) -> None:
    with session() as s:
        run = s.get(PipelineRun, run_id)
        run.status, run.finished_at = status, datetime.now(timezone.utc)
        run.rows_in, run.rows_out = rows_in, rows_out
        run.error_message = (error or "")[:2000] or None
        if details is not None:
            run.details = details
        audit.record(s, actor="etl", action=f"pipeline.run.{status}", resource_type="pipeline_run",
                     resource_id=run_id, outcome="success" if status == "succeeded" else "failure",
                     detail={"error": (error or "")[:300]} if error else {})
        s.commit()


@dataclass
class RunContext:
    run_id: int
    pipeline_id: int
    pipeline_name: str
    reset_baseline: bool = False
    baselines: dict[tuple[str, str], dict] = field(default_factory=dict)

    def load_baselines(self) -> None:
        with session() as s:
            rows = s.scalars(select(DriftBaseline).where(DriftBaseline.pipeline_id == self.pipeline_id))
            self.baselines = {(b.dataset, b.column_name): b.baseline for b in rows}

    def save_results(self, results: list[CheckResult]) -> None:
        with session() as s:
            for r in results:
                row = DqResult(run_id=self.run_id, dataset=r.dataset, check_name=r.check_name,
                               check_type=r.check_type, severity=r.severity, status=r.status,
                               observed=r.observed, threshold=r.threshold, detail=r.detail)
                s.add(row)
                s.flush()
                if r.status in ("failed", "warning"):
                    sev = ("high" if r.severity == "error" else "medium") if r.status == "failed" else "low"
                    s.add(DqAlert(run_id=self.run_id, dq_result_id=row.id, severity=sev,
                                  title=f"[{self.pipeline_name}] {r.dataset}.{r.check_name} {r.status}",
                                  detail={"observed": r.observed, "threshold": r.threshold, **r.detail}))
            s.commit()

    def persist_baselines(self, items: list[tuple[str, str, str, dict]]) -> None:
        """items: (dataset, column, kind, baseline). Called only after a fully successful run so a
        bad first batch can never become the reference."""
        with session() as s:
            for dataset, col, kind, base in items:
                row = s.scalar(select(DriftBaseline).where(
                    DriftBaseline.pipeline_id == self.pipeline_id, DriftBaseline.dataset == dataset,
                    DriftBaseline.column_name == col))
                if row is None:
                    s.add(DriftBaseline(pipeline_id=self.pipeline_id, dataset=dataset, column_name=col,
                                        kind=kind, baseline=base, created_run_id=self.run_id))
                else:
                    row.kind, row.baseline, row.created_run_id = kind, base, self.run_id
            s.commit()
