"""Pipeline runner: run lifecycle, bounded retry with exponential backoff, failure classification, CLI.

  python -m etl.runner run sales|events [--retries N] [--backoff S] [--reset-baseline] [--simulate-transient N]
  python -m etl.runner retry RUN_ID
  python -m etl.runner status [--limit N]
  python -m etl.runner diagnose RUN_ID
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Callable

from sqlalchemy import select

from etl import runlog
from etl.common import PipelineConfig, PipelineError, TransientError, classify
from etl.runlog import RunContext
from app.models import DqAlert, DqResult, Pipeline, PipelineRun


@dataclass
class PipelineResult:
    rows_in: int
    rows_out: int
    details: dict


@dataclass
class Outcome:
    status: str                 # succeeded | failed
    run_ids: list[int]
    error: str | None = None
    attempts: int = 1


def apply_overrides(cfg: PipelineConfig, params: dict) -> dict:
    """Apply engineer-approved overrides (written only by the approval flow). Values are re-validated here
    (defence in depth): anything out of range is ignored."""
    applied: dict = {}
    mrp = params.get("max_reject_pct")
    if isinstance(mrp, (int, float)) and not isinstance(mrp, bool) and 0 < mrp <= 10:
        cfg.max_reject_pct = float(mrp)
        applied["max_reject_pct"] = float(mrp)
    if params.get("reset_baseline_next_run") is True:
        applied["reset_baseline_next_run"] = True
    return applied


def execute(cfg: PipelineConfig, run_fn: Callable, *, spark=None, trigger: str = "manual",
            retry_of: int | None = None, max_retries: int | None = None, backoff_s: float = 5.0,
            reset_baseline: bool = False, simulate_transient: int = 0, sleep=time.sleep,
            first_run_id: int | None = None) -> Outcome:
    """Run a pipeline with bounded retries. Only retryable (transient) failures are retried; contract
    and data-quality failures are final because another attempt would see the same data."""
    pipeline = runlog.ensure_pipeline(cfg.pipeline_name)
    runlog.claim_pipeline(pipeline.id, cfg.pipeline_name, exclude_run_id=first_run_id)
    retries = pipeline.max_retries if max_retries is None else max_retries
    overrides = apply_overrides(cfg, pipeline.params or {})
    if overrides.get("reset_baseline_next_run"):
        reset_baseline = True
    run_ids: list[int] = []
    attempt = 1
    while True:
        if attempt == 1 and first_run_id is not None:         # a queued run the worker already claimed
            run_id = first_run_id
        else:
            run_id = runlog.start_run(pipeline.id, cfg.pipeline_name, trigger if attempt == 1 else "retry",
                                      attempt, retry_of if attempt == 1 else run_ids[-1])
        run_ids.append(run_id)
        ctx = RunContext(run_id, pipeline.id, cfg.pipeline_name, reset_baseline)
        try:
            ctx.load_baselines()
            if attempt <= simulate_transient:
                raise TransientError(f"[simulated] database connection lost (attempt {attempt})")
            result: PipelineResult = run_fn(spark, cfg, ctx)
            if overrides:
                result.details["overrides"] = overrides
            runlog.finish_run(run_id, "succeeded", rows_in=result.rows_in, rows_out=result.rows_out,
                              details=result.details)
            if overrides.get("reset_baseline_next_run"):       # one-shot: consumed only after a successful run
                runlog.clear_param(pipeline.id, "reset_baseline_next_run")
            return Outcome("succeeded", run_ids, None, attempt)
        except Exception as exc:  # noqa: BLE001 - every failure must be recorded
            err = classify(exc)
            retryable = err is not None and err.retryable
            message = f"{type(err or exc).__name__}: {err or exc}"[:1800]
            will_retry = retryable and attempt <= retries
            runlog.finish_run(run_id, "retrying" if will_retry else "failed", error=message,
                              details={"traceback_tail": traceback.format_exc()[-1500:],
                                       "retryable": retryable, "attempt": attempt, "max_retries": retries})
            if not will_retry:
                return Outcome("failed", run_ids, message, attempt)
            sleep(backoff_s * (2 ** (attempt - 1)))
            attempt += 1


# ------------------------------------------------------------------------------ CLI
def _load(name: str):
    """Import only the requested pipeline (keeps startup light and failures isolated)."""
    import importlib
    mod = importlib.import_module(f"etl.pipelines.{name}")
    return mod.default_config, mod.run


def _print_status(limit: int) -> None:
    with runlog.session() as s:
        rows = s.execute(select(PipelineRun, Pipeline.name).join(Pipeline, Pipeline.id == PipelineRun.pipeline_id)
                         .order_by(PipelineRun.id.desc()).limit(limit)).all()
    print(f"{'run':>5} {'pipeline':<10} {'status':<10} {'try':>3} {'trigger':<9} {'rows_in':>9} {'rows_out':>9}  error")
    for run, name in rows:
        print(f"{run.id:>5} {name:<10} {run.status:<10} {run.attempt:>3} {run.trigger:<9} "
              f"{run.rows_in or '':>9} {run.rows_out or '':>9}  {(run.error_message or '')[:70]}")


def _diagnose(run_id: int) -> int:
    with runlog.session() as s:
        run = s.get(PipelineRun, run_id)
        if run is None:
            print(f"error: no run {run_id}", file=sys.stderr)
            return 1
        name = s.get(Pipeline, run.pipeline_id).name
        secs = (run.finished_at - run.started_at).total_seconds() if run.finished_at and run.started_at else None
        print(f"run {run.id}: pipeline={name} status={run.status} attempt={run.attempt} trigger={run.trigger}"
              f" retry_of={run.retry_of_run_id} duration_s={secs and round(secs, 1)}")
        if run.error_message:
            print(f"error: {run.error_message}")
        d = run.details or {}
        if "retryable" in d:
            print(f"retryable={d['retryable']} (max_retries={d.get('max_retries')})")
        results = s.scalars(select(DqResult).where(DqResult.run_id == run_id).order_by(DqResult.id)).all()
        bad = [r for r in results if r.status != "passed"]
        print(f"checks: {len(results)} total, {len([r for r in results if r.status == 'passed'])} passed, {len(bad)} not passed")
        for r in bad:
            print(f"  [{r.status.upper():<7}] {r.severity:<5} {r.dataset}.{r.check_name} observed={r.observed} threshold={r.threshold}")
            reasons = r.detail.get("reasons")
            if reasons:
                print(f"            reject reasons: {reasons}")
        print(f"alerts: {len(s.scalars(select(DqAlert).where(DqAlert.run_id == run_id)).all())} raised")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="etl.runner")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run"); p.add_argument("pipeline", choices=["sales", "events"])
    p.add_argument("--retries", type=int); p.add_argument("--backoff", type=float, default=5.0)
    p.add_argument("--reset-baseline", action="store_true")
    p.add_argument("--simulate-transient", type=int, default=0, help="DEMO ONLY: fail the first N attempts")
    p = sub.add_parser("retry"); p.add_argument("run_id", type=int)
    p = sub.add_parser("status"); p.add_argument("--limit", type=int, default=10)
    p = sub.add_parser("diagnose"); p.add_argument("run_id", type=int)
    a = ap.parse_args(argv)

    if a.cmd == "status":
        _print_status(a.limit); return 0
    if a.cmd == "diagnose":
        return _diagnose(a.run_id)

    if a.cmd == "retry":
        with runlog.session() as s:
            run = s.get(PipelineRun, a.run_id)
            if run is None or run.status not in ("failed", "retrying"):
                print("error: run not found or not in a failed state", file=sys.stderr); return 1
            name = s.get(Pipeline, run.pipeline_id).name
        kwargs = dict(trigger="retry", retry_of=a.run_id)
    else:
        name, kwargs = a.pipeline, dict(retry_of=None, max_retries=a.retries, backoff_s=a.backoff,
                                         reset_baseline=a.reset_baseline, simulate_transient=a.simulate_transient)
    make_cfg, run_fn = _load(name)
    from etl.common import get_spark
    spark = get_spark(f"etl-{name}")
    try:
        out = execute(make_cfg(), run_fn, spark=spark, **kwargs)
    except PipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    print(f"{name}: {out.status} after {out.attempts} attempt(s); run ids {out.run_ids}")
    try:                                    # deliver this run's alerts; an integration outage never fails the run
        from app.integrations.dispatch import dispatch_pending_alerts
        delivered = dispatch_pending_alerts(only_runs=out.run_ids)
        if delivered["configured"]:
            print(f"alerts: {delivered}")
    except Exception as exc:  # noqa: BLE001
        print(f"alert dispatch skipped: {type(exc).__name__}", file=sys.stderr)
    if out.error:
        print(f"error: {out.error}", file=sys.stderr)
    return 0 if out.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
