"""Volume + drift checks shared by both pipelines."""
from __future__ import annotations

from pyspark.sql import DataFrame

from etl import drift
from etl.common import PipelineConfig
from etl.dq import DQ, CheckResult

ROWS = "__rows__"


def volume_and_drift(ctx, dq: DQ, cfg: PipelineConfig, frames: dict[str, DataFrame],
                     specs: list[tuple[str, str, str]]) -> list[tuple[str, str, str, dict]]:
    """frames: silver DataFrames by dataset. specs: (dataset, column, 'numeric'|'categorical').
    Returns baselines to persist after the run succeeds (those that did not exist yet, or all when
    --reset-baseline was given)."""
    pending: list[tuple[str, str, str, dict]] = []
    for dataset, df in frames.items():
        rows = df.count()
        base = ctx.baselines.get((dataset, ROWS))
        if base is None or ctx.reset_baseline:
            pending.append((dataset, ROWS, "volume", {"kind": "volume", "rows": rows}))
            dq.add(CheckResult(dataset, "row_count_vs_baseline", "volume", "error", "passed", float(rows), None,
                               {"baseline": "created", "rows": rows}))
            continue
        ratio = rows / base["rows"] if base["rows"] else float("inf")
        if ratio < cfg.volume_min_ratio:
            status, sev = "failed", "error"
        elif ratio > cfg.volume_max_ratio:
            status, sev = "failed", "warn"
        else:
            status, sev = "passed", "error"
        dq.add(CheckResult(dataset, "row_count_vs_baseline", "volume", sev, status, round(ratio, 4),
                           cfg.volume_min_ratio, {"rows": rows, "baseline_rows": base["rows"],
                                                  "allowed_ratio": [cfg.volume_min_ratio, cfg.volume_max_ratio]}))
    for dataset, col, kind in specs:
        df = frames[dataset]
        base = ctx.baselines.get((dataset, col))
        if base is None or ctx.reset_baseline:
            pending.append((dataset, col, kind, drift.profile(df, col, kind)))
            dq.add(CheckResult(dataset, f"drift_{col}", "drift", "warn", "passed", None, cfg.drift_fail,
                               {"baseline": "created"}))
            continue
        score, extra = drift.compare(base, df, col)
        status = "failed" if score >= cfg.drift_fail else ("warning" if score >= cfg.drift_warn else "passed")
        dq.add(CheckResult(dataset, f"drift_{col}", "drift", "warn", status, score, cfg.drift_fail,
                           {"psi": score, "warn_at": cfg.drift_warn, "fail_at": cfg.drift_fail, **extra}))
    return pending
