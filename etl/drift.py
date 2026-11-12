"""Distribution drift (Population Stability Index) and volume checks against stored baselines.

PSI = sum((actual_i - expected_i) * ln(actual_i / expected_i)).  Conventional reading:
  < 0.10 stable   0.10-0.25 moderate shift (warning)   >= 0.25 major shift (failed, alert)
"""
from __future__ import annotations

import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EPS = 1e-4
MAX_CATEGORIES = 50
OTHER = "__other__"


def psi(expected: list[float], actual: list[float]) -> float:
    total = 0.0
    for e, a in zip(expected, actual):
        e, a = max(e, EPS), max(a, EPS)
        total += (a - e) * math.log(a / e)
    return round(total, 6)


def _props(counts: dict, keys: list) -> list[float]:
    n = sum(counts.get(k, 0) for k in keys)
    return [counts.get(k, 0) / n if n else 0.0 for k in keys]


# ---------------------------------------------------------------- numeric
def _bin_counts(df: DataFrame, col: str, edges: list[float]) -> dict[int, int]:
    # bin index = how many edges the value exceeds (0..len(edges)); NULLs are ignored
    idx = sum((F.when(F.col(col).cast("double") > F.lit(e), 1).otherwise(0) for e in edges), F.lit(0))
    rows = df.where(F.col(col).isNotNull()).groupBy(idx.alias("b")).count().collect()
    return {r["b"]: r["count"] for r in rows}


def numeric_profile(df: DataFrame, col: str, bins: int = 10) -> dict:
    qs = [i / bins for i in range(1, bins)]
    raw = df.where(F.col(col).isNotNull()).approxQuantile(col, qs, 0.001)
    edges = sorted(set(round(float(x), 6) for x in raw))
    if len(edges) < 2:
        raise ValueError(f"column {col} has too few distinct values for numeric drift; use categorical")
    counts = _bin_counts(df, col, edges)
    keys = list(range(len(edges) + 1))
    return {"kind": "numeric", "edges": edges, "props": _props(counts, keys), "n": sum(counts.values())}


def numeric_psi(baseline: dict, df: DataFrame, col: str) -> tuple[float, dict]:
    counts = _bin_counts(df, col, baseline["edges"])
    keys = list(range(len(baseline["edges"]) + 1))
    actual = _props(counts, keys)
    return psi(baseline["props"], actual), {"n": sum(counts.values()), "actual_props": [round(x, 4) for x in actual]}


# ---------------------------------------------------------------- categorical
def _cat_counts(df: DataFrame, col: str) -> dict[str, int]:
    rows = df.groupBy(F.coalesce(F.col(col).cast("string"), F.lit("__null__")).alias("v")).count().collect()
    return {r["v"]: r["count"] for r in rows}


def categorical_profile(df: DataFrame, col: str) -> dict:
    counts = _cat_counts(df, col)
    top = dict(sorted(counts.items(), key=lambda kv: -kv[1])[:MAX_CATEGORIES])
    other = sum(v for k, v in counts.items() if k not in top)
    if other:
        top[OTHER] = other
    n = sum(top.values())
    return {"kind": "categorical", "props": {k: v / n for k, v in top.items()}, "n": n}


def categorical_psi(baseline: dict, df: DataFrame, col: str) -> tuple[float, dict]:
    counts = _cat_counts(df, col)
    known = set(baseline["props"]) - {OTHER}
    folded: dict[str, int] = {}
    for k, v in counts.items():
        key = k if k in known else OTHER
        folded[key] = folded.get(key, 0) + v
    keys = sorted(set(baseline["props"]) | set(folded))
    expected = [baseline["props"].get(k, 0.0) for k in keys]
    actual = _props(folded, keys)
    new_cats = sorted(k for k in counts if k not in known)[:10]
    return psi(expected, actual), {"n": sum(counts.values()), "new_categories": new_cats}


def profile(df: DataFrame, col: str, kind: str) -> dict:
    return numeric_profile(df, col) if kind == "numeric" else categorical_profile(df, col)


def compare(baseline: dict, df: DataFrame, col: str) -> tuple[float, dict]:
    return numeric_psi(baseline, df, col) if baseline["kind"] == "numeric" else categorical_psi(baseline, df, col)
