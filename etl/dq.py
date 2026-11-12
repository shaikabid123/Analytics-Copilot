"""Data-quality engine: check results, schema contracts, row-level reject summaries.

Severity semantics:  error = failing blocks Gold publication.  warn = alert only, run continues.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame

_FAMILY = {"tinyint": "integer", "smallint": "integer", "int": "integer", "bigint": "integer",
           "float": "float", "double": "float", "string": "string", "boolean": "boolean",
           "date": "date", "timestamp": "timestamp", "timestamp_ntz": "timestamp"}


def type_family(spark_type: str) -> str:
    if spark_type.startswith("decimal"):
        return "decimal"
    return _FAMILY.get(spark_type, spark_type)


def _compatible(expected: str, actual_family: str) -> bool:
    if expected == "numeric":
        return actual_family in ("integer", "decimal", "float")
    return expected == actual_family


@dataclass
class CheckResult:
    dataset: str
    check_name: str
    check_type: str
    severity: str            # error | warn
    status: str              # passed | warning | failed
    observed: float | None = None
    threshold: float | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class DQ:
    def __init__(self) -> None:
        self.results: list[CheckResult] = []
        self._saved = 0

    def add(self, r: CheckResult) -> CheckResult:
        self.results.append(r)
        return r

    def blocking(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == "error" and r.status == "failed"]

    def flush(self, ctx) -> None:
        """Persist results not yet saved (called in `finally` so failures keep their evidence)."""
        pending = self.results[self._saved:]
        if pending:
            ctx.save_results(pending)
            self._saved = len(self.results)

    def gate_message(self) -> str:
        return "; ".join(
            f"{r.dataset}.{r.check_name} (observed={r.observed}, threshold={r.threshold})" for r in self.blocking())

    # ------------------------------------------------------------------ checks
    def contract(self, df: DataFrame, dataset: str, expected: dict[str, str]) -> CheckResult:
        actual = {name: type_family(t) for name, t in df.dtypes}
        missing = sorted(set(expected) - set(actual))
        mismatch = {c: {"expected": e, "actual": actual[c]} for c, e in expected.items()
                    if c in actual and not _compatible(e, actual[c])}
        unexpected = sorted(c for c in actual if c not in expected and not c.startswith("_"))
        failed = bool(missing or mismatch)
        return self.add(CheckResult(
            dataset, "schema_contract", "contract", "error",
            "failed" if failed else ("warning" if unexpected else "passed"),
            float(len(missing) + len(mismatch)), 0.0,
            {"missing_columns": missing, "type_mismatch": mismatch, "unexpected_columns": unexpected}))

    def reject_rate(self, dataset: str, total: int, rejected: int, max_pct: float,
                    reasons: dict[str, int]) -> CheckResult:
        pct = round(100.0 * rejected / total, 4) if total else 0.0
        return self.add(CheckResult(dataset, "reject_rate", "validity", "error",
                                    "failed" if pct > max_pct else "passed", pct, max_pct,
                                    {"rejected": rejected, "evaluated": total, "reasons": reasons}))

    def duplicates(self, dataset: str, removed: int) -> CheckResult:
        return self.add(CheckResult(dataset, "duplicate_keys_removed", "uniqueness", "warn",
                                    "failed" if removed > 0 else "passed", float(removed), 0.0,
                                    {"rule": "keep most complete row; tie-break by smallest JSON"}))

    def invariant(self, dataset: str, name: str, check_type: str, violations: int,
                  severity: str = "error", threshold: float = 0.0, detail: dict | None = None) -> CheckResult:
        return self.add(CheckResult(dataset, name, check_type, severity,
                                    "failed" if violations > threshold else "passed",
                                    float(violations), threshold, detail or {}))
