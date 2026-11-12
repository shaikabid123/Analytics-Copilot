"""Unit tests for the DQ engine and drift maths on tiny in-memory DataFrames (no database)."""
from datetime import date

import pytest
from pyspark.sql import functions as F

from etl import drift
from etl.common import blank_to_null, dedup, first_reason
from etl.dq import DQ, type_family


def test_type_family_mapping():
    assert [type_family(t) for t in ("bigint", "int", "decimal(10,2)", "timestamp_ntz", "string", "double")] == \
        ["integer", "integer", "decimal", "timestamp", "string", "float"]


def test_contract_passes_flags_missing_type_mismatch_and_unexpected(spark):
    df = spark.createDataFrame([(1, "a", 2.5)], "id long, name string, amount double")
    dq = DQ()
    assert dq.contract(df, "t", {"id": "integer", "name": "string", "amount": "numeric"}).status == "passed"
    missing = dq.contract(df, "t", {"id": "integer", "name": "string", "amount": "numeric", "channel": "string"})
    assert missing.status == "failed" and missing.detail["missing_columns"] == ["channel"]
    wrong = dq.contract(df, "t", {"id": "string", "name": "string", "amount": "numeric"})
    assert wrong.status == "failed" and wrong.detail["type_mismatch"]["id"] == {"expected": "string", "actual": "integer"}
    extra = dq.contract(df, "t", {"id": "integer", "name": "string"})
    assert extra.status == "warning" and extra.detail["unexpected_columns"] == ["amount"]
    assert len(dq.blocking()) == 2                      # missing + wrong type are blocking (error severity)


def test_dedup_keeps_most_complete_row_deterministically(spark):
    df = spark.createDataFrame([(1, "a@x.com", None), (1, "a@x.com", "555"), (1, "a@x.com", None), (2, "b@x.com", None)],
                               "id int, email string, phone string")
    out = {r["id"]: r["phone"] for r in dedup(df, ["id"], ["id", "email", "phone"]).collect()}
    assert out == {1: "555", 2: None}


def test_first_reason_is_null_safe_and_first_match_wins(spark):
    df = spark.createDataFrame([(None, 5), (3, -1), (3, 5), (None, -1)], "a int, b int")
    reason = first_reason((F.col("a").isNull(), "a_null"), (F.col("b").isNull() | (F.col("b") <= 0), "b_bad"))
    got = [r["r"] for r in df.select(reason.alias("r")).collect()]
    assert got == ["a_null", "b_bad", None, "a_null"]    # NULL handled, first rule wins


def test_blank_to_null_trims(spark):
    df = spark.createDataFrame([("  x ",), ("   ",), ("",), (None,)], "v string")
    assert [r["v"] for r in df.select(blank_to_null("v").alias("v")).collect()] == ["x", None, None, None]


def test_psi_basics():
    assert drift.psi([0.5, 0.5], [0.5, 0.5]) == 0
    assert drift.psi([0.5, 0.5], [0.9, 0.1]) > 0.25
    assert drift.psi([0.5, 0.5], [0.0, 1.0]) > 0.25     # empty bucket handled by epsilon, no crash


def test_numeric_drift_detects_shift_and_ignores_resampling(spark):
    base = spark.range(1000).select((F.col("id") % 100).cast("double").alias("v"))
    prof = drift.numeric_profile(base, "v")
    same, _ = drift.numeric_psi(prof, base, "v")
    shifted = base.select((F.col("v") + 60).alias("v"))
    moved, _ = drift.numeric_psi(prof, shifted, "v")
    assert same < 0.01 and moved > 0.25


def test_categorical_drift_new_category_and_proportion_change(spark):
    base = spark.createDataFrame([("web",)] * 60 + [("mobile",)] * 40, "c string")
    prof = drift.categorical_profile(base, "c")
    assert drift.categorical_psi(prof, base, "c")[0] < 0.01
    flipped = spark.createDataFrame([("web",)] * 10 + [("mobile",)] * 90, "c string")
    assert drift.categorical_psi(prof, flipped, "c")[0] > 0.25
    new_cat = spark.createDataFrame([("web",)] * 50 + [("kiosk",)] * 50, "c string")
    score, extra = drift.categorical_psi(prof, new_cat, "c")
    assert score > 0.25 and extra["new_categories"] == ["kiosk"]


def test_numeric_profile_refuses_constant_column(spark):
    with pytest.raises(ValueError, match="too few distinct"):
        drift.numeric_profile(spark.createDataFrame([(1.0,)] * 20, "v double"), "v")
