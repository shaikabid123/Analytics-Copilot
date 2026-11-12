"""Pipeline 1 'sales': Postgres src.{customers,products,orders,order_items}
    -> bronze (raw snapshot + lineage columns) -> silver (clean, dedupe, validate, quarantine)
    -> gold.daily_sales, gold.customer_value   (published atomically, only if every gate passes)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl import monitor
from etl.common import (
    ContractError, DQGateError, PipelineConfig, blank_to_null, dedup, first_reason, publish_gold,
    read_table, rejects_frame, write_table,
)
from etl.dq import DQ

CONTRACTS = {
    "customers": {"customer_id": "integer", "first_name": "string", "last_name": "string", "email": "string",
                  "phone": "string", "city": "string", "country": "string", "segment": "string",
                  "signup_date": "date"},
    "products": {"product_id": "integer", "sku": "string", "name": "string", "category": "string",
                 "unit_price": "numeric", "unit_cost": "numeric", "is_active": "boolean", "created_date": "date"},
    "orders": {"order_id": "integer", "customer_id": "integer", "order_ts": "timestamp", "status": "string",
               "channel": "string", "total_amount": "numeric"},
    "order_items": {"order_item_id": "integer", "order_id": "integer", "product_id": "integer",
                    "quantity": "integer", "unit_price": "numeric"},
}
SEGMENTS = ["consumer", "small_business", "enterprise"]
STATUSES = ["processing", "shipped", "delivered", "cancelled", "returned"]
CHANNELS = ["web", "mobile", "store"]
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
DRIFT_SPECS = [("orders", "total_amount", "numeric"), ("order_items", "unit_price", "numeric"),
               ("order_items", "quantity", "categorical"), ("orders", "status", "categorical"),
               ("orders", "channel", "categorical"), ("customers", "segment", "categorical")]
LINEAGE = [
    ["src.customers", "bronze.sales_customers", "silver.customers", "gold.customer_value"],
    ["src.orders", "bronze.sales_orders", "silver.orders", "gold.daily_sales"],
    ["src.orders", "bronze.sales_orders", "silver.orders", "gold.customer_value"],
    ["src.order_items", "bronze.sales_order_items", "silver.order_items", "gold.daily_sales"],
    ["src.products", "bronze.sales_products", "silver.products", "(silver only)"]]


def default_config() -> PipelineConfig:
    return PipelineConfig(pipeline_name="sales")


# ----------------------------------------------------------------------------- silver cleaners
def _split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = df.cache()
    return df.filter("_reject_reason IS NULL"), df.filter("_reject_reason IS NOT NULL")


def clean_customers(b: DataFrame, cfg: PipelineConfig):
    cols = ["customer_id", "first_name", "last_name", "email", "phone", "city", "country", "segment", "signup_date"]
    d = dedup(b, ["customer_id"], cols)
    c = (d.withColumn("first_name", blank_to_null("first_name")).withColumn("last_name", blank_to_null("last_name"))
         .withColumn("email", F.lower(blank_to_null("email"))).withColumn("phone", blank_to_null("phone"))
         .withColumn("city", blank_to_null("city")).withColumn("country", blank_to_null("country"))
         .withColumn("segment", F.lower(F.trim("segment"))))
    reason = first_reason(
        (F.col("customer_id").isNull(), "null_key"),
        (F.col("email").isNull(), "missing_email"),
        (~F.col("email").rlike(EMAIL_RE), "invalid_email"),
        (F.col("first_name").isNull() | F.col("last_name").isNull(), "missing_name"),
        (F.col("segment").isNull() | ~F.col("segment").isin(SEGMENTS), "invalid_segment"),
        (F.col("signup_date").isNull() | (F.col("signup_date") > F.lit(cfg.as_of)), "bad_signup_date"))
    return c.withColumn("_reject_reason", reason), cols


def clean_products(b: DataFrame, cfg: PipelineConfig):
    cols = ["product_id", "sku", "name", "category", "unit_price", "unit_cost", "is_active", "created_date"]
    d = dedup(b, ["product_id"], cols)
    c = (d.withColumn("sku", F.upper(blank_to_null("sku"))).withColumn("name", blank_to_null("name"))
         .withColumn("category", blank_to_null("category"))
         .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
         .withColumn("unit_cost", F.col("unit_cost").cast("decimal(10,2)")))
    reason = first_reason(
        (F.col("product_id").isNull(), "null_key"),
        (F.col("sku").isNull(), "missing_sku"),
        (F.col("name").isNull() | F.col("category").isNull(), "missing_name_or_category"),
        (F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "non_positive_price"),
        (F.col("unit_cost").isNull() | (F.col("unit_cost") <= 0), "non_positive_cost"))
    return c.withColumn("_reject_reason", reason), cols


def clean_orders(b: DataFrame, cfg: PipelineConfig, customer_keys: DataFrame):
    cols = ["order_id", "customer_id", "order_ts", "status", "channel", "total_amount"]
    d = dedup(b, ["order_id"], cols)
    c = (d.withColumn("status", F.lower(F.trim("status"))).withColumn("channel", F.lower(F.trim("channel")))
         .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)")))
    c = c.join(customer_keys.select(F.col("customer_id").alias("_ck")), c.customer_id == F.col("_ck"), "left")
    reason = first_reason(
        (F.col("order_id").isNull(), "null_key"),
        (F.col("customer_id").isNull(), "missing_customer"),
        (F.col("status").isNull() | ~F.col("status").isin(STATUSES), "invalid_status"),
        (F.col("channel").isNull() | ~F.col("channel").isin(CHANNELS), "invalid_channel"),
        (F.col("order_ts").isNull() | (F.col("order_ts") > F.lit(cfg.as_of).cast("timestamp")), "bad_order_ts"),
        (F.col("total_amount").isNull() | (F.col("total_amount") < 0), "bad_total"),
        (F.col("_ck").isNull(), "orphan_customer"))
    return c.withColumn("_reject_reason", reason).drop("_ck"), cols


def clean_items(b: DataFrame, cfg: PipelineConfig, order_keys: DataFrame, product_keys: DataFrame):
    cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]
    d = dedup(b, ["order_item_id"], cols)
    c = d.withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    c = c.join(order_keys.select(F.col("order_id").alias("_ok")), c.order_id == F.col("_ok"), "left")
    c = c.join(product_keys.select(F.col("product_id").alias("_pk")), c.product_id == F.col("_pk"), "left")
    reason = first_reason(
        (F.col("order_item_id").isNull(), "null_key"),
        (F.col("order_id").isNull() | F.col("product_id").isNull(), "missing_reference"),
        (F.col("quantity").isNull() | (F.col("quantity") <= 0), "non_positive_quantity"),
        (F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "non_positive_price"),
        (F.col("_ok").isNull(), "orphan_order"),
        (F.col("_pk").isNull(), "orphan_product"))
    return c.withColumn("_reject_reason", reason).drop("_ok", "_pk"), cols


# ----------------------------------------------------------------------------- pipeline
def run(spark: SparkSession, cfg: PipelineConfig, ctx):
    dq = DQ()
    try:
        return _run(spark, cfg, ctx, dq)
    finally:
        dq.flush(ctx)


def _run(spark: SparkSession, cfg: PipelineConfig, ctx, dq: DQ):
    stages: dict[str, float] = {}
    t = time.time()
    ingested = datetime.now(timezone.utc)

    # ---------------- BRONZE: read raw, verify contract BEFORE writing anything
    raw = {name: read_table(spark, cfg.src_tables[name], role="source") for name in CONTRACTS}
    for name, df in raw.items():
        dq.contract(df, f"src.{name}", CONTRACTS[name])
    broken = [r for r in dq.results if r.check_type == "contract" and r.status == "failed"]
    if broken:
        dq.flush(ctx)
        raise ContractError("; ".join(f"{r.dataset}: missing={r.detail['missing_columns']} "
                                      f"type_mismatch={r.detail['type_mismatch']}" for r in broken))
    bronze_rows: dict[str, int] = {}
    for name, df in raw.items():
        tagged = (df.withColumn("_run_id", F.lit(ctx.run_id)).withColumn("_ingested_at", F.lit(ingested))
                  .withColumn("_source", F.lit(cfg.src_tables[name])))
        write_table(tagged, cfg.out("bronze", f"sales_{name}"))
    bronze = {name: read_table(spark, cfg.out("bronze", f"sales_{name}")).cache() for name in CONTRACTS}
    for name, df in bronze.items():
        bronze_rows[name] = df.count()
    stages["bronze_s"] = round(time.time() - t, 1)

    # ---------------- SILVER: clean -> dedupe -> validate -> quarantine
    t = time.time()
    silver: dict[str, DataFrame] = {}
    rejects: list[DataFrame] = []
    stats: dict[str, dict] = {}

    def finish(name: str, cleaned: DataFrame, cols: list[str], key: str, drop_extra: list[str] | None = None):
        good, bad = _split(cleaned)
        deduped_n = good.count() + bad.count()
        removed = bronze_rows[name] - deduped_n
        reasons = {r["_reject_reason"]: r["count"] for r in bad.groupBy("_reject_reason").count().collect()}
        n_bad = sum(reasons.values())
        keep_cols = [c for c in cols if c not in (drop_extra or [])]
        out = good.select(*keep_cols).withColumn("_run_id", F.lit(ctx.run_id)).cache()
        silver[name] = out
        rejects.append(rejects_frame(bad, name, key, cols, ctx.run_id))
        stats[name] = {"bronze": bronze_rows[name], "duplicates_removed": removed,
                       "rejected": n_bad, "silver": deduped_n - n_bad, "reject_reasons": reasons}
        dq.duplicates(name, removed)
        dq.reject_rate(name, deduped_n, n_bad, cfg.max_reject_pct, reasons)

    c, cols = clean_customers(bronze["customers"], cfg); finish("customers", c, cols, "customer_id")
    c, cols = clean_products(bronze["products"], cfg); finish("products", c, cols, "product_id")
    c, cols = clean_orders(bronze["orders"], cfg, silver["customers"]); finish("orders", c, cols, "order_id")
    c, cols = clean_items(bronze["order_items"], cfg, silver["orders"], silver["products"])
    finish("order_items", c, cols, "order_item_id")

    for name, df in silver.items():
        write_table(df, cfg.out("silver", name))
    all_rejects = rejects[0]
    for r in rejects[1:]:
        all_rejects = all_rejects.unionByName(r)
    write_table(all_rejects, cfg.out("silver", "sales_rejects"))

    # invariants on the Silver output (post-conditions: must hold for anything we publish)
    keys = {"customers": "customer_id", "products": "product_id", "orders": "order_id", "order_items": "order_item_id"}
    for name, key in keys.items():
        df = silver[name]
        dq.invariant(name, f"{key}_not_null", "not_null", df.filter(F.col(key).isNull()).count())
        dq.invariant(name, f"{key}_unique", "uniqueness", df.count() - df.select(key).distinct().count())
    dq.invariant("orders", "customer_exists", "referential",
                 silver["orders"].join(silver["customers"], "customer_id", "left_anti").count())
    dq.invariant("order_items", "order_exists", "referential",
                 silver["order_items"].join(silver["orders"], "order_id", "left_anti").count())
    dq.invariant("order_items", "product_exists", "referential",
                 silver["order_items"].join(silver["products"], "product_id", "left_anti").count())
    dup_emails = (silver["customers"].groupBy("email").count().filter("count > 1").count())
    dq.invariant("customers", "email_unique", "uniqueness", dup_emails, severity="warn")
    dq.invariant("products", "cost_below_price", "validity",
                 silver["products"].filter(F.col("unit_cost") >= F.col("unit_price")).count(), severity="warn")
    sums = silver["order_items"].groupBy("order_id").agg(F.sum(F.col("quantity") * F.col("unit_price")).alias("s"))
    n_orders = silver["orders"].count()
    mism = (silver["orders"].join(sums, "order_id", "left")
            .filter(F.abs(F.col("total_amount") - F.coalesce(F.col("s"), F.lit(0))) > 0.01).count())
    dq.invariant("orders", "total_matches_items", "reconciliation", mism, severity="warn",
                 threshold=cfg.max_total_mismatch_pct / 100 * n_orders,
                 detail={"mismatch_pct": round(100 * mism / n_orders, 4) if n_orders else 0})
    pending = monitor.volume_and_drift(ctx, dq, cfg, silver, DRIFT_SPECS)
    stages["silver_s"] = round(time.time() - t, 1)

    dq.flush(ctx)
    if dq.blocking():                                     # GATE 1: nothing reaches Gold
        raise DQGateError("data-quality gate failed, Gold not published: " + dq.gate_message())

    # ---------------- GOLD: build staged tables, reconcile against Silver, publish atomically
    t = time.time()
    orders, items, customers = silver["orders"], silver["order_items"], silver["customers"]
    net = F.when(F.col("status").isin("cancelled", "returned"), F.lit(0)).otherwise(F.col("total_amount"))
    lines = items.groupBy("order_id").agg(F.count("*").alias("lines"), F.sum("quantity").alias("units"))
    o = orders.join(lines, "order_id", "left").withColumn("order_date", F.to_date("order_ts")).withColumn("net", net)
    daily = (o.groupBy("order_date", "channel").agg(
        F.count("*").alias("orders"), F.coalesce(F.sum("lines"), F.lit(0)).alias("items"),
        F.coalesce(F.sum("units"), F.lit(0)).alias("units"),
        F.sum("total_amount").cast("decimal(16,2)").alias("gross_merchandise_value"),
        F.sum("net").cast("decimal(16,2)").alias("net_revenue"),
        F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("status") == "returned", 1).otherwise(0)).alias("returned_orders"))
        .withColumn("avg_order_value", (F.col("gross_merchandise_value") / F.col("orders")).cast("decimal(12,2)"))
        .withColumn("return_rate", F.round(F.col("returned_orders") / F.col("orders"), 4))
        .withColumn("_run_id", F.lit(ctx.run_id)))
    max_day = o.agg(F.max("order_date")).collect()[0][0]
    per_cust = o.groupBy("customer_id").agg(
        F.count("*").alias("orders"), F.sum("net").cast("decimal(16,2)").alias("lifetime_value"),
        F.min("order_date").alias("first_order_date"), F.max("order_date").alias("last_order_date"))
    cv = (customers.select("customer_id", "segment", "country").join(per_cust, "customer_id", "left")
          .withColumn("orders", F.coalesce(F.col("orders"), F.lit(0)))
          .withColumn("lifetime_value", F.coalesce(F.col("lifetime_value"), F.lit(0)).cast("decimal(16,2)"))
          .withColumn("avg_order_value", F.when(F.col("orders") > 0, (F.col("lifetime_value") / F.col("orders"))
                                                .cast("decimal(12,2)")))
          .withColumn("is_active_90d", F.coalesce(F.col("last_order_date") >= F.date_sub(F.lit(max_day), 90), F.lit(False)))
          .withColumn("_run_id", F.lit(ctx.run_id)))
    write_table(daily, cfg.stage("daily_sales"))
    write_table(cv, cfg.stage("customer_value"))
    g_daily, g_cv = read_table(spark, cfg.stage("daily_sales")), read_table(spark, cfg.stage("customer_value"))

    s_orders, s_gmv = orders.count(), orders.agg(F.sum("total_amount")).collect()[0][0] or 0
    gd = g_daily.agg(F.sum("orders"), F.sum("gross_merchandise_value")).collect()[0]
    dq.invariant("gold.daily_sales", "orders_match_silver", "reconciliation", abs((gd[0] or 0) - s_orders),
                 detail={"gold": gd[0], "silver": s_orders})
    dq.invariant("gold.daily_sales", "gmv_matches_silver", "reconciliation",
                 float(abs((gd[1] or 0) - s_gmv)), threshold=0.01, detail={"gold": str(gd[1]), "silver": str(s_gmv)})
    cvs = g_cv.agg(F.count("*"), F.sum("orders")).collect()[0]
    dq.invariant("gold.customer_value", "customers_match_silver", "reconciliation",
                 abs(cvs[0] - customers.count()), detail={"gold": cvs[0], "silver": customers.count()})
    dq.invariant("gold.customer_value", "orders_match_silver", "reconciliation", abs((cvs[1] or 0) - s_orders))
    dq.flush(ctx)
    if dq.blocking():                                     # GATE 2: reconciliation
        raise DQGateError("reconciliation failed, Gold not published: " + dq.gate_message())

    gold_rows = {"daily_sales": g_daily.count(), "customer_value": g_cv.count()}
    publish_gold([(cfg.out("gold", "daily_sales"), cfg.stage("daily_sales")),
                  (cfg.out("gold", "customer_value"), cfg.stage("customer_value"))])
    stages["gold_s"] = round(time.time() - t, 1)
    ctx.persist_baselines(pending)

    from etl.runner import PipelineResult
    failed = [r for r in dq.results if r.status == "failed"]
    return PipelineResult(
        rows_in=sum(bronze_rows.values()), rows_out=sum(gold_rows.values()),
        details={"bronze_rows": bronze_rows, "silver": stats, "gold_rows": gold_rows, "stages": stages,
                 "lineage": LINEAGE, "dq": {"checks": len(dq.results), "failed_non_blocking": len(failed),
                                            "warnings": len([r for r in dq.results if r.status == "warning"])},
                 "baselines_created": len(pending)})
