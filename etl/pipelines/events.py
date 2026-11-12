"""Pipeline 2 'events': web_events.csv + shipments.parquet (files)
    -> bronze (raw, CSV kept as strings) -> silver (typed, deduped, validated, quarantined)
    -> gold.funnel_daily, gold.delivery_sla
Referential checks use ID columns read from the Postgres source (read-only), so this pipeline does not
depend on the sales pipeline having run.
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

EVENT_COLS = ["event_id", "session_id", "customer_id", "event_type", "product_id", "event_ts", "device"]
CONTRACTS = {
    "web_events": {c: "string" for c in EVENT_COLS},       # CSV is schema-on-read: every column arrives as text
    "shipments": {"shipment_id": "integer", "order_id": "integer", "carrier": "string",
                  "shipped_at": "timestamp", "delivered_at": "timestamp", "status": "string"},
}
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout_start", "purchase"]
DEVICES = ["desktop", "mobile", "tablet"]
SHIP_STATUSES = ["shipped", "delivered", "returned"]
DRIFT_SPECS = [("web_events", "event_type", "categorical"), ("web_events", "device", "categorical"),
               ("shipments", "carrier", "categorical"), ("shipments", "lead_days", "numeric")]
LINEAGE = [
    ["files/web_events.csv", "bronze.events_web_events", "silver.web_events", "gold.funnel_daily"],
    ["files/shipments.parquet", "bronze.events_shipments", "silver.shipments", "gold.delivery_sla"]]
CSV_NAME, PARQUET_NAME = "web_events.csv", "shipments.parquet"


def default_config() -> PipelineConfig:
    return PipelineConfig(pipeline_name="events")


def clean_events(b: DataFrame, cfg: PipelineConfig, customers: DataFrame, products: DataFrame):
    cols = EVENT_COLS
    d = dedup(b, ["event_id"], cols)
    c = (d.withColumn("event_id_raw", blank_to_null("event_id"))
         .withColumn("event_id", F.col("event_id_raw").cast("int"))
         .withColumn("session_id", blank_to_null("session_id"))
         .withColumn("customer_raw", blank_to_null("customer_id")).withColumn("customer_id", F.col("customer_raw").cast("int"))
         .withColumn("product_raw", blank_to_null("product_id")).withColumn("product_id", F.col("product_raw").cast("int"))
         .withColumn("event_type", F.lower(F.trim("event_type"))).withColumn("device", F.lower(F.trim("device")))
         .withColumn("event_ts", F.to_timestamp(F.trim("event_ts"))))
    c = c.join(customers.select(F.col("customer_id").alias("_ck")), c.customer_id == F.col("_ck"), "left")
    c = c.join(products.select(F.col("product_id").alias("_pk")), c.product_id == F.col("_pk"), "left")
    reason = first_reason(
        (F.col("event_id").isNull(), "bad_event_id"),
        (F.col("session_id").isNull(), "missing_session"),
        (F.col("event_ts").isNull() | (F.col("event_ts") > F.lit(cfg.as_of).cast("timestamp")), "bad_event_ts"),
        (F.col("event_type").isNull() | ~F.col("event_type").isin(EVENT_TYPES), "invalid_event_type"),
        (F.col("device").isNull() | ~F.col("device").isin(DEVICES), "invalid_device"),
        (F.col("customer_raw").isNotNull() & F.col("customer_id").isNull(), "bad_customer_id"),
        (F.col("product_raw").isNotNull() & F.col("product_id").isNull(), "bad_product_id"),
        (F.col("event_type").isin("product_view", "add_to_cart") & F.col("product_id").isNull(), "missing_product"),
        (F.col("customer_id").isNotNull() & F.col("_ck").isNull(), "orphan_customer"),
        (F.col("product_id").isNotNull() & F.col("_pk").isNull(), "orphan_product"))
    c = c.withColumn("_reject_reason", reason).drop("_ck", "_pk", "customer_raw", "product_raw", "event_id_raw")
    return c, cols


def clean_shipments(b: DataFrame, cfg: PipelineConfig, orders: DataFrame):
    cols = ["shipment_id", "order_id", "carrier", "shipped_at", "delivered_at", "status"]
    d = dedup(b, ["shipment_id"], cols)
    c = (d.withColumn("carrier", blank_to_null("carrier")).withColumn("status", F.lower(F.trim("status"))))
    c = c.join(orders.select(F.col("order_id").alias("_ok")), c.order_id == F.col("_ok"), "left")
    reason = first_reason(
        (F.col("shipment_id").isNull(), "null_key"),
        (F.col("order_id").isNull(), "missing_order"),
        (F.col("carrier").isNull(), "missing_carrier"),
        (F.col("status").isNull() | ~F.col("status").isin(SHIP_STATUSES), "invalid_status"),
        (F.col("shipped_at").isNull(), "bad_shipped_at"),
        (F.col("delivered_at").isNotNull() & (F.col("delivered_at") < F.col("shipped_at")), "delivered_before_shipped"),
        ((F.col("status") == "shipped") & F.col("delivered_at").isNotNull(), "status_mismatch"),
        (F.col("status").isin("delivered", "returned") & F.col("delivered_at").isNull(), "status_mismatch"),
        (F.col("_ok").isNull(), "orphan_order"))
    return c.withColumn("_reject_reason", reason).drop("_ok"), cols


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

    # ---------------- BRONZE
    csv_path, pq_path = cfg.files_dir / CSV_NAME, cfg.files_dir / PARQUET_NAME
    for p in (csv_path, pq_path):
        if not p.is_file():
            raise ContractError(f"input file missing: {p.name}")
    raw = {
        "web_events": (spark.read.option("header", True).option("mode", "PERMISSIVE")
                       .option("inferSchema", False).csv(str(csv_path))),
        "shipments": spark.read.parquet(str(pq_path)),
    }
    for name, df in raw.items():
        dq.contract(df, f"file.{name}", CONTRACTS[name])
    broken = [r for r in dq.results if r.check_type == "contract" and r.status == "failed"]
    if broken:
        dq.flush(ctx)
        raise ContractError("; ".join(f"{r.dataset}: missing={r.detail['missing_columns']} "
                                      f"type_mismatch={r.detail['type_mismatch']}" for r in broken))
    sources = {"web_events": f"file:{CSV_NAME}", "shipments": f"file:{PARQUET_NAME}"}
    for name, df in raw.items():
        write_table(df.withColumn("_run_id", F.lit(ctx.run_id)).withColumn("_ingested_at", F.lit(ingested))
                    .withColumn("_source", F.lit(sources[name])), cfg.out("bronze", f"events_{name}"))
    bronze = {n: read_table(spark, cfg.out("bronze", f"events_{n}")).cache() for n in CONTRACTS}
    bronze_rows = {n: df.count() for n, df in bronze.items()}
    parents = {  # ID columns only, from the read-only source role
        "customers": read_table(spark, cfg.src_tables["customers"], "source", "customer_id").cache(),
        "products": read_table(spark, cfg.src_tables["products"], "source", "product_id").cache(),
        "orders": read_table(spark, cfg.src_tables["orders"], "source", "order_id").cache()}
    stages["bronze_s"] = round(time.time() - t, 1)

    # ---------------- SILVER
    t = time.time()
    silver: dict[str, DataFrame] = {}
    rejects: list[DataFrame] = []
    stats: dict[str, dict] = {}

    def finish(name: str, cleaned: DataFrame, cols: list[str], key: str, extra_out: list[str] | None = None):
        cleaned = cleaned.cache()
        good, bad = cleaned.filter("_reject_reason IS NULL"), cleaned.filter("_reject_reason IS NOT NULL")
        n_good, n_bad = good.count(), bad.count()
        deduped_n = n_good + n_bad
        removed = bronze_rows[name] - deduped_n
        reasons = {r["_reject_reason"]: r["count"] for r in bad.groupBy("_reject_reason").count().collect()}
        out = good.select(*cols, *(extra_out or [])).withColumn("_run_id", F.lit(ctx.run_id)).cache()
        silver[name] = out
        rejects.append(rejects_frame(bad, name, key, cols, ctx.run_id))
        stats[name] = {"bronze": bronze_rows[name], "duplicates_removed": removed, "rejected": n_bad,
                       "silver": n_good, "reject_reasons": reasons}
        dq.duplicates(name, removed)
        dq.reject_rate(name, deduped_n, n_bad, cfg.max_reject_pct, reasons)

    c, cols = clean_events(bronze["web_events"], cfg, parents["customers"], parents["products"])
    c = c.withColumn("event_date", F.to_date("event_ts"))
    finish("web_events", c, cols, "event_id", ["event_date"])
    c, cols = clean_shipments(bronze["shipments"], cfg, parents["orders"])
    c = c.withColumn("lead_days", F.datediff("delivered_at", "shipped_at"))
    finish("shipments", c, cols, "shipment_id", ["lead_days"])

    for name, df in silver.items():
        write_table(df, cfg.out("silver", name))
    all_rejects = rejects[0].unionByName(rejects[1])
    write_table(all_rejects, cfg.out("silver", "events_rejects"))

    ev, sh = silver["web_events"], silver["shipments"]
    for name, key in (("web_events", "event_id"), ("shipments", "shipment_id")):
        df = silver[name]
        dq.invariant(name, f"{key}_not_null", "not_null", df.filter(F.col(key).isNull()).count())
        dq.invariant(name, f"{key}_unique", "uniqueness", df.count() - df.select(key).distinct().count())
    dq.invariant("web_events", "customer_exists", "referential",
                 ev.filter("customer_id IS NOT NULL").join(parents["customers"], "customer_id", "left_anti").count())
    dq.invariant("web_events", "product_exists", "referential",
                 ev.filter("product_id IS NOT NULL").join(parents["products"], "product_id", "left_anti").count())
    dq.invariant("shipments", "order_exists", "referential", sh.join(parents["orders"], "order_id", "left_anti").count())
    dq.invariant("shipments", "one_shipment_per_order", "uniqueness",
                 sh.count() - sh.select("order_id").distinct().count(), severity="warn")
    pending = monitor.volume_and_drift(ctx, dq, cfg, silver, DRIFT_SPECS)
    stages["silver_s"] = round(time.time() - t, 1)

    dq.flush(ctx)
    if dq.blocking():                                     # GATE 1
        raise DQGateError("data-quality gate failed, Gold not published: " + dq.gate_message())

    # ---------------- GOLD
    t = time.time()
    is_ = lambda et: F.sum(F.when(F.col("event_type") == et, 1).otherwise(0))
    funnel = (ev.groupBy("event_date", "device").agg(
        F.countDistinct("session_id").alias("sessions"), is_("page_view").alias("page_views"),
        is_("product_view").alias("product_views"), is_("add_to_cart").alias("add_to_carts"),
        is_("checkout_start").alias("checkout_starts"), is_("purchase").alias("purchases"))
        .withColumn("view_to_cart_rate", F.round(F.col("add_to_carts") / F.nullif(F.col("product_views"), F.lit(0)), 4))
        .withColumn("cart_to_purchase_rate", F.round(F.col("purchases") / F.nullif(F.col("add_to_carts"), F.lit(0)), 4))
        .withColumn("purchase_rate", F.round(F.col("purchases") / F.col("sessions"), 4))
        .withColumn("_run_id", F.lit(ctx.run_id)))
    sla = (sh.withColumn("ship_month", F.trunc("shipped_at", "month")).groupBy("ship_month", "carrier").agg(
        F.count("*").alias("shipments"), F.count("delivered_at").alias("delivered"),
        F.round(F.avg("lead_days"), 2).alias("avg_lead_days"),
        F.expr("percentile_approx(lead_days, 0.9)").alias("p90_lead_days"),
        F.sum(F.when(F.col("lead_days") <= cfg.sla_days, 1).otherwise(0)).alias("on_time"))
        .withColumn("on_time_pct", F.round(100 * F.col("on_time") / F.nullif(F.col("delivered"), F.lit(0)), 2))
        .drop("on_time").withColumn("sla_days", F.lit(cfg.sla_days)).withColumn("_run_id", F.lit(ctx.run_id)))
    write_table(funnel, cfg.stage("funnel_daily"))
    write_table(sla, cfg.stage("delivery_sla"))
    g_f, g_s = read_table(spark, cfg.stage("funnel_daily")), read_table(spark, cfg.stage("delivery_sla"))

    s_ev, s_sh = ev.count(), sh.count()
    gf = g_f.agg(F.sum(F.col("page_views") + F.col("product_views") + F.col("add_to_carts")
                       + F.col("checkout_starts") + F.col("purchases"))).collect()[0][0] or 0
    gs = g_s.agg(F.sum("shipments"), F.sum("delivered")).collect()[0]
    dq.invariant("gold.funnel_daily", "events_match_silver", "reconciliation", abs(gf - s_ev),
                 detail={"gold": gf, "silver": s_ev})
    dq.invariant("gold.delivery_sla", "shipments_match_silver", "reconciliation", abs((gs[0] or 0) - s_sh),
                 detail={"gold": gs[0], "silver": s_sh})
    dq.invariant("gold.delivery_sla", "delivered_match_silver", "reconciliation",
                 abs((gs[1] or 0) - sh.filter("delivered_at IS NOT NULL").count()))
    dq.flush(ctx)
    if dq.blocking():                                     # GATE 2
        raise DQGateError("reconciliation failed, Gold not published: " + dq.gate_message())

    gold_rows = {"funnel_daily": g_f.count(), "delivery_sla": g_s.count()}
    publish_gold([(cfg.out("gold", "funnel_daily"), cfg.stage("funnel_daily")),
                  (cfg.out("gold", "delivery_sla"), cfg.stage("delivery_sla"))])
    stages["gold_s"] = round(time.time() - t, 1)
    ctx.persist_baselines(pending)

    from etl.runner import PipelineResult
    return PipelineResult(
        rows_in=sum(bronze_rows.values()), rows_out=sum(gold_rows.values()),
        details={"bronze_rows": bronze_rows, "silver": stats, "gold_rows": gold_rows, "stages": stages,
                 "lineage": LINEAGE, "dq": {"checks": len(dq.results),
                                            "failed_non_blocking": len([r for r in dq.results if r.status == "failed"]),
                                            "warnings": len([r for r in dq.results if r.status == "warning"])},
                 "baselines_created": len(pending)})
