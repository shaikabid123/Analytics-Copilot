"""Verifies the REAL pipeline outputs. Run the pipelines first:
    python -m etl.runner run sales ; python -m etl.runner run events
Every expected figure is recomputed independently from the raw sources (plain SQL, csv, pyarrow), not Spark."""
import csv
import os
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, pool, text


@pytest.fixture(scope="module")
def q(etl_engine):
    def run(sql):
        with etl_engine.connect() as c:
            return c.execute(text(sql)).all()
    try:
        run("SELECT 1 FROM gold.daily_sales LIMIT 1")
        run("SELECT 1 FROM gold.funnel_daily LIMIT 1")
    except Exception:
        pytest.fail("Gold tables missing: run both pipelines first (see module docstring)", pytrace=False)
    return run


@pytest.fixture(scope="module")
def files():
    return Path(os.environ["DATA_DIR"]) / "files"


def test_latest_real_runs_succeeded_with_no_blocking_failures(app_engine):
    with app_engine.connect() as c:
        for name in ("sales", "events"):
            run = c.execute(text("SELECT r.id, r.status FROM app.pipeline_runs r JOIN app.pipelines p ON p.id=r.pipeline_id "
                                 "WHERE p.name=:n ORDER BY r.id DESC LIMIT 1"), {"n": name}).first()
            assert run and run.status == "succeeded", (name, run)
            bad = c.execute(text("SELECT count(*) FROM app.dq_results WHERE run_id=:i AND status='failed' AND severity='error'"),
                            {"i": run.id}).scalar()
            assert bad == 0


def test_sales_gold_matches_source_computed_independently(q):
    src = q("SELECT count(*), sum(total_amount), sum(total_amount) FILTER (WHERE status NOT IN ('cancelled','returned')), "
            "count(*) FILTER (WHERE status='cancelled'), count(*) FILTER (WHERE status='returned') FROM src.orders")[0]
    gold = q("SELECT sum(orders), sum(gross_merchandise_value), sum(net_revenue), sum(cancelled_orders), sum(returned_orders) "
             "FROM gold.daily_sales")[0]
    assert tuple(gold) == tuple(src)
    items = q("SELECT count(*), sum(quantity) FROM src.order_items")[0]
    assert tuple(q("SELECT sum(items), sum(units) FROM gold.daily_sales")[0]) == tuple(items)
    assert q("SELECT count(*) FROM gold.customer_value")[0][0] == q("SELECT count(*) FROM src.customers")[0][0]
    ltv_src = q("SELECT sum(total_amount) FILTER (WHERE status NOT IN ('cancelled','returned')) FROM src.orders")[0][0]
    assert q("SELECT sum(lifetime_value) FROM gold.customer_value")[0][0] == ltv_src
    # spot check one day/channel against the source
    day, ch, n, gmv = q("SELECT order_date, channel, orders, gross_merchandise_value FROM gold.daily_sales "
                        "ORDER BY orders DESC, order_date LIMIT 1")[0]
    assert q(f"SELECT count(*), sum(total_amount) FROM src.orders WHERE order_ts::date = '{day}' AND channel = '{ch}'")[0] == (n, gmv)
    # one customer against the source
    cid = q("SELECT customer_id FROM gold.customer_value WHERE orders > 0 ORDER BY lifetime_value DESC LIMIT 1")[0][0]
    assert q(f"SELECT orders, lifetime_value FROM gold.customer_value WHERE customer_id={cid}")[0] == \
        tuple(q(f"SELECT count(*), sum(total_amount) FILTER (WHERE status NOT IN ('cancelled','returned')) "
                f"FROM src.orders WHERE customer_id={cid}")[0])


def test_events_gold_matches_raw_files(q, files):
    by_type = Counter()
    with open(files / "web_events.csv", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            by_type[r["event_type"]] += 1
    gold = q("SELECT sum(page_views), sum(product_views), sum(add_to_carts), sum(checkout_starts), sum(purchases) FROM gold.funnel_daily")[0]
    assert tuple(gold) == tuple(by_type[t] for t in ("page_view", "product_view", "add_to_cart", "checkout_start", "purchase"))
    t = pq.read_table(files / "shipments.parquet").to_pydict()
    lead = [(d - s).days for s, d in zip(t["shipped_at"], t["delivered_at"]) if d is not None]
    shipments, delivered = q("SELECT sum(shipments), sum(delivered) FROM gold.delivery_sla")[0]
    assert (shipments, delivered) == (len(t["order_id"]), len(lead))
    avg = q("SELECT sum(avg_lead_days*delivered)/sum(delivered) FROM gold.delivery_sla")[0][0]
    assert abs(float(avg) - sum(lead) / len(lead)) < 0.01
    on_time = q("SELECT sum(round(on_time_pct*delivered/100.0)) FROM gold.delivery_sla")[0][0]
    assert abs(float(on_time) - sum(1 for x in lead if x <= 5)) <= 5       # per-group percentages are rounded to 2 dp


def test_layers_reconcile_bronze_to_silver(q):
    for name in ("customers", "products", "orders", "order_items"):
        bronze = q(f"SELECT count(*) FROM bronze.sales_{name}")[0][0]
        silver = q(f"SELECT count(*) FROM silver.{name}")[0][0]
        rejected = q(f"SELECT count(*) FROM silver.sales_rejects WHERE dataset='{name}'")[0][0]
        assert silver + rejected == bronze, name                          # nothing silently dropped (seed has no duplicates)
    for name, table in (("web_events", "events_web_events"), ("shipments", "events_shipments")):
        assert q(f"SELECT count(*) FROM silver.{name}")[0][0] + \
            q(f"SELECT count(*) FROM silver.events_rejects WHERE dataset='{name}'")[0][0] == \
            q(f"SELECT count(*) FROM bronze.{table}")[0][0]
    assert q("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'stg\\_%'")[0][0] == 0


def test_bronze_carries_lineage_columns(q):
    cols = {r[0] for r in q("SELECT column_name FROM information_schema.columns WHERE table_schema='bronze' AND table_name='sales_orders'")}
    assert {"_run_id", "_ingested_at", "_source"} <= cols
    assert q("SELECT DISTINCT _source FROM bronze.sales_orders")[0][0] == "src.orders"


def test_run_details_record_lineage_and_counts(app_engine):
    with app_engine.connect() as c:
        d = c.execute(text("SELECT r.details, r.rows_in, r.rows_out FROM app.pipeline_runs r JOIN app.pipelines p ON p.id=r.pipeline_id "
                           "WHERE p.name='sales' AND r.status='succeeded' ORDER BY r.id DESC LIMIT 1")).first()
    assert d.details["lineage"] and d.rows_in > 290_000 and d.rows_out > 20_000
    assert set(d.details["gold_rows"]) == {"daily_sales", "customer_value"}


def test_analytics_role_reads_gold_only():
    """The read-only analytics role (used for generated SQL) sees the new Gold tables and nothing below them."""
    engine = create_engine(os.environ["ANALYTICS_RO_URL"], poolclass=pool.NullPool)
    try:
        with engine.connect() as c:
            for t in ("daily_sales", "customer_value", "funnel_daily", "delivery_sla"):
                assert c.execute(text(f"SELECT count(*) FROM gold.{t}")).scalar() > 0
        for t in ("silver.orders", "bronze.sales_orders", "src.orders", "silver.sales_rejects", "app.users"):
            with pytest.raises(Exception, match="permission denied"):
                with engine.connect() as c:
                    c.execute(text(f"SELECT 1 FROM {t} LIMIT 1"))
    finally:
        engine.dispose()
