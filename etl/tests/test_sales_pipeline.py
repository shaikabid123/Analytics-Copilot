"""End-to-end tests of the PySpark 'sales' pipeline on small copies of the source tables (src.zt_*),
writing to isolated t_-prefixed outputs. Real Gold tables are never touched."""
import json
import uuid
from datetime import date

import pytest
from sqlalchemy import text

from etl.pipelines import sales
from etl.runner import execute
from etl.common import PipelineConfig
from etl.tests.conftest import drop_test_artifacts

SRC = {k: f"src.zt_{k}" for k in ("customers", "products", "orders", "order_items")}
AS_OF = date(2026, 10, 1)
CHECKSUM = """SELECT md5(string_agg(x::text, '|' ORDER BY x::text)) FROM (
  SELECT order_date, channel, orders, items, units, gross_merchandise_value, net_revenue, cancelled_orders,
         returned_orders, avg_order_value, return_rate FROM gold.t_daily_sales) x"""


def build_sources(etl_engine):
    with etl_engine.begin() as c:
        for t in SRC.values():
            c.execute(text(f"DROP TABLE IF EXISTS {t}"))
        c.execute(text("CREATE TABLE src.zt_customers AS SELECT * FROM src.customers WHERE customer_id <= 200"))
        c.execute(text("CREATE TABLE src.zt_products AS SELECT * FROM src.products"))
        c.execute(text("CREATE TABLE src.zt_orders AS SELECT * FROM src.orders WHERE customer_id <= 200"))
        c.execute(text("CREATE TABLE src.zt_order_items AS SELECT i.* FROM src.order_items i "
                       "JOIN src.zt_orders o USING (order_id)"))


class Env:
    def __init__(self, spark, etl_engine, app_engine, **cfg_kwargs):
        self.spark, self.etl, self.app = spark, etl_engine, app_engine
        self.cfg = PipelineConfig(pipeline_name=f"sales_t_{uuid.uuid4().hex[:8]}", prefix="t_", src_tables=SRC,
                                  as_of=AS_OF, **cfg_kwargs)

    def run(self, **kw):
        kw = {"max_retries": 3, "backoff_s": 0, "sleep": lambda s: None, **kw}
        return execute(self.cfg, sales.run, spark=self.spark, **kw)

    def etl_q(self, sql, **p):
        with self.etl.connect() as c:
            return c.execute(text(sql), p).all()

    def app_q(self, sql, **p):
        with self.app.connect() as c:
            return c.execute(text(sql), p).all()

    def sql(self, sql):
        with self.etl.begin() as c:
            c.execute(text(sql))

    def details(self, run_id):
        return self.app_q("SELECT details FROM app.pipeline_runs WHERE id=:i", i=run_id)[0][0]

    def results(self, run_id, **where):
        rows = self.app_q("SELECT dataset, check_name, status, severity, observed, detail FROM app.dq_results "
                          "WHERE run_id=:i", i=run_id)
        return rows

    def baselines(self):
        rows = self.app_q("SELECT dataset, column_name, baseline FROM app.drift_baselines b JOIN app.pipelines p "
                          "ON p.id=b.pipeline_id WHERE p.name=:n ORDER BY 1,2", n=self.cfg.pipeline_name)
        return {(d, c): b for d, c, b in rows}


@pytest.fixture()
def env(spark, etl_engine, app_engine, request):
    drop_test_artifacts(etl_engine, app_engine, "sales\\_t\\_%")
    build_sources(etl_engine)
    e = Env(spark, etl_engine, app_engine, **getattr(request, "param", {}))
    yield e
    drop_test_artifacts(etl_engine, app_engine, "sales\\_t\\_%")


def test_clean_run_publishes_reconciled_gold_and_is_idempotent(env):
    out = env.run()
    assert out.status == "succeeded", out.error
    rid = out.run_ids[-1]
    res = env.results(rid)
    assert len(res) >= 35 and all(r.status == "passed" for r in res)
    assert env.etl_q("SELECT count(*) FROM gold.t_customer_value")[0][0] == 200
    src_gmv = env.etl_q("SELECT sum(total_amount) FROM src.zt_orders")[0][0]
    assert env.etl_q("SELECT sum(gross_merchandise_value) FROM gold.t_daily_sales")[0][0] == src_gmv
    assert env.etl_q("SELECT sum(orders) FROM gold.t_daily_sales")[0][0] == env.etl_q("SELECT count(*) FROM src.zt_orders")[0][0]
    assert env.etl_q("SELECT count(*) FROM silver.t_sales_rejects")[0][0] == 0
    assert env.etl_q("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'stg\\_t\\_%'")[0][0] == 0
    assert len(env.baselines()) == 10                       # 6 drift columns + 4 volume baselines
    first = env.etl_q(CHECKSUM)[0][0]
    lineage = env.details(rid)["lineage"]
    assert ["src.orders", "bronze.sales_orders", "silver.orders", "gold.daily_sales"] in lineage

    out2 = env.run()                                        # same input, second run
    assert out2.status == "succeeded"
    assert env.etl_q(CHECKSUM)[0][0] == first               # idempotent: identical Gold
    assert env.details(out2.run_ids[-1])["baselines_created"] == 0
    drift = [r for r in env.results(out2.run_ids[-1]) if r.check_name.startswith("drift_")]
    assert len(drift) == 6 and all(r.status == "passed" and r.observed < 0.01 for r in drift)


def test_dirty_rows_are_cleaned_or_quarantined_with_reasons(env):
    env.sql("""
      INSERT INTO src.zt_customers SELECT * FROM src.zt_customers WHERE customer_id = 1;           -- exact duplicate
      UPDATE src.zt_customers SET email = NULL WHERE customer_id = 2;                              -- missing email
      UPDATE src.zt_customers SET segment = 'VIP' WHERE customer_id = 3;                           -- invalid segment
      UPDATE src.zt_customers SET email = 'not-an-email' WHERE customer_id = 4;                    -- invalid email
      UPDATE src.zt_customers SET segment = '  Consumer ', email = UPPER(email) WHERE customer_id = 5;  -- fixable by cleaning
      UPDATE src.zt_products SET unit_price = -5 WHERE product_id = 10;                            -- non-positive price
      INSERT INTO src.zt_orders SELECT 900001, 999999, order_ts, status, channel, total_amount
        FROM src.zt_orders LIMIT 1;                                                                -- orphan customer
      INSERT INTO src.zt_orders SELECT 900002, 50, order_ts, 'LOST', channel, total_amount
        FROM src.zt_orders LIMIT 1;                                                                -- invalid status
      UPDATE src.zt_orders SET status = ' DELIVERED ' WHERE order_id = (SELECT min(order_id) FROM src.zt_orders WHERE customer_id = 60);
      INSERT INTO src.zt_order_items SELECT * FROM src.zt_order_items WHERE order_item_id = (SELECT min(order_item_id) FROM src.zt_order_items);  -- duplicate item
      UPDATE src.zt_order_items SET quantity = 0 WHERE order_item_id = (SELECT min(order_item_id) FROM src.zt_order_items i
        JOIN src.zt_orders o USING (order_id) WHERE o.customer_id = 100);                          -- zero quantity
      INSERT INTO src.zt_order_items SELECT 900001, min(order_id), 888888, 1, 10.00 FROM src.zt_orders WHERE customer_id = 50;""")
    env.cfg.max_reject_pct = 10
    out = env.run()
    assert out.status == "succeeded", out.error
    rid = out.run_ids[-1]
    reasons = dict(env.etl_q("SELECT reason, count(*) FROM silver.t_sales_rejects GROUP BY 1"))
    for exact in ("missing_email", "invalid_segment", "invalid_email", "non_positive_price",
                  "invalid_status", "non_positive_quantity", "orphan_product"):
        assert reasons.get(exact) == 1, (exact, reasons)
    cascaded = env.etl_q("SELECT count(*) FROM src.zt_orders WHERE customer_id IN (2,3,4)")[0][0]
    assert reasons["orphan_customer"] == cascaded + 1       # orders of rejected customers + the orphan we added
    stats = env.details(rid)["silver"]
    assert stats["customers"]["duplicates_removed"] == 1 and stats["order_items"]["duplicates_removed"] == 1
    # cleaning normalises instead of rejecting
    assert env.etl_q("SELECT segment, email FROM silver.t_customers WHERE customer_id = 5")[0] == \
        ("consumer", env.etl_q("SELECT lower(email) FROM src.zt_customers WHERE customer_id = 5")[0][0])
    assert env.etl_q("SELECT status FROM silver.t_orders WHERE order_id = (SELECT min(order_id) "
                     "FROM src.zt_orders WHERE customer_id = 60)")[0][0] == "delivered"
    # quarantined rows are absent from Silver and Gold; rejects keep the evidence
    assert env.etl_q("SELECT count(*) FROM silver.t_customers WHERE customer_id IN (2,3,4)")[0][0] == 0
    assert env.etl_q("SELECT count(*) FROM silver.t_orders o JOIN silver.t_customers c USING (customer_id)")[0][0] == \
        env.etl_q("SELECT count(*) FROM silver.t_orders")[0][0]                       # no orphans survive
    assert env.etl_q("SELECT sum(orders) FROM gold.t_daily_sales")[0][0] == env.etl_q("SELECT count(*) FROM silver.t_orders")[0][0]
    assert '"customer_id":2' in env.etl_q("SELECT record_json FROM silver.t_sales_rejects WHERE reason='missing_email'")[0][0].replace(" ", "")
    # duplicates raise a non-blocking alert; nothing blocks
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND severity='medium'", i=rid)[0][0] >= 1
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND severity='high'", i=rid)[0][0] == 0


def test_gate_blocks_gold_when_too_many_rows_are_rejected(env):
    assert env.run().status == "succeeded"
    before = env.etl_q(CHECKSUM)[0][0]
    env.sql("UPDATE src.zt_customers SET email = NULL WHERE customer_id % 5 = 0")        # 20% bad
    out = env.run()
    assert out.status == "failed" and out.attempts == 1                                  # no pointless retries
    assert "Gold not published" in out.error and "reject_rate" in out.error
    assert env.etl_q(CHECKSUM)[0][0] == before                                           # Gold untouched
    rid = out.run_ids[-1]
    blocking = [r for r in env.results(rid) if r.status == "failed" and r.severity == "error"]
    assert any(r.dataset == "customers" and r.check_name == "reject_rate" and r.observed == 20.0 for r in blocking)
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND severity='high'", i=rid)[0][0] >= 1
    assert env.etl_q("SELECT count(*) FROM silver.t_sales_rejects WHERE reason='missing_email'")[0][0] == 40   # diagnosable
    assert env.etl_q("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'stg\\_t\\_%'")[0][0] == 0


def test_contract_violations_fail_fast_without_retry_or_writes(env):
    env.sql("ALTER TABLE src.zt_orders DROP COLUMN channel")
    out = env.run(max_retries=3)
    assert out.status == "failed" and out.attempts == 1 and "channel" in out.error and "ContractError" in out.error
    assert env.etl_q("SELECT count(*) FROM information_schema.tables WHERE table_name = 't_sales_orders'")[0][0] == 0
    contract = [r for r in env.results(out.run_ids[-1]) if r.check_name == "schema_contract" and r.status == "failed"]
    assert contract and contract[0].detail["missing_columns"] == ["channel"]
    build_sources(env.etl)
    env.sql("ALTER TABLE src.zt_orders ALTER COLUMN total_amount TYPE text USING total_amount::text")
    out2 = env.run(max_retries=3)
    assert out2.status == "failed" and out2.attempts == 1 and "total_amount" in out2.error and "type_mismatch" in out2.error


def test_drift_alerts_but_still_publishes_and_baselines_are_stable(env):
    assert env.run().status == "succeeded"
    base_before = env.baselines()
    env.sql("UPDATE src.zt_orders SET total_amount = total_amount * 6 WHERE order_id % 2 = 0; "
            "UPDATE src.zt_orders SET channel = 'store' WHERE order_id % 3 > 0")
    out = env.run()
    assert out.status == "succeeded", out.error                      # drift is a warning, not a blocker
    rid = out.run_ids[-1]
    by = {r.check_name: r for r in env.results(rid)}
    assert by["drift_total_amount"].status == "failed" and by["drift_total_amount"].observed >= 0.25
    assert by["drift_channel"].status == "failed" and by["drift_channel"].severity == "warn"
    assert by["drift_status"].status == "passed" and by["drift_segment"].status == "passed"    # untouched columns stay quiet
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND title LIKE '%drift_total_amount%'", i=rid)[0][0] == 1
    assert env.baselines() == base_before                            # reference is not silently overwritten
    new_gmv = env.etl_q("SELECT sum(total_amount) FROM src.zt_orders")[0][0]
    assert env.etl_q("SELECT sum(gross_merchandise_value) FROM gold.t_daily_sales")[0][0] == new_gmv   # Gold still published

    out3 = env.run(reset_baseline=True)                              # operator accepts the new normal
    assert out3.status == "succeeded"
    assert env.baselines() != base_before
    assert all(r.status == "passed" for r in env.results(out3.run_ids[-1]) if r.check_name.startswith("drift_"))


def test_volume_collapse_blocks_gold(env):
    assert env.run().status == "succeeded"
    before = env.etl_q(CHECKSUM)[0][0]
    env.sql("DELETE FROM src.zt_order_items WHERE order_id % 10 < 7; DELETE FROM src.zt_orders WHERE order_id % 10 < 7")
    out = env.run()
    assert out.status == "failed" and "row_count_vs_baseline" in out.error
    assert env.etl_q(CHECKSUM)[0][0] == before
    vol = [r for r in env.results(out.run_ids[-1]) if r.check_name == "row_count_vs_baseline" and r.status == "failed"]
    assert {r.dataset for r in vol} >= {"orders", "order_items"} and all(r.observed < 0.5 for r in vol)
