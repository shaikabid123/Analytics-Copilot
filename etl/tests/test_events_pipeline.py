"""End-to-end tests of the PySpark 'events' pipeline on small generated CSV/Parquet files (tmp dir),
writing to isolated t_-prefixed outputs. Referential parents come from the real src tables (ids 1..)."""
import csv
import uuid
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from etl.common import PipelineConfig
from etl.pipelines import events
from etl.runner import execute
from etl.tests.conftest import drop_test_artifacts

AS_OF = date(2026, 10, 1)
TYPES = ["page_view", "page_view", "product_view", "product_view", "add_to_cart", "checkout_start", "purchase"]
DEVICES = ["desktop", "mobile", "mobile", "tablet"]
HEADER = ["event_id", "session_id", "customer_id", "event_type", "product_id", "event_ts", "device"]
SCHEMA = pa.schema([("shipment_id", pa.int64(), False), ("order_id", pa.int64(), False), ("carrier", pa.string(), True),
                    ("shipped_at", pa.timestamp("us"), True), ("delivered_at", pa.timestamp("us"), True),
                    ("status", pa.string(), True)])
CHECKSUM = """SELECT md5(string_agg(x::text, '|' ORDER BY x::text)) FROM (
  SELECT event_date, device, sessions, page_views, product_views, add_to_carts, checkout_starts, purchases
  FROM gold.t_funnel_daily) x"""


def clean_events(n=300):
    rows = []
    for i in range(1, n + 1):
        et = TYPES[i % len(TYPES)]
        rows.append([i, f"s{i % 40:03d}", "" if i % 3 == 0 else (i % 50) + 1, et,
                     (i % 100) + 1 if et in ("product_view", "add_to_cart") else "",
                     f"2026-09-{(i % 28) + 1:02d}T10:{i % 60:02d}:00", DEVICES[i % len(DEVICES)]])
    return rows


def clean_shipments(n=100):
    out = []
    for i in range(1, n + 1):
        shipped = datetime(2026, 9, 1) + timedelta(hours=i)
        status = "shipped" if i % 5 == 0 else ("returned" if i % 7 == 0 else "delivered")
        delivered = None if status == "shipped" else shipped + timedelta(days=1 + i % 9)
        out.append((i, i, ["DHL", "FedEx", "BlueDart"][i % 3], shipped, delivered, status))
    return out


def write_files(d, events_rows=None, ships=None, header=HEADER, schema=SCHEMA):
    with open(d / "web_events.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(events_rows if events_rows is not None else clean_events())
    ships = ships if ships is not None else clean_shipments()
    cols = list(zip(*ships))
    pq.write_table(pa.table({f.name: list(cols[i]) for i, f in enumerate(schema)}, schema=schema), d / "shipments.parquet")


class Env:
    def __init__(self, spark, etl_engine, app_engine, files_dir, **kw):
        self.spark, self.etl, self.app, self.dir = spark, etl_engine, app_engine, files_dir
        self.cfg = PipelineConfig(pipeline_name=f"events_t_{uuid.uuid4().hex[:8]}", prefix="t_",
                                  files_dir=files_dir, as_of=AS_OF, **kw)

    def run(self, **kw):
        kw = {"max_retries": 3, "backoff_s": 0, "sleep": lambda s: None, **kw}
        return execute(self.cfg, events.run, spark=self.spark, **kw)

    def etl_q(self, sql):
        with self.etl.connect() as c:
            return c.execute(text(sql)).all()

    def app_q(self, sql, **p):
        with self.app.connect() as c:
            return c.execute(text(sql), p).all()

    def results(self, run_id):
        return self.app_q("SELECT dataset, check_name, status, severity, observed FROM app.dq_results WHERE run_id=:i", i=run_id)

    def baselines(self):
        return {(d, c): b for d, c, b in self.app_q(
            "SELECT dataset, column_name, baseline FROM app.drift_baselines b JOIN app.pipelines p ON p.id=b.pipeline_id "
            "WHERE p.name=:n ORDER BY 1,2", n=self.cfg.pipeline_name)}

    def reasons(self):
        return dict(self.etl_q("SELECT reason, count(*) FROM silver.t_events_rejects GROUP BY 1"))


@pytest.fixture()
def env(spark, etl_engine, app_engine, tmp_path, request):
    drop_test_artifacts(etl_engine, app_engine, "events\\_t\\_%")
    write_files(tmp_path)
    yield Env(spark, etl_engine, app_engine, tmp_path, **getattr(request, "param", {}))
    drop_test_artifacts(etl_engine, app_engine, "events\\_t\\_%")


def test_clean_files_publish_reconciled_gold_and_are_idempotent(env):
    out = env.run()
    assert out.status == "succeeded", out.error
    res = env.results(out.run_ids[-1])
    assert len(res) >= 20 and all(r.status == "passed" for r in res)
    funnel = env.etl_q("SELECT sum(page_views+product_views+add_to_carts+checkout_starts+purchases), sum(purchases) "
                       "FROM gold.t_funnel_daily")[0]
    assert funnel[0] == 300 and funnel[1] == sum(1 for i in range(1, 301) if TYPES[i % 7] == "purchase")
    sla = env.etl_q("SELECT sum(shipments), sum(delivered) FROM gold.t_delivery_sla")[0]
    assert sla[0] == 100 and sla[1] == sum(1 for s in clean_shipments() if s[5] != "shipped")
    on_time = sum(1 for s in clean_shipments() if s[4] and (s[4] - s[3]).days <= 5)
    assert env.etl_q("SELECT round(sum(on_time_pct*delivered/100.0)::numeric, 0) FROM gold.t_delivery_sla")[0][0] == on_time
    assert env.etl_q("SELECT count(*) FROM silver.t_events_rejects")[0][0] == 0
    assert len(env.baselines()) == 6                      # 4 drift columns + 2 volume baselines
    first = env.etl_q(CHECKSUM)[0][0]
    out2 = env.run()
    assert out2.status == "succeeded" and env.etl_q(CHECKSUM)[0][0] == first
    drift = [r for r in env.results(out2.run_ids[-1]) if r.check_name.startswith("drift_")]
    assert len(drift) == 4 and all(r.status == "passed" and r.observed < 0.01 for r in drift)


@pytest.mark.parametrize("env", [{"max_reject_pct": 10}], indirect=True)
def test_dirty_rows_are_cleaned_or_quarantined_with_reasons(env):
    ev = clean_events()
    ev += [list(ev[0]),                                                         # exact duplicate
           [1001, "s001", "", "page_view", "", "not-a-time", "mobile"],
           [1002, "s001", "", "teleport", "", "2026-09-05T10:00:00", "mobile"],
           [1003, "s001", "abc", "page_view", "", "2026-09-05T10:00:00", "mobile"],
           [1004, "s001", 99999999, "page_view", "", "2026-09-05T10:00:00", "mobile"],
           [1005, "s001", "", "product_view", "", "2026-09-05T10:00:00", "mobile"],
           [1006, "s001", "", "add_to_cart", 77777777, "2026-09-05T10:00:00", "mobile"],
           [1007, "s001", "", "page_view", "", "2026-09-05T10:00:00", "fridge"],
           ["x", "s001", "", "page_view", "", "2026-09-05T10:00:00", "mobile"],
           [1010, "", "", "page_view", "", "2026-09-05T10:00:00", "mobile"],
           [1009, "s001", "", " PAGE_VIEW ", "", " 2026-09-05T10:00:00 ", " Mobile "]]      # fixable by cleaning
    sh = clean_shipments()
    t0 = datetime(2026, 9, 10)
    sh += [sh[0],                                                               # duplicate
           (1001, 101, "DHL", t0, t0 - timedelta(days=1), "delivered"),         # delivered before shipped
           (1002, 102, "DHL", t0, t0 + timedelta(days=2), "shipped"),           # shipped but has delivery date
           (1003, 103, "DHL", t0, None, "delivered"),                           # delivered without date
           (1004, 99999999, "DHL", t0, None, "shipped"),                        # orphan order
           (1005, 104, "DHL", t0, None, "lost"),                                # unknown status
           (1006, 105, None, t0, None, "shipped"),                              # missing carrier
           (1007, 106, " DHL ", t0, None, " Shipped ")]                         # fixable
    write_files(env.dir, ev, sh)
    out = env.run()
    assert out.status == "succeeded", out.error
    assert env.reasons() == {"bad_event_ts": 1, "invalid_event_type": 1, "bad_customer_id": 1, "orphan_customer": 1,
                             "missing_product": 1, "orphan_product": 1, "invalid_device": 1, "bad_event_id": 1,
                             "missing_session": 1, "delivered_before_shipped": 1, "status_mismatch": 2,
                             "orphan_order": 1, "invalid_status": 1, "missing_carrier": 1}
    stats = env.app_q("SELECT details FROM app.pipeline_runs WHERE id=:i", i=out.run_ids[-1])[0][0]["silver"]
    assert stats["web_events"]["duplicates_removed"] == 1 and stats["shipments"]["duplicates_removed"] == 1
    assert env.etl_q("SELECT event_type, device FROM silver.t_web_events WHERE event_id = 1009")[0] == ("page_view", "mobile")
    assert env.etl_q("SELECT carrier, status FROM silver.t_shipments WHERE shipment_id = 1007")[0] == ("DHL", "shipped")
    assert env.etl_q("SELECT count(*) FROM silver.t_web_events WHERE event_id IN (1001,1002,1003,1004,1005,1006,1007,1010)")[0][0] == 0
    silver_n = env.etl_q("SELECT count(*) FROM silver.t_web_events")[0][0]
    assert env.etl_q("SELECT sum(page_views+product_views+add_to_carts+checkout_starts+purchases) FROM gold.t_funnel_daily")[0][0] == silver_n
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND severity='medium'", i=out.run_ids[-1])[0][0] >= 1
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND severity='high'", i=out.run_ids[-1])[0][0] == 0


def test_gate_blocks_gold_when_too_many_rows_are_rejected(env):
    assert env.run().status == "succeeded"
    before = env.etl_q(CHECKSUM)[0][0]
    ev = clean_events()
    for r in ev[:120]:
        r[3] = "teleport"                                                       # 40% invalid
    write_files(env.dir, ev)
    out = env.run()
    assert out.status == "failed" and out.attempts == 1 and "Gold not published" in out.error
    assert env.etl_q(CHECKSUM)[0][0] == before
    assert env.reasons()["invalid_event_type"] == 120
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND severity='high'", i=out.run_ids[-1])[0][0] >= 1


def test_contract_violations_and_missing_files_fail_fast(env):
    write_files(env.dir, header=[h for h in HEADER if h != "device"],
                events_rows=[[c for j, c in enumerate(r) if j != 6] for r in clean_events()])
    out = env.run(max_retries=3)
    assert out.status == "failed" and out.attempts == 1 and "device" in out.error and "ContractError" in out.error
    assert env.etl_q("SELECT count(*) FROM information_schema.tables WHERE table_name = 't_events_web_events'")[0][0] == 0

    bad_schema = pa.schema([("shipment_id", pa.int64(), False), ("order_id", pa.string(), False),
                            ("carrier", pa.string(), True), ("shipped_at", pa.timestamp("us"), True),
                            ("delivered_at", pa.timestamp("us"), True), ("status", pa.string(), True)])
    write_files(env.dir, ships=[(i, str(i), "DHL", datetime(2026, 9, 1), None, "shipped") for i in range(1, 4)],
                schema=bad_schema)
    out2 = env.run(max_retries=3)
    assert out2.status == "failed" and out2.attempts == 1 and "order_id" in out2.error and "type_mismatch" in out2.error

    (env.dir / "shipments.parquet").unlink()
    out3 = env.run(max_retries=3)
    assert out3.status == "failed" and out3.attempts == 1 and "input file missing" in out3.error


def test_drift_alerts_but_still_publishes(env):
    assert env.run().status == "succeeded"
    base = env.baselines()
    ev = clean_events()
    for r in ev:
        r[3] = "page_view" if r[0] % 4 else "purchase"                          # event mix collapses
        r[4] = ""
        r[6] = "desktop"                                                        # device mix collapses
    write_files(env.dir, ev)
    out = env.run()
    assert out.status == "succeeded", out.error
    by = {r.check_name: r for r in env.results(out.run_ids[-1])}
    assert by["drift_event_type"].status == "failed" and by["drift_event_type"].observed >= 0.25
    assert by["drift_device"].status == "failed" and by["drift_carrier"].status == "passed"
    assert env.baselines() == base
    assert env.app_q("SELECT count(*) FROM app.dq_alerts WHERE run_id=:i AND title LIKE '%drift_device%'", i=out.run_ids[-1])[0][0] == 1
