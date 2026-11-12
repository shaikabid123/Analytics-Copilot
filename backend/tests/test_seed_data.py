"""Verifies the ACTUAL seeded data (run `python /seed/generate_data.py` first). Fails loudly if not seeded."""
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from app.config import get_settings
from app.services.connectors import build_connector


@pytest.fixture(scope="module")
def q(etl_engine):
    def run(sql: str):
        with etl_engine.connect() as c:
            return c.execute(text(sql)).scalar_one()
    try:
        run("SELECT count(*) FROM src.customers")
    except Exception:
        pytest.fail("src tables missing: run `python /seed/generate_data.py` first", pytrace=False)
    return run


def test_named_tables_alone_reach_100k(q):
    n = {t: q(f"SELECT count(*) FROM src.{t}") for t in ("customers", "products", "orders")}
    assert n == {"customers": 20_000, "products": 2_000, "orders": 80_000}
    assert sum(n.values()) >= 100_000


def test_total_records_across_all_datasets(q):
    pg = sum(q(f"SELECT count(*) FROM src.{t}") for t in ("customers", "products", "orders", "order_items"))
    assert pg >= 250_000 and q("SELECT count(*) FROM src.order_items") > 150_000


@pytest.mark.parametrize("child,fk,parent,pk", [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
])
def test_no_orphan_foreign_keys(q, child, fk, parent, pk):
    orphans = q(f"SELECT count(*) FROM src.{child} c LEFT JOIN src.{parent} p ON p.{pk} = c.{fk} WHERE p.{pk} IS NULL")
    assert orphans == 0


def test_every_order_has_items_and_total_matches_items(q):
    assert q("SELECT count(*) FROM src.orders o WHERE NOT EXISTS (SELECT 1 FROM src.order_items i WHERE i.order_id=o.order_id)") == 0
    mismatched = q("""SELECT count(*) FROM src.orders o JOIN (
        SELECT order_id, sum(quantity*unit_price) s FROM src.order_items GROUP BY order_id) i USING (order_id)
        WHERE abs(o.total_amount - i.s) > 0.01""")
    assert mismatched == 0


def test_business_rules(q):
    assert q("SELECT count(DISTINCT email) - count(*) FROM src.customers") == 0
    assert q("SELECT count(*) FROM src.orders o JOIN src.customers c USING (customer_id) WHERE o.order_ts::date < c.signup_date") == 0
    assert q("SELECT count(*) FROM src.products WHERE unit_cost >= unit_price") == 0
    null_phone_pct = q("SELECT 100.0 * count(*) FILTER (WHERE phone IS NULL) / count(*) FROM src.customers")
    assert 5 < null_phone_pct < 15            # realistic missing values, not zero and not most
    statuses = q("SELECT count(DISTINCT status) FROM src.orders")
    assert statuses == 5


def test_files_counts_and_valid_references(q):
    base = Path(get_settings().data_dir)
    events = build_connector("csv", {"path": "files/web_events.csv"}).list_datasets()[0]
    ships = build_connector("parquet", {"path": "files/shipments.parquet"}).list_datasets()[0]
    assert events.row_count == 50_000 and ships.row_count > 50_000

    import csv
    max_c, max_p, anon = q("SELECT max(customer_id) FROM src.customers"), q("SELECT max(product_id) FROM src.products"), 0
    with open(base / "files/web_events.csv", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["customer_id"] == "":
                anon += 1
            else:
                assert 1 <= int(r["customer_id"]) <= max_c
            if r["product_id"] != "":
                assert 1 <= int(r["product_id"]) <= max_p
    assert 0.25 < anon / events.row_count < 0.45        # anonymous visitors exist (edge case)

    t = pq.read_table(base / "files/shipments.parquet").to_pydict()
    order_ids = set(t["order_id"])
    assert len(order_ids) == len(t["order_id"])          # at most one shipment per order
    # every shipped order id must exist and have a shippable status in Postgres
    with_status = q(f"SELECT count(*) FROM src.orders WHERE order_id = ANY(ARRAY{sorted(order_ids)}) "
                    "AND status IN ('shipped','delivered','returned')")
    assert with_status == len(order_ids)
    for status, delivered in zip(t["status"], t["delivered_at"]):
        assert (delivered is None) == (status == "shipped")
