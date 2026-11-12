"""Deterministic synthetic retail data (no real people). Re-running RESETS everything it creates.

Postgres (schema src, written as the etl_rw role via ETL_URL):
    customers, products, orders, order_items      (foreign keys enforced by the database)
Files (under DATA_DIR/files):
    web_events.csv, shipments.parquet             (ids reference the Postgres tables)

Usage (inside the api container):  python /seed/generate_data.py
Options: --customers N --products N --orders N --events N --seed N
"""
import argparse
import csv
import os
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

FIRST = ["Aarav", "Vivaan", "Aditi", "Diya", "Rohan", "Ananya", "Kabir", "Meera", "Arjun", "Isha",
         "Liam", "Olivia", "Noah", "Emma", "Mateo", "Sofia", "Yuki", "Hana", "Omar", "Fatima",
         "Lucas", "Mia", "Ethan", "Zara", "Ravi", "Priya", "Sana", "Dev", "Nina", "Leo"]
LAST = ["Sharma", "Reddy", "Patel", "Iyer", "Khan", "Gupta", "Nair", "Singh", "Smith", "Johnson",
        "Garcia", "Silva", "Tanaka", "Kim", "Nguyen", "Brown", "Martin", "Lopez", "Ali", "Rossi"]
PLACES = [("Hyderabad", "India"), ("Mumbai", "India"), ("Bengaluru", "India"), ("Delhi", "India"),
          ("London", "UK"), ("Manchester", "UK"), ("Austin", "USA"), ("Seattle", "USA"),
          ("Toronto", "Canada"), ("Berlin", "Germany"), ("Sydney", "Australia"), ("Singapore", "Singapore")]
SEGMENTS = (["consumer"] * 70 + ["small_business"] * 22 + ["enterprise"] * 8)
CATEGORIES = {
    "Electronics": ["Headphones", "Charger", "Speaker", "Webcam", "Keyboard", "Mouse"],
    "Home": ["Lamp", "Mug", "Cushion", "Organizer", "Kettle", "Rug"],
    "Fitness": ["Yoga Mat", "Dumbbell", "Bottle", "Band", "Jump Rope"],
    "Books": ["Notebook", "Planner", "Cookbook", "Atlas", "Journal"],
    "Apparel": ["T-Shirt", "Hoodie", "Cap", "Socks", "Jacket"],
}
STATUSES = ["delivered"] * 70 + ["shipped"] * 10 + ["processing"] * 6 + ["cancelled"] * 8 + ["returned"] * 6
CHANNELS = ["web"] * 55 + ["mobile"] * 35 + ["store"] * 10
CARRIERS = ["BlueDart", "Delhivery", "DHL", "FedEx", "RoyalMail"]
EVENT_TYPES = ["page_view"] * 45 + ["product_view"] * 25 + ["add_to_cart"] * 15 + ["checkout_start"] * 8 + ["purchase"] * 7
DEVICES = ["desktop"] * 40 + ["mobile"] * 52 + ["tablet"] * 8

START, END = date(2023, 1, 1), date(2026, 9, 30)

DDL = """
DROP TABLE IF EXISTS src.order_items, src.orders, src.products, src.customers CASCADE;
CREATE TABLE src.customers (
  customer_id integer PRIMARY KEY, first_name text NOT NULL, last_name text NOT NULL,
  email text NOT NULL UNIQUE, phone text, city text NOT NULL, country text NOT NULL,
  segment text NOT NULL, signup_date date NOT NULL);
CREATE TABLE src.products (
  product_id integer PRIMARY KEY, sku text NOT NULL UNIQUE, name text NOT NULL, category text NOT NULL,
  unit_price numeric(10,2) NOT NULL CHECK (unit_price > 0), unit_cost numeric(10,2) NOT NULL CHECK (unit_cost > 0),
  is_active boolean NOT NULL, created_date date NOT NULL);
CREATE TABLE src.orders (
  order_id integer PRIMARY KEY, customer_id integer NOT NULL REFERENCES src.customers(customer_id),
  order_ts timestamp NOT NULL, status text NOT NULL, channel text NOT NULL,
  total_amount numeric(12,2) NOT NULL);
CREATE TABLE src.order_items (
  order_item_id integer PRIMARY KEY, order_id integer NOT NULL REFERENCES src.orders(order_id),
  product_id integer NOT NULL REFERENCES src.products(product_id),
  quantity integer NOT NULL CHECK (quantity > 0), unit_price numeric(10,2) NOT NULL);
CREATE INDEX ON src.orders (customer_id);
CREATE INDEX ON src.order_items (order_id);
CREATE INDEX ON src.order_items (product_id);
"""


def _dsn() -> str:
    url = os.environ.get("ETL_URL")
    if not url:
        sys.exit("ETL_URL is not set (expected postgresql+psycopg://etl_rw:...@host:5432/db)")
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _rand_date(rng: random.Random, lo: date, hi: date) -> date:
    return lo + timedelta(days=rng.randint(0, (hi - lo).days))


def _rand_ts(rng: random.Random, day: date) -> datetime:
    return datetime(day.year, day.month, day.day) + timedelta(seconds=rng.randint(0, 86399))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=20_000)
    ap.add_argument("--products", type=int, default=2_000)
    ap.add_argument("--orders", type=int, default=80_000)
    ap.add_argument("--events", type=int, default=50_000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rng = random.Random(a.seed)

    customers = []
    for cid in range(1, a.customers + 1):
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        city, country = rng.choice(PLACES)
        phone = None if rng.random() < 0.10 else f"+{rng.randint(10, 99)}-{rng.randint(6000000000, 9999999999)}"
        customers.append((cid, fn, ln, f"{fn}.{ln}.{cid}@example.com".lower(), phone, city, country,
                          rng.choice(SEGMENTS), _rand_date(rng, START, date(2026, 6, 30))))
    signup = {c[0]: c[8] for c in customers}

    products = []
    for pid in range(1, a.products + 1):
        cat = rng.choice(list(CATEGORIES))
        price = round(rng.uniform(4, 400), 2)
        products.append((pid, f"SKU-{pid:05d}", f"{rng.choice(CATEGORIES[cat])} {pid}", cat, price,
                         round(price * rng.uniform(0.4, 0.8), 2), rng.random() > 0.05,
                         _rand_date(rng, START, date(2025, 12, 31))))
    price_of = {p[0]: p[4] for p in products}

    orders, items, item_id = [], [], 0
    for oid in range(1, a.orders + 1):
        cid = rng.randint(1, a.customers)
        day = _rand_date(rng, signup[cid], END)
        total = 0.0
        lines = []
        for _ in range(rng.choices([1, 2, 3, 4, 5], weights=[30, 30, 20, 12, 8])[0]):
            item_id += 1
            pid = rng.randint(1, a.products)
            qty = rng.randint(1, 4)
            lines.append((item_id, oid, pid, qty, price_of[pid]))
            total += qty * price_of[pid]
        items.extend(lines)
        orders.append((oid, cid, _rand_ts(rng, day), rng.choice(STATUSES), rng.choice(CHANNELS), round(total, 2)))

    with psycopg.connect(_dsn()) as conn:
        conn.execute(DDL)
        for name, cols, rows in (
            ("customers", "customer_id, first_name, last_name, email, phone, city, country, segment, signup_date", customers),
            ("products", "product_id, sku, name, category, unit_price, unit_cost, is_active, created_date", products),
            ("orders", "order_id, customer_id, order_ts, status, channel, total_amount", orders),
            ("order_items", "order_item_id, order_id, product_id, quantity, unit_price", items),
        ):
            with conn.cursor() as cur, cur.copy(f"COPY src.{name} ({cols}) FROM STDIN") as copy:
                for r in rows:
                    copy.write_row(r)
        conn.commit()

    # ---- files
    out = Path(os.environ.get("DATA_DIR", "./data")) / "files"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "web_events.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "session_id", "customer_id", "event_type", "product_id", "event_ts", "device"])
        for eid in range(1, a.events + 1):
            etype = rng.choice(EVENT_TYPES)
            cid = "" if rng.random() < 0.35 else rng.randint(1, a.customers)  # anonymous visitors
            pid = rng.randint(1, a.products) if etype in ("product_view", "add_to_cart") else ""
            ts = _rand_ts(rng, _rand_date(rng, date(2025, 1, 1), END))
            w.writerow([eid, f"s{rng.randint(1, a.events // 3):07d}", cid, etype, pid, ts.isoformat(), rng.choice(DEVICES)])

    ship_rows = []
    for oid, _cid, ots, status, _ch, _t in orders:
        if status in ("shipped", "delivered", "returned"):
            shipped = ots + timedelta(hours=rng.randint(6, 72))
            delivered = None if status == "shipped" else shipped + timedelta(days=rng.randint(1, 9))
            ship_rows.append((len(ship_rows) + 1, oid, rng.choice(CARRIERS), shipped, delivered, status))
    schema = pa.schema([("shipment_id", pa.int64(), False), ("order_id", pa.int64(), False),
                        ("carrier", pa.string(), False), ("shipped_at", pa.timestamp("us"), False),
                        ("delivered_at", pa.timestamp("us"), True), ("status", pa.string(), False)])
    cols = list(zip(*ship_rows))
    pq.write_table(pa.table({s.name: list(cols[i]) for i, s in enumerate(schema)}, schema=schema),
                   out / "shipments.parquet")

    total_pg = len(customers) + len(products) + len(orders) + len(items)
    print(f"customers={len(customers)} products={len(products)} orders={len(orders)} order_items={len(items)}")
    print(f"web_events.csv={a.events} shipments.parquet={len(ship_rows)}")
    print(f"TOTAL records: postgres={total_pg} files={a.events + len(ship_rows)} overall={total_pg + a.events + len(ship_rows)}")


if __name__ == "__main__":
    main()
