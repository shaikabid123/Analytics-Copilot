"""Deterministic SQL guard. Everything the LLM (or a template) produces passes through here before it
can touch the database; nothing in this module calls an LLM.

Policy (all enforced on the parsed AST, never by string matching alone):
  * exactly one statement, and it is a plain SELECT (WITH ... SELECT allowed; no set operations)
  * no DML/DDL/COPY/SET/transaction commands anywhere in the tree (including inside CTEs)
  * no SELECT INTO, no locking clauses, no comments, no table-valued functions
  * only gold tables the caller's role may read; unqualified names are rewritten to gold.<table>
  * only allow-listed functions (so pg_sleep, pg_read_file, set_config, dblink... cannot appear)
  * no wildcard projections; internal columns (leading underscore) are not selectable
  * a LIMIT is always present and never above MAX_ROWS
"""
from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

MAX_ROWS = 1000
MAX_SQL_CHARS = 4000
MAX_JOINS = 3
GOLD = "gold"

ALL_GOLD_TABLES = frozenset({"daily_sales", "customer_value", "funnel_daily", "delivery_sla"})
ROLE_TABLES: dict[str, frozenset[str]] = {
    "analyst": frozenset({"daily_sales", "funnel_daily", "delivery_sla"}),   # no customer-level table
    "engineer": ALL_GOLD_TABLES,
    "admin": ALL_GOLD_TABLES,
}

# Column allowlist per gold table (internal columns such as _run_id are deliberately absent).
TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "daily_sales": frozenset({"order_date", "channel", "orders", "items", "units", "gross_merchandise_value", "net_revenue",
                              "cancelled_orders", "returned_orders", "avg_order_value", "return_rate"}),
    "customer_value": frozenset({"customer_id", "segment", "country", "orders", "lifetime_value", "first_order_date",
                                 "last_order_date", "avg_order_value", "is_active_90d"}),
    "funnel_daily": frozenset({"event_date", "device", "sessions", "page_views", "product_views", "add_to_carts",
                               "checkout_starts", "purchases", "view_to_cart_rate", "cart_to_purchase_rate", "purchase_rate"}),
    "delivery_sla": frozenset({"ship_month", "carrier", "shipments", "delivered", "avg_lead_days", "p90_lead_days",
                               "on_time_pct", "sla_days"}),
}
assert set(TABLE_COLUMNS) == ALL_GOLD_TABLES

ALLOWED_FUNCTIONS = frozenset({
    "SUM", "COUNT", "AVG", "MIN", "MAX", "ROUND", "COALESCE", "NULLIF", "ABS", "GREATEST", "LEAST",
    "TIMESTAMP_TRUNC", "EXTRACT", "CAST", "LOWER", "UPPER", "CEIL", "FLOOR", "TRUNC", "LENGTH",
    "SUBSTRING", "CONCAT", "TIME_TO_STR", "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD",
    "PERCENTILE_CONT", "CURRENT_DATE",
})

_FORBIDDEN_NODE_NAMES = (
    "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "Command", "Copy", "Set", "Use",
    "Commit", "Rollback", "Transaction", "Into", "Lock", "Union", "Intersect", "Except", "Lateral",
    "Unnest", "Values", "Pragma", "Grant", "Show", "Describe", "TruncateTable", "AlterTable",
)
_FORBIDDEN_NODES = tuple(getattr(exp, n) for n in _FORBIDDEN_NODE_NAMES if hasattr(exp, n))


@dataclass
class GuardResult:
    ok: bool
    sql: str | None = None                       # normalised, table-qualified, limit-enforced SQL
    errors: list[str] = field(default_factory=list)
    tables: set[str] = field(default_factory=set)
    limit_applied: int | None = None


def _ordered(errors: list[str]) -> list[str]:
    """Root causes first: table/schema problems, then the rest alphabetically."""
    return sorted(set(errors), key=lambda e: (not e.startswith(("table", "schema", "cross-database")), e))


def _fail(*errors: str) -> GuardResult:
    return GuardResult(False, None, list(errors))


def validate_sql(sql: object, role: str) -> GuardResult:
    """Never raises: any unexpected internal error rejects the query (fail closed)."""
    try:
        return _validate(sql, role)
    except Exception:  # noqa: BLE001 - deliberately broad: a guard must fail closed
        return _fail("the query could not be validated and was rejected")


def _validate(sql: object, role: str) -> GuardResult:
    if not isinstance(sql, str) or not sql.strip():
        return _fail("empty SQL")
    if role not in ROLE_TABLES:
        return _fail(f"unknown role '{role}': access denied")
    raw = sql.strip()
    if len(raw) > MAX_SQL_CHARS:
        return _fail(f"SQL longer than {MAX_SQL_CHARS} characters")
    for token, why in (("--", "SQL comments are not allowed"), ("/*", "SQL comments are not allowed"),
                       ("\x00", "NUL byte"), ("$$", "dollar quoting is not allowed")):
        if token in raw:
            return _fail(why)
    try:
        trees = [t for t in sqlglot.parse(raw, dialect="postgres") if t is not None]
    except SqlglotError as exc:
        return _fail(f"SQL could not be parsed: {str(exc).splitlines()[0][:120]}")
    if len(trees) != 1:
        return _fail("exactly one SQL statement is allowed")
    root = trees[0]
    if not isinstance(root, exp.Select):
        return _fail("only a single SELECT statement is allowed")

    errors: list[str] = []
    for node in root.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            errors.append(f"{type(node).__name__.upper()} is not allowed (read-only SELECT only)")
    if errors:
        return _fail(*sorted(set(errors)))

    allowed_tables = ROLE_TABLES[role]
    cte_names = {c.alias_or_name.lower() for c in root.find_all(exp.CTE)}
    tables: set[str] = set()
    alias_to_table: dict[str, str] = {}          # alias (or table name) -> real gold table
    for tb in root.find_all(exp.Table):
        if not isinstance(tb.this, exp.Identifier):
            errors.append("table-valued functions are not allowed")
            continue
        if tb.catalog:
            errors.append("cross-database references are not allowed")
            continue
        name = tb.name if tb.this.quoted else tb.name.lower()
        db = (tb.db if tb.args["db"].quoted else tb.db.lower()) if tb.args.get("db") else ""
        if not db and name in cte_names:
            continue
        if db and db != GOLD:
            errors.append(f"schema '{db}' is not allowed: only gold tables can be queried")
        elif name in allowed_tables:
            tb.set("this", exp.to_identifier(name))
            tb.set("db", exp.to_identifier(GOLD))
            tables.add(f"{GOLD}.{name}")
            alias_to_table[(tb.alias or name).lower()] = name
        elif name in ALL_GOLD_TABLES:
            errors.append(f"table '{GOLD}.{name}' is not permitted for role '{role}'")
        else:
            errors.append(f"table '{(db + '.' if db else '') + name}' is not allowed")

    for f in root.find_all(exp.Func):
        fname = (f.name if isinstance(f, exp.Anonymous) else f.sql_name()).upper()
        if fname not in ALLOWED_FUNCTIONS:
            errors.append(f"function {fname} is not allowed")
    for star in root.find_all(exp.Star):
        if not isinstance(star.parent, exp.Count):
            errors.append("wildcard (*) is not allowed: list the columns explicitly")
            break
    table_errors = bool(errors)
    derived = set(cte_names) | {sq.alias.lower() for sq in root.find_all(exp.Subquery) if sq.alias}
    out_names = {a.alias.lower() for a in root.find_all(exp.Alias) if a.alias}
    for cte in root.find_all(exp.CTE):
        alias = cte.args.get("alias")
        out_names |= {c.name.lower() for c in (alias.columns if alias else [])}
    visible = set().union(*(TABLE_COLUMNS[t.split(".")[1]] for t in tables)) if tables else set()
    for col in ([] if table_errors else root.find_all(exp.Column)):
        if not isinstance(col.this, exp.Identifier):          # t.* style wildcard: reported by the wildcard rule
            continue
        cname = col.name if col.this.quoted else col.name.lower()
        qual = (col.table if col.args["table"] is not None and col.args["table"].quoted else col.table.lower()) if col.table else ""
        if cname.startswith("_"):
            errors.append(f"column '{cname}' is internal and not selectable")
        elif qual:
            if qual in alias_to_table:
                if cname not in TABLE_COLUMNS[alias_to_table[qual]]:
                    errors.append(f"column '{cname}' does not exist on table gold.{alias_to_table[qual]} (or is not permitted)")
            elif qual not in derived:
                errors.append(f"unknown table or alias '{qual}'")
        elif cname not in visible and cname not in out_names:
            errors.append(f"column '{cname}' is unknown or not permitted")
    if len(list(root.find_all(exp.Join))) > MAX_JOINS:
        errors.append(f"more than {MAX_JOINS} joins")
    if errors:
        return _fail(*_ordered(errors))
    if not tables:
        return _fail("the query must read at least one permitted gold table")

    limit_node = root.args.get("limit")
    applied: int
    if limit_node is None:
        applied = MAX_ROWS
        root.limit(applied, copy=False)
    else:
        expr = limit_node.expression
        if not (isinstance(expr, exp.Literal) and expr.is_int):
            return _fail("LIMIT must be a plain integer")
        applied = min(int(expr.name), MAX_ROWS)
        root.limit(applied, copy=False)
    return GuardResult(True, root.sql(dialect="postgres"), [], tables, applied)
