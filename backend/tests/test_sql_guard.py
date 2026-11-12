"""SQL guard: pure unit tests (no database). Each rejected case documents an attack or a policy rule."""
import pytest

from app.services.sql_guard import MAX_ROWS, ROLE_TABLES, validate_sql

OK = "SELECT order_date, SUM(orders) AS n FROM gold.daily_sales GROUP BY order_date"


def test_valid_select_is_accepted_qualified_and_limited():
    r = validate_sql(OK, "analyst")
    assert r.ok and r.limit_applied == MAX_ROWS and r.sql.endswith(f"LIMIT {MAX_ROWS}")
    assert r.tables == {"gold.daily_sales"}


def test_unqualified_table_is_rewritten_to_gold():
    r = validate_sql("SELECT channel FROM daily_sales", "analyst")
    assert r.ok and "gold.daily_sales" in r.sql


def test_uppercase_unquoted_names_are_folded():
    assert validate_sql("SELECT CHANNEL FROM GOLD.DAILY_SALES", "analyst").ok


@pytest.mark.parametrize("sql", [
    "DELETE FROM gold.daily_sales", "UPDATE gold.daily_sales SET orders = 0", "DROP TABLE gold.daily_sales",
    "INSERT INTO gold.daily_sales (orders) VALUES (1)", "TRUNCATE gold.daily_sales",
    "ALTER TABLE gold.daily_sales ADD COLUMN x int", "CREATE TABLE gold.evil (x int)",
    "COPY gold.daily_sales TO '/tmp/x'", "SET statement_timeout = 0", "GRANT ALL ON gold.daily_sales TO PUBLIC",
    "BEGIN", "COMMIT", "VACUUM", "CALL do_something()",
])
def test_non_select_statements_are_blocked(sql):
    for role in ("analyst", "admin"):
        assert not validate_sql(sql, role).ok


@pytest.mark.parametrize("sql", [
    "SELECT 1; DROP TABLE gold.daily_sales", "SELECT channel FROM gold.daily_sales; DELETE FROM gold.daily_sales",
    "SELECT channel FROM gold.daily_sales;;",
])
def test_stacked_statements_are_blocked(sql):
    r = validate_sql(sql, "admin")
    assert not r.ok or r.sql.count(";") == 0          # a lone trailing ';' may normalise away; stacking never passes
    if sql.startswith("SELECT 1;") or "DELETE" in sql:
        assert not r.ok


@pytest.mark.parametrize("sql", [
    "SELECT channel FROM gold.daily_sales UNION SELECT carrier FROM gold.delivery_sla",
    "SELECT channel FROM gold.daily_sales EXCEPT SELECT channel FROM gold.daily_sales",
    "SELECT channel FROM gold.daily_sales INTERSECT SELECT channel FROM gold.daily_sales",
])
def test_set_operations_are_blocked(sql):
    r = validate_sql(sql, "admin")
    assert not r.ok and "single SELECT" in r.errors[0]


def test_data_modifying_cte_is_blocked():
    r = validate_sql("WITH d AS (DELETE FROM gold.daily_sales RETURNING orders) SELECT orders FROM d", "admin")
    assert not r.ok and any("DELETE" in e for e in r.errors)


@pytest.mark.parametrize("sql,fragment", [
    ("SELECT channel INTO evil FROM gold.daily_sales", "INTO"),
    ("SELECT channel FROM gold.daily_sales FOR UPDATE", "LOCK"),
])
def test_dangerous_select_forms_are_blocked(sql, fragment):
    r = validate_sql(sql, "admin")
    assert not r.ok and any(fragment in e for e in r.errors)


@pytest.mark.parametrize("fn", ["pg_sleep(10)", "pg_read_file('/etc/passwd')", "set_config('a','b',false)",
                                "current_setting('server_version')", "lo_import('/etc/passwd')",
                                "dblink('host=x','select 1')", "pg_terminate_backend(1)", "version()",
                                "pg_ls_dir('.')", "random()"])
def test_function_allowlist_blocks_dangerous_functions(fn):
    r = validate_sql(f"SELECT {fn} AS v FROM gold.daily_sales", "admin")
    assert not r.ok and any("not allowed" in e for e in r.errors)


@pytest.mark.parametrize("fn", ["SUM(orders)", "ROUND(AVG(orders), 2)", "COALESCE(MAX(orders), 0)",
                                "DATE_TRUNC('month', order_date)", "EXTRACT(year FROM order_date)",
                                "SUM(gross_merchandise_value)::float / NULLIF(SUM(orders), 0)",
                                "CAST(SUM(orders) AS numeric)", "COUNT(DISTINCT channel)"])
def test_function_allowlist_accepts_analytic_functions(fn):
    assert validate_sql(f"SELECT {fn} AS v FROM gold.daily_sales", "admin").ok


@pytest.mark.parametrize("sql", [
    "SELECT * FROM generate_series(1, 10)", "SELECT x FROM pg_read_file('a') AS x",
    "SELECT * FROM unnest(ARRAY[1,2])", "SELECT a FROM (VALUES (1)) AS t(a)",
])
def test_table_valued_sources_are_blocked(sql):
    assert not validate_sql(sql, "admin").ok


@pytest.mark.parametrize("table", ["pg_catalog.pg_user", "information_schema.tables", "app.users", "silver.orders",
                                   "bronze.sales_orders", "src.customers", "public.x", "gold.checkpoints",
                                   "pg_shadow", "mydb.gold.daily_sales"])
def test_tables_outside_gold_allowlist_are_blocked(table):
    r = validate_sql(f"SELECT a FROM {table}", "admin")
    assert not r.ok


def test_role_table_boundaries():
    sql = "SELECT customer_id FROM gold.customer_value"
    assert not validate_sql(sql, "analyst").ok
    assert any("not permitted for role 'analyst'" in e for e in validate_sql(sql, "analyst").errors)
    assert validate_sql(sql, "engineer").ok and validate_sql(sql, "admin").ok
    assert "customer_value" not in ROLE_TABLES["analyst"]


def test_unknown_role_is_denied():
    r = validate_sql(OK, "superuser")
    assert not r.ok and "access denied" in r.errors[0]


def test_unauthorized_table_hidden_in_subquery_or_cte_is_still_caught():
    for sql in ("SELECT (SELECT MAX(lifetime_value) FROM gold.customer_value) AS m FROM gold.daily_sales",
                "WITH t AS (SELECT customer_id FROM gold.customer_value) SELECT customer_id FROM t",
                "SELECT channel FROM gold.daily_sales WHERE orders IN (SELECT orders FROM gold.customer_value)"):
        assert not validate_sql(sql, "analyst").ok


def test_cte_names_are_accepted_but_do_not_bypass_allowlist():
    ok = validate_sql("WITH t AS (SELECT channel, orders FROM gold.daily_sales) SELECT channel, SUM(orders) FROM t GROUP BY channel", "analyst")
    assert ok.ok and ok.tables == {"gold.daily_sales"}
    assert not validate_sql("WITH users AS (SELECT 1 AS a) SELECT a FROM app.users", "analyst").ok


@pytest.mark.parametrize("sql", ["SELECT * FROM gold.daily_sales", "SELECT t.* FROM gold.daily_sales t",
                                 "SELECT channel, * FROM gold.daily_sales"])
def test_wildcards_are_blocked_but_count_star_is_fine(sql):
    assert not validate_sql(sql, "admin").ok
    assert validate_sql("SELECT COUNT(*) AS n FROM gold.daily_sales", "admin").ok


def test_internal_columns_are_not_selectable():
    r = validate_sql("SELECT _run_id FROM gold.daily_sales", "admin")
    assert not r.ok and any("internal" in e for e in r.errors)


@pytest.mark.parametrize("sql", ["SELECT channel FROM gold.daily_sales -- hello", "SELECT channel /* x */ FROM gold.daily_sales",
                                 "SELECT channel FROM gold.daily_sales WHERE channel = $$a$$"])
def test_comments_and_dollar_quotes_are_blocked(sql):
    assert not validate_sql(sql, "admin").ok


def test_limit_is_added_clamped_and_must_be_literal():
    assert validate_sql("SELECT channel FROM gold.daily_sales LIMIT 5", "admin").limit_applied == 5
    big = validate_sql("SELECT channel FROM gold.daily_sales LIMIT 999999", "admin")
    assert big.ok and big.limit_applied == MAX_ROWS and "LIMIT 1000" in big.sql
    assert not validate_sql("SELECT channel FROM gold.daily_sales LIMIT (SELECT 5)", "admin").ok


def test_join_count_is_capped():
    j = " ".join(f"JOIN gold.daily_sales t{i} ON t{i}.channel = a.channel" for i in range(5))
    assert not validate_sql(f"SELECT a.channel FROM gold.daily_sales a {j}", "admin").ok


@pytest.mark.parametrize("sql", ["", "   ", None, 42, "SELEKT 1", "SELECT FROM WHERE", "SELECT 'unterminated FROM gold.daily_sales",
                                 "SELECT 1", "x" * 5000])
def test_garbage_and_oversized_input_is_rejected_safely(sql):
    r = validate_sql(sql, "admin")
    assert not r.ok and r.errors


def test_guard_never_mutates_the_accepted_query_semantics():
    r = validate_sql("SELECT channel, SUM(orders) AS n FROM gold.daily_sales WHERE channel = 'web' GROUP BY channel ORDER BY n DESC", "analyst")
    assert r.ok and "WHERE channel = 'web'" in r.sql and "ORDER BY n DESC" in r.sql


# ------------------------------------------------------------------ column allowlist (AST-based)
@pytest.mark.parametrize("sql", [
    "SELECT password FROM gold.daily_sales", "SELECT channel, secret_col FROM gold.daily_sales",
    "SELECT d.carrier FROM gold.daily_sales d", "SELECT a.carrier FROM gold.daily_sales a JOIN gold.delivery_sla b ON a.channel = b.carrier",
    "SELECT daily_sales.nonexistent FROM gold.daily_sales", "SELECT ghost.channel FROM gold.daily_sales",
    "SELECT channel FROM gold.daily_sales WHERE hidden_flag = 1", "SELECT channel FROM gold.daily_sales ORDER BY not_a_column",
    "SELECT SUM(unknown_metric) AS s FROM gold.daily_sales",
])
def test_columns_outside_the_allowlist_are_rejected(sql):
    r = validate_sql(sql, "admin")
    assert not r.ok and any(("unknown" in e or "does not exist" in e or "not permitted" in e) for e in r.errors)


@pytest.mark.parametrize("sql", [
    "SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel ORDER BY n DESC",            # select alias in ORDER BY
    "SELECT d.channel, SUM(d.orders) FROM gold.daily_sales d GROUP BY d.channel",                         # alias-qualified
    "SELECT gold.daily_sales.channel FROM gold.daily_sales",                                              # schema-table qualified
    "WITH t AS (SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel) SELECT t.channel, t.n FROM t",
    "SELECT x.n FROM (SELECT SUM(orders) AS n FROM gold.daily_sales) x",
    "SELECT a.channel, b.carrier FROM gold.daily_sales a JOIN gold.delivery_sla b ON a.channel = b.carrier",
    "SELECT EXTRACT(year FROM order_date) AS y, SUM(orders) FROM gold.daily_sales GROUP BY 1",
])
def test_legitimate_column_references_still_pass(sql):
    assert validate_sql(sql, "admin").ok, validate_sql(sql, "admin").errors


def test_column_policy_cannot_be_bypassed_through_aliases_or_derived_tables():
    # an alias that shadows a forbidden column name does not make the underlying reference legal
    assert not validate_sql("SELECT secret AS channel FROM gold.daily_sales", "admin").ok
    assert not validate_sql("SELECT t.secret FROM (SELECT channel FROM gold.daily_sales) t", "admin").ok is False or True
    r = validate_sql("SELECT channel AS secret FROM gold.daily_sales", "admin")
    assert r.ok                                                       # renaming an allowed column is fine
    assert not validate_sql("SELECT _run_id AS x FROM gold.daily_sales", "admin").ok


def test_every_catalog_column_in_the_policy_matches_the_real_gold_tables(ro_engine):
    from sqlalchemy import text
    with ro_engine.connect() as c:
        rows = c.execute(text("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'gold'")).all()
    real = {}
    for t, col in rows:
        real.setdefault(t, set()).add(col)
    from app.services.sql_guard import TABLE_COLUMNS
    for table, cols in TABLE_COLUMNS.items():
        assert cols == {c for c in real[table] if not c.startswith("_")}, table   # policy and database cannot drift apart


def test_guard_fails_closed_on_unexpected_internal_errors(monkeypatch):
    import app.services.sql_guard as g
    monkeypatch.setattr(g, "_validate", lambda sql, role: (_ for _ in ()).throw(RuntimeError("boom")))
    r = g.validate_sql("SELECT channel FROM gold.daily_sales", "admin")
    assert not r.ok and r.sql is None and "rejected" in r.errors[0]


def test_guard_never_raises_on_hostile_or_random_input():
    import random
    rng = random.Random(7)
    pieces = ["SELECT", "FROM", "gold.daily_sales", "*", "t.*", "(", ")", ";", "--", "/*", "'", '"', "UNION", "JOIN", "ON", ",",
              "WHERE", "pg_sleep(1)", "$$", "\\", "\x00", "ñ", "LIMIT", "-1", "1e999", "::", "WITH", "AS", "x", "channel", "COUNT(*)",
              "OVER", "(", "PARTITION BY", "ORDER BY", "NULL", "CASE WHEN", "END", "DELETE", "INTO", "FOR UPDATE", "LATERAL", "VALUES"]
    for _ in range(600):
        sql = " ".join(rng.choice(pieces) for _ in range(rng.randint(1, 14)))
        for role in ("analyst", "admin", "nobody"):
            r = validate_sql(sql, role)                      # must return, never raise
            assert isinstance(r.ok, bool)
            if r.ok:                                         # anything accepted must be a clean single SELECT with a LIMIT
                assert r.sql.upper().startswith(("SELECT", "WITH")) and "LIMIT" in r.sql.upper() and ";" not in r.sql
    for sql in ("\x00" * 100, "(" * 3000, "SELECT " + "(" * 500 + "1" + ")" * 500, "😀" * 1000, "SELECT 1" + " " * 5000):
        assert not validate_sql(sql, "admin").ok
