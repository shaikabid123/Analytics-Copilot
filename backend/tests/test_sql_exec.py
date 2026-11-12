"""Executor tests against the REAL gold tables (run the ETL pipelines first). Uses the analytics_ro role."""
import pytest
from sqlalchemy import text

from app.services import sql_exec
from app.services.sql_exec import CostExceeded, GuardRejected, QueryError, run_guarded_query


@pytest.fixture(scope="module", autouse=True)
def _gold_present(ro_engine):
    try:
        with ro_engine.connect() as c:
            assert c.execute(text("SELECT count(*) FROM gold.daily_sales")).scalar() > 0
    except Exception:
        pytest.fail("gold tables missing: run the ETL pipelines first", pytrace=False)


def test_valid_query_executes_and_matches_direct_sql(ro_engine):
    res = run_guarded_query("SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel ORDER BY channel", "analyst")
    assert res.columns == ["channel", "n"] and res.row_count == 3 and not res.truncated
    with ro_engine.connect() as c:
        direct = c.execute(text("SELECT channel, SUM(orders)::bigint FROM gold.daily_sales GROUP BY channel ORDER BY channel")).all()
    assert [(r[0], int(r[1])) for r in res.rows] == [(d[0], int(d[1])) for d in direct]
    assert res.estimated_cost > 0 and "LIMIT 1000" in res.sql


def test_row_cap_and_truncation_flag():
    res = run_guarded_query("SELECT order_date, channel FROM gold.daily_sales", "analyst")
    assert res.row_count == 1000 and res.truncated is False        # LIMIT 1000 added by the guard, so exactly 1000
    cols, rows, truncated, _ = sql_exec.execute_readonly("SELECT order_date FROM gold.daily_sales", max_rows=10)
    assert len(rows) == 10 and truncated is True


def test_guard_rejection_prevents_any_execution():
    with pytest.raises(GuardRejected) as e:
        run_guarded_query("DELETE FROM gold.daily_sales", "admin")
    assert "SELECT" in str(e.value)
    with pytest.raises(GuardRejected):
        run_guarded_query("SELECT customer_id FROM gold.customer_value", "analyst")


def test_cost_limit_blocks_expensive_cartesian_products():
    # An aggregate must read every joined row, so its planned cost is the true cost.
    sql = "SELECT COUNT(*) AS n FROM gold.daily_sales a CROSS JOIN gold.daily_sales b CROSS JOIN gold.daily_sales c"
    try:
        run_guarded_query(sql, "analyst")
    except CostExceeded as e:
        assert e.cost > sql_exec.MAX_COST
    except Exception as e:                       # an unexplained intermittent failure was seen here before: say exactly what happened
        pytest.fail(f"expected CostExceeded but got {type(e).__name__}: {e}")
    else:
        pytest.fail("the 3-way cross join was allowed to run (no cost limit applied)")
    # A LIMITed cross join stops after 1000 rows, so its planned cost is correctly low (not blocked by cost).
    cheap = "SELECT a.channel FROM gold.daily_sales a CROSS JOIN gold.daily_sales b LIMIT 5"
    assert run_guarded_query(cheap, "analyst").row_count == 5


def test_statement_timeout_is_enforced():
    with pytest.raises(QueryError, match="time limit"):
        sql_exec.execute_readonly("SELECT pg_sleep(5)", timeout_ms=300)   # bypasses the guard on purpose: tests the timeout layer


def test_database_layer_still_blocks_writes_even_if_the_guard_were_bypassed():
    with pytest.raises(QueryError, match="failed"):
        sql_exec.execute_readonly("DELETE FROM gold.daily_sales")
    with pytest.raises(QueryError):
        sql_exec.execute_readonly("SELECT count(*) FROM silver.orders")
    with ro_engine_readable() as c:
        assert c.execute(text("SELECT count(*) FROM gold.daily_sales")).scalar() > 0     # nothing was deleted


def ro_engine_readable():
    return sql_exec.get_ro_engine().connect()


def test_execution_errors_do_not_leak_connection_details():
    with pytest.raises(QueryError) as e:
        run_guarded_query("SELECT nonexistent_col FROM gold.daily_sales", "analyst")
    assert "password" not in str(e.value).lower() and "postgresql://" not in str(e.value)


# ---------------------------------------------------------------- the deterministic fallback must actually WORK
TEMPLATE_QUESTIONS = [
    "total sales", "total sales by month in 2026", "net revenue by channel", "total gmv for web", "how many orders came from mobile",
    "orders by month", "what is the return rate by channel", "return rate by channel in 2026", "top 5 customers by lifetime value",
    "best 3 days by revenue", "average delivery time by carrier", "which carrier has the best on-time delivery",
    "conversion rate by device", "conversion rate on mobile", "show the funnel by device", "funnel for tablet",
]


@pytest.mark.parametrize("question", TEMPLATE_QUESTIONS)
def test_every_template_passes_the_guard_AND_executes_against_the_real_gold_tables(question):
    from app.services.sql_templates import plan_from_template
    plan = plan_from_template(question)
    assert plan is not None, question
    res = run_guarded_query(plan.sql, "engineer")                  # guard + cost cap + read-only execution, not just validation
    assert res.row_count >= 1 and res.columns, (question, plan.name)


def test_template_questions_cover_every_template_name():
    from app.services.sql_templates import plan_from_template
    names = {plan_from_template(q).name for q in TEMPLATE_QUESTIONS}
    assert names == {"total", "by_month", "by_channel", "return_rate_by_channel", "top_customers", "top_days", "lead_time_by_carrier",
                     "on_time_by_carrier", "purchase_rate_by_device", "funnel_totals_by_device"}
