"""Least-privilege checks: what each database role can and cannot do."""
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


def test_analytics_ro_cannot_read_app_tables(ro_engine):
    with pytest.raises(DBAPIError, match="permission denied"):
        with ro_engine.connect() as c:
            c.execute(text("SELECT * FROM app.users"))


def test_analytics_ro_cannot_read_bronze_or_silver(ro_engine):
    for schema in ("src", "bronze", "silver"):
        with pytest.raises(DBAPIError, match="permission denied"):
            with ro_engine.connect() as c:
                c.execute(text(f"SELECT 1 FROM {schema}.nonexistent"))  # denied before lookup


def test_analytics_ro_cannot_create_or_write(ro_engine):
    for stmt in ("CREATE TABLE gold.evil (x int)", "CREATE TABLE public.evil (x int)"):
        with pytest.raises(DBAPIError, match="permission denied|read-only"):
            with ro_engine.begin() as c:
                c.execute(text(stmt))


def test_privileges_block_writes_even_if_read_only_default_is_bypassed():
    """Layer 2: the read-only default can be overridden by a session, so the GRANTs must
    independently stop writes. (The SQL guard also blocks SET statements.)"""
    import os
    from sqlalchemy import create_engine, pool
    # Dedicated engine without pooling so the SET cannot leak into other tests.
    engine = create_engine(os.environ["ANALYTICS_RO_URL"], poolclass=pool.NullPool)
    try:
        for stmt in ("CREATE TABLE gold.evil (x int)", "INSERT INTO app.users (username) VALUES ('x')"):
            with pytest.raises(DBAPIError, match="permission denied"):
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
                    c.execute(text("SET default_transaction_read_only = off"))
                    assert c.execute(text("SHOW default_transaction_read_only")).scalar() == "off"
                    c.execute(text(stmt))
    finally:
        engine.dispose()


def test_analytics_ro_default_is_read_only_and_time_limited(ro_engine):
    with ro_engine.connect() as c:
        assert c.execute(text("SHOW default_transaction_read_only")).scalar() == "on"
        assert c.execute(text("SHOW statement_timeout")).scalar() == "10s"


def test_gold_tables_created_by_etl_are_selectable_by_ro_but_not_writable(etl_engine, ro_engine):
    with etl_engine.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS gold._pytest_probe"))
        c.execute(text("CREATE TABLE gold._pytest_probe AS SELECT 1 AS x"))
    try:
        with ro_engine.connect() as c:
            assert c.execute(text("SELECT x FROM gold._pytest_probe")).scalar() == 1
        with pytest.raises(DBAPIError, match="permission denied|read-only"):
            with ro_engine.begin() as c:
                c.execute(text("INSERT INTO gold._pytest_probe VALUES (2)"))
        with pytest.raises(DBAPIError, match="permission denied|read-only|must be owner"):
            with ro_engine.begin() as c:
                c.execute(text("DROP TABLE gold._pytest_probe"))
    finally:
        with etl_engine.begin() as c:
            c.execute(text("DROP TABLE IF EXISTS gold._pytest_probe"))


def test_etl_role_cannot_touch_app_schema(etl_engine):
    with pytest.raises(DBAPIError, match="permission denied"):
        with etl_engine.connect() as c:
            c.execute(text("SELECT * FROM app.users"))


def _source_ro_engine():
    import os
    from sqlalchemy import create_engine, pool
    from sqlalchemy.engine import make_url
    url = make_url(os.environ["DATABASE_URL"]).set(username="source_ro", password=os.environ["SRC_PG_PASSWORD"])
    return create_engine(url, poolclass=pool.NullPool)


def test_source_ro_reads_src_only_and_cannot_write():
    engine = _source_ro_engine()
    try:
        with engine.connect() as c:
            assert c.execute(text("SELECT count(*) FROM src.customers")).scalar() > 0
        for stmt in ("SELECT 1 FROM app.users", "SELECT 1 FROM gold.nonexistent",
                     "INSERT INTO src.customers (customer_id) VALUES (-1)", "DROP TABLE src.customers",
                     "CREATE TABLE src.evil (x int)"):
            with pytest.raises(DBAPIError, match="permission denied|read-only|must be owner"):
                with engine.begin() as c:
                    c.execute(text(stmt))
    finally:
        engine.dispose()
