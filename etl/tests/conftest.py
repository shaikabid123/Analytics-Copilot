"""ETL tests need: a migrated DB (alembic upgrade head), the seeded data, Java + the Postgres JDBC jar.
Missing environment fails loudly (never silently skipped)."""
import os

import pytest
from sqlalchemy import create_engine, text


def _need(name):
    if not os.environ.get(name):
        pytest.fail(f"environment variable {name} is required for ETL tests", pytrace=False)
    return os.environ[name]


@pytest.fixture(scope="session")
def app_engine():
    return create_engine(_need("DATABASE_URL"))


@pytest.fixture(scope="session")
def etl_engine():
    return create_engine(_need("ETL_URL"))


@pytest.fixture(scope="session")
def spark():
    for v in ("DATABASE_URL", "ETL_URL", "SRC_PG_PASSWORD", "DATA_DIR"):
        _need(v)
    from etl.common import get_spark
    s = get_spark("etl-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def drop_test_artifacts(etl_engine, app_engine, name_like: str):
    """Remove tables prefixed t_ / stg_t_ / zt_ and the app rows of test pipelines (audit rows are
    insert-only by design and stay)."""
    with etl_engine.begin() as c:
        rows = c.execute(text(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema IN ('bronze','silver','gold','src') "
            "AND (table_name LIKE 't\\_%' OR table_name LIKE 'stg\\_t\\_%' OR table_name LIKE 'zt\\_%')")).all()
        for s, t in rows:
            c.execute(text(f'DROP TABLE IF EXISTS {s}."{t}" CASCADE'))
    with app_engine.begin() as c:
        c.execute(text("DELETE FROM app.pipeline_runs WHERE pipeline_id IN "
                       "(SELECT id FROM app.pipelines WHERE name LIKE :p)"), {"p": name_like})
        c.execute(text("DELETE FROM app.pipelines WHERE name LIKE :p"), {"p": name_like})
