import csv

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.config import get_settings
from app.services.connectors import (
    ConnectorError, CsvConnector, MySQLConnector, ParquetConnector, PostgresConnector, build_connector,
)


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    yield tmp_path
    get_settings.cache_clear()


def _write_csv(path, rows, header=("id", "amount", "flag", "day", "ts", "note")):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def test_csv_discovery_types_nulls_and_count(data_dir):
    _write_csv(data_dir / "t.csv", [
        (1, "9.50", "true", "2026-01-02", "2026-01-02T10:00:00", "a"),
        (2, "3", "false", "2026-01-03", "2026-01-03 11:30:00", ""),
        (3, "7.25", "true", "2026-01-04", "2026-01-04T12:00:00", "c")])
    conn = build_connector("csv", {"path": "t.csv"})
    assert isinstance(conn, CsvConnector)
    [ds] = conn.list_datasets()
    assert (ds.schema_name, ds.table_name, ds.row_count) == ("file", "t", 3)
    types = {c.name: c.data_type for c in ds.columns}
    assert types == {"id": "integer", "amount": "numeric", "flag": "boolean", "day": "date",
                     "ts": "timestamp", "note": "text"}
    assert {c.name: c.is_nullable for c in ds.columns}["note"] is True
    assert {c.name: c.is_nullable for c in ds.columns}["id"] is False


def test_csv_bad_files_rejected(data_dir):
    (data_dir / "empty.csv").write_text("")
    with pytest.raises(ConnectorError, match="empty"):
        build_connector("csv", {"path": "empty.csv"}).list_datasets()
    (data_dir / "dup.csv").write_text("a,a\n1,2\n")
    with pytest.raises(ConnectorError, match="duplicate"):
        build_connector("csv", {"path": "dup.csv"}).list_datasets()
    (data_dir / "blank.csv").write_text("a,,c\n1,2,3\n")
    with pytest.raises(ConnectorError, match="empty column names"):
        build_connector("csv", {"path": "blank.csv"}).list_datasets()
    with pytest.raises(ConnectorError, match="not found"):
        build_connector("csv", {"path": "missing.csv"}).test_connection()


def test_csv_size_limit_enforced(data_dir):
    _write_csv(data_dir / "big.csv", [(i, "1.5", "true", "2026-01-01", "2026-01-01T00:00:00", "x" * 200)
                                      for i in range(7000)])
    assert (data_dir / "big.csv").stat().st_size > 1024 * 1024
    with pytest.raises(ConnectorError, match="exceeds"):
        build_connector("csv", {"path": "big.csv", "max_file_mb": 1}).list_datasets()


def test_csv_sample_is_clamped_and_restricted(data_dir):
    _write_csv(data_dir / "s.csv", [(i, "1", "true", "2026-01-01", "2026-01-01T00:00:00", "n") for i in range(50)])
    conn = build_connector("csv", {"path": "s.csv"})
    assert len(conn.sample("file", "s", 5)) == 5
    assert len(conn.sample("file", "s", 1000)) == 20          # hard cap
    with pytest.raises(ConnectorError, match="unknown dataset"):
        conn.sample("file", "other", 5)
    with pytest.raises(ConnectorError, match="positive integer"):
        conn.sample("file", "s", 0)


def test_parquet_roundtrip_metadata_and_nulls(data_dir):
    pq.write_table(pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                             "delivered": pa.array([None, "2026-01-01", None], pa.string())}),
                   data_dir / "p.parquet")
    conn = build_connector("parquet", {"path": "p.parquet"})
    assert isinstance(conn, ParquetConnector)
    [ds] = conn.list_datasets()
    assert ds.row_count == 3 and [c.name for c in ds.columns] == ["id", "delivered"]
    assert ds.columns[0].data_type == "int64"
    assert len(conn.sample("file", "p", 2)) == 2
    (data_dir / "bad.parquet").write_bytes(b"not parquet")
    with pytest.raises(ConnectorError, match="cannot read Parquet"):
        build_connector("parquet", {"path": "bad.parquet"}).test_connection()


def test_postgres_failure_message_leaks_no_secret(monkeypatch):
    monkeypatch.setenv("SRC_PG_PASSWORD", "super-secret-pw")
    conn = build_connector("postgres", {"host": "127.0.0.1", "port": 1, "database": "copilot",
                                        "username": "source_ro", "password_env": "SRC_PG_PASSWORD",
                                        "schemas": ["src"], "connect_timeout_s": 1})
    assert isinstance(conn, PostgresConnector)
    with pytest.raises(ConnectorError) as exc:
        conn.test_connection()
    assert "super-secret-pw" not in str(exc.value)


def test_missing_password_env_is_reported_by_name_only(monkeypatch):
    monkeypatch.delenv("SRC_PG_PASSWORD", raising=False)
    conn = build_connector("postgres", {"host": "postgres", "port": 5432, "database": "copilot",
                                        "username": "source_ro", "password_env": "SRC_PG_PASSWORD",
                                        "schemas": ["src"]})
    with pytest.raises(ConnectorError, match="SRC_PG_PASSWORD is not set"):
        conn.test_connection()


def test_sql_sample_rejects_unlisted_schema_and_table():
    conn = build_connector("postgres", {"host": "postgres", "port": 5432, "database": "copilot",
                                        "username": "source_ro", "password_env": "SRC_PG_PASSWORD",
                                        "schemas": ["src"]})
    with pytest.raises(ConnectorError, match="not configured"):
        conn.sample("pg_catalog", "pg_user", 1)


def test_mysql_connector_builds_url_without_live_server(monkeypatch):
    """Interface-level check only: NO MySQL server is contacted (see HANDOFF)."""
    monkeypatch.setenv("SRC_MYSQL_PASSWORD", "p@ss/word")
    conn = build_connector("mysql", {"host": "mysql", "port": 3306, "database": "retail",
                                     "username": "reader", "password_env": "SRC_MYSQL_PASSWORD",
                                     "schemas": ["retail"]})
    assert isinstance(conn, MySQLConnector)
    engine = conn._engine()
    try:
        shown = engine.url.render_as_string(hide_password=True)
        assert shown == "mysql+pymysql://reader:***@mysql:3306/retail"
        assert engine.url.password == "p@ss/word"       # special characters survive URL building
    finally:
        engine.dispose()
