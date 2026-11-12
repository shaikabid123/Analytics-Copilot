"""Safe connection configuration: every unsafe config must be rejected before anything connects."""
import os

import pytest

from app.config import get_settings
from app.services.source_config import SourceConfigError, resolve_data_path, validate_source_config

GOOD_PG = {"host": "postgres", "port": 5432, "database": "copilot", "username": "source_ro",
           "password_env": "SRC_PG_PASSWORD", "schemas": ["src"]}


@pytest.fixture(autouse=True)
def _settings(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_valid_configs_accepted():
    assert validate_source_config("postgres", GOOD_PG).host == "postgres"
    assert validate_source_config("mysql", {**GOOD_PG, "port": 3306, "host": "mysql"}).port == 3306
    assert validate_source_config("csv", {"path": "files/a.csv"}).delimiter == ","
    assert validate_source_config("parquet", {"path": "files/a.parquet"}).path == "files/a.parquet"


@pytest.mark.parametrize("patch,fragment", [
    ({"password": "hunter2"}, "password"),                       # secret in config
    ({"host": "evil.example.com"}, "ALLOWED_SOURCE_HOSTS"),       # not allowlisted
    ({"host": "postgres;rm -rf /"}, "invalid characters"),
    ({"password_env": "DATABASE_URL"}, "SRC_"),                   # must not reference app secrets
    ({"password_env": "src_pg_password"}, "SRC_"),
    ({"port": 0}, "port"), ({"port": 70000}, "port"),
    ({"database": "x'; DROP TABLE y;--"}, "identifier"),
    ({"username": "a b"}, "identifier"),
    ({"schemas": []}, "schemas"),
    ({"schemas": ["ok", "bad-schema"]}, "invalid schema"),
    ({"connect_timeout_s": 999}, "connect_timeout_s"),
])
def test_unsafe_sql_configs_rejected(patch, fragment):
    with pytest.raises(SourceConfigError, match=fragment):
        validate_source_config("postgres", {**GOOD_PG, **patch})


@pytest.mark.parametrize("path", ["/etc/passwd", "\\windows\\x.csv", "C:\\data\\x.csv", "../x.csv",
                                  "files/../../x.csv", ""])
def test_unsafe_file_paths_rejected(path):
    with pytest.raises(SourceConfigError):
        validate_source_config("csv", {"path": path})


def test_wrong_extension_and_unknown_type_rejected():
    with pytest.raises(SourceConfigError, match="must end with"):
        validate_source_config("csv", {"path": "files/a.parquet"})
    with pytest.raises(SourceConfigError, match="must end with"):
        validate_source_config("parquet", {"path": "files/a.csv"})
    with pytest.raises(SourceConfigError, match="unsupported source_type"):
        validate_source_config("mongodb", {})


def test_unknown_keys_and_bad_delimiter_rejected():
    with pytest.raises(SourceConfigError):
        validate_source_config("csv", {"path": "a.csv", "delimiter": "x"})
    with pytest.raises(SourceConfigError):
        validate_source_config("csv", {"path": "a.csv", "extra": 1})


def test_symlink_escape_is_blocked(tmp_path):
    outside = tmp_path.parent / "outside_secret.csv"
    outside.write_text("a\n1\n")
    (tmp_path / "link.csv").symlink_to(outside)
    with pytest.raises(SourceConfigError, match="escapes DATA_DIR"):
        resolve_data_path("link.csv")
    assert resolve_data_path("normal.csv").is_relative_to(tmp_path.resolve())
