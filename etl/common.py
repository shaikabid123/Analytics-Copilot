
"""Shared ETL plumbing: errors, config, Spark session, JDBC/psycopg helpers, cleaning helpers.

Conventions
  * Spark reads sources with the read-only `source_ro` role and writes with `etl_rw`.
  * ANSI mode is OFF on purpose: invalid casts yield NULL, which explicit validation
    rules can quarantine with a reason instead of crashing the whole job.
  * Table names/identifiers used in SQL are validated against a strict pattern first.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from sqlalchemy.engine import make_url

JDBC_DRIVER = "org.postgresql.Driver"
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")


# ----------------------------------------------------------------------------- errors
class PipelineError(Exception):
    """Base class. `retryable` tells the runner whether another attempt could help."""

    retryable = False


class ContractError(PipelineError):
    """Input does not match the expected schema. Retrying cannot fix it."""


class DQGateError(PipelineError):
    """A blocking data-quality check failed. Gold was NOT published."""


class TransientError(PipelineError):
    """Infrastructure hiccup (DB/network/IO). Worth retrying with backoff."""

    retryable = True


def classify(exc: BaseException) -> PipelineError | None:
    """Map raw exceptions to retryable/non-retryable pipeline errors."""
    if isinstance(exc, PipelineError):
        return exc

    text = f"{type(exc).__name__}: {exc}"
    transient_markers = (
        "Connection refused",
        "connection to server",
        "java.net.",
        "I/O error",
        "Connection reset",
        "terminating connection",
        "timeout",
        "Timeout",
    )

    if (
        isinstance(exc, (psycopg.OperationalError, ConnectionError, TimeoutError))
        or any(marker in text for marker in transient_markers)
    ):
        error = TransientError(text[:500])
        error.__cause__ = exc
        return error

    return None


# ----------------------------------------------------------------------------- config
def _files_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data")) / "files"


@dataclass
class PipelineConfig:
    pipeline_name: str
    prefix: str = ""
    src_tables: dict[str, str] = field(
        default_factory=lambda: {
            "customers": "src.customers",
            "products": "src.products",
            "orders": "src.orders",
            "order_items": "src.order_items",
        }
    )
    files_dir: Path = field(default_factory=_files_dir)
    as_of: date = field(default_factory=date.today)
    max_reject_pct: float = 2.0
    max_total_mismatch_pct: float = 0.1
    drift_warn: float = 0.10
    drift_fail: float = 0.25
    volume_min_ratio: float = 0.5
    volume_max_ratio: float = 2.0
    sla_days: int = 5

    def out(self, schema: str, name: str) -> str:
        return f"{schema}.{self.prefix}{name}"

    def stage(self, name: str) -> str:
        return f"bronze.stg_{self.prefix}{name}"


def check_ident(name: str) -> str:
    if not _IDENT.fullmatch(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


def _db(var: str):
    url = os.environ.get(var)
    if not url:
        raise PipelineError(f"environment variable {var} is not set")
    return make_url(url)


def _creds(role: str) -> tuple[str, str, str, str]:
    """Return (JDBC URL, username, password, psycopg DSN)."""
    u = _db("ETL_URL")

    if role == "source":
        user = "source_ro"
        pw = os.environ.get("SRC_PG_PASSWORD")
        if not pw:
            raise PipelineError(
                "environment variable SRC_PG_PASSWORD is not set"
            )
    else:
        user, pw = u.username, u.password

    jdbc = (
        f"jdbc:postgresql://{u.host}:{u.port or 5432}/{u.database}"
        "?rewriteBatchedInserts=true"
    )
    dsn = (
        f"host={u.host} port={u.port or 5432} "
        f"dbname={u.database} user={user} password={pw}"
    )
    return jdbc, user, pw, dsn


# ----------------------------------------------------------------------------- Spark + JDBC
def get_spark(app_name: str = "copilot-etl") -> SparkSession:
    """Create a local Spark session with PostgreSQL JDBC available on its classpaths.

    Using extraClassPath avoids Spark distributing the JDBC JAR via spark.jars,
    which triggered a missing-winutils error in this Windows environment.
    """
    jdbc_jar = os.environ.get(
        "POSTGRES_JDBC_JAR",
        "/usr/share/java/postgresql.jar",
    )

    builder = (
        SparkSession.builder
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app_name)
        .config("spark.driver.extraClassPath", jdbc_jar)
        .config("spark.executor.extraClassPath", jdbc_jar)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
    )

    return builder.getOrCreate()


def read_table(
    spark: SparkSession,
    qualified: str,
    role: str = "etl",
    columns: str | None = None,
) -> DataFrame:
    check_ident(qualified)
    jdbc, user, pw, _ = _creds(role)

    if columns and not re.fullmatch(r"[a-z_][a-z0-9_, ]*", columns):
        raise ValueError("unsafe column list")

    target = qualified if not columns else f"(SELECT {columns} FROM {qualified}) q"

    return (
        spark.read.format("jdbc")
        .option("url", jdbc)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", target)
        .option("user", user)
        .option("password", pw)
        .load()
    )


def write_table(df: DataFrame, qualified: str) -> None:
    """Overwrite a table as the ETL database role."""
    check_ident(qualified)
    jdbc, user, pw, _ = _creds("etl")

    (
        df.write.format("jdbc")
        .option("url", jdbc)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", qualified)
        .option("user", user)
        .option("password", pw)
        .option("batchsize", "10000")
        .mode("overwrite")
        .save()
    )


def pg_exec(statements: list[str], role: str = "etl") -> None:
    """Run SQL statements in one transaction."""
    _, _, _, dsn = _creds(role)

    with psycopg.connect(dsn) as conn:
        for statement in statements:
            conn.execute(statement)  # type: ignore[arg-type]
        conn.commit()


def publish_gold(pairs: list[tuple[str, str]]) -> None:
    """Replace Gold tables with staged versions and then drop the staging tables."""
    statements: list[str] = []

    for final, stage in pairs:
        check_ident(final)
        check_ident(stage)
        statements.extend(
            [
                f"CREATE TABLE IF NOT EXISTS {final} (LIKE {stage})",
                f"DELETE FROM {final}",
                f"INSERT INTO {final} SELECT * FROM {stage}",
            ]
        )

    pg_exec(statements)
    pg_exec([f"DROP TABLE IF EXISTS {stage}" for _, stage in pairs])


# ----------------------------------------------------------------------------- cleaning helpers
def first_reason(*rules: tuple) -> F.Column:
    """Return the first matching rejection reason."""
    expr = None

    for condition, reason in rules:
        if expr is None:
            expr = F.when(condition, F.lit(reason))
        else:
            expr = expr.when(condition, F.lit(reason))

    if expr is None:
        return F.lit(None).cast("string")

    return expr.otherwise(F.lit(None).cast("string"))


def dedup(
    df: DataFrame,
    key_cols: list[str],
    data_cols: list[str],
) -> DataFrame:
    """Keep the most complete row per key; break ties deterministically."""
    completeness = sum(
        (
            F.when(F.col(column).isNotNull(), 1).otherwise(0)
            for column in data_cols
        ),
        F.lit(0),
    )

    window = Window.partitionBy(*key_cols).orderBy(
        F.desc("_completeness"),
        F.asc("_row_json"),
    )

    return (
        df.withColumn("_completeness", completeness)
        .withColumn("_row_json", F.to_json(F.struct(*data_cols)))
        .withColumn("_rn", F.row_number().over(window))
        .filter("_rn = 1")
        .drop("_completeness", "_row_json", "_rn")
    )


def blank_to_null(c: str) -> F.Column:
    trimmed = F.trim(F.col(c))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def rejects_frame(
    bad: DataFrame,
    dataset: str,
    key_col: str,
    data_cols: list[str],
    run_id: int,
) -> DataFrame:
    return bad.select(
        F.lit(dataset).alias("dataset"),
        F.col(key_col).cast("string").alias("record_key"),
        F.col("_reject_reason").alias("reason"),
        F.to_json(F.struct(*data_cols)).alias("record_json"),
        F.lit(run_id).alias("_run_id"),
    )
