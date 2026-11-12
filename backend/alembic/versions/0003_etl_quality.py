"""ETL data-quality results, drift baselines, alerts + pipeline definitions

Revision ID: 0003
Revises: 0002
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.models import (
    ALERT_SEVERITIES, ALERT_STATUSES, BASELINE_KINDS, DQ_CHECK_TYPES, DQ_SEVERITIES, DQ_STATUSES, _in,
)

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None
S = "app"


def upgrade() -> None:
    op.create_table(
        "dq_results",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("run_id", sa.BigInteger(), sa.ForeignKey(f"{S}.pipeline_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("dataset", sa.String(96), nullable=False),
        sa.Column("check_name", sa.String(96), nullable=False),
        sa.Column("check_type", sa.String(16), nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("observed", sa.Float()),
        sa.Column("threshold", sa.Float()),
        sa.Column("detail", postgresql.JSONB(), server_default="{}", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(_in("check_type", DQ_CHECK_TYPES), name="ck_dq_results_type"),
        sa.CheckConstraint(_in("severity", DQ_SEVERITIES), name="ck_dq_results_severity"),
        sa.CheckConstraint(_in("status", DQ_STATUSES), name="ck_dq_results_status"),
        schema=S)
    op.create_index("ix_app_dq_results_run_id", "dq_results", ["run_id"], schema=S)
    op.create_table(
        "drift_baselines",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pipeline_id", sa.Integer(), sa.ForeignKey(f"{S}.pipelines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("dataset", sa.String(96), nullable=False),
        sa.Column("column_name", sa.String(96), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("baseline", postgresql.JSONB(), nullable=False),
        sa.Column("created_run_id", sa.BigInteger(), sa.ForeignKey(f"{S}.pipeline_runs.id", ondelete="SET NULL")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("pipeline_id", "dataset", "column_name", name="uq_drift_baseline"),
        sa.CheckConstraint(_in("kind", BASELINE_KINDS), name="ck_drift_baselines_kind"),
        schema=S)
    op.create_table(
        "dq_alerts",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("run_id", sa.BigInteger(), sa.ForeignKey(f"{S}.pipeline_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("dq_result_id", sa.BigInteger(), sa.ForeignKey(f"{S}.dq_results.id", ondelete="CASCADE")),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("detail", postgresql.JSONB(), server_default="{}", nullable=False),
        sa.Column("status", sa.String(8), server_default="pending", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(_in("severity", ALERT_SEVERITIES), name="ck_dq_alerts_severity"),
        sa.CheckConstraint(_in("status", ALERT_STATUSES), name="ck_dq_alerts_status"),
        schema=S)
    op.create_index("ix_app_dq_alerts_run_id", "dq_alerts", ["run_id"], schema=S)
    op.execute("""
    INSERT INTO app.pipelines (name, description, schedule_cron, max_retries) VALUES
      ('sales',  'Postgres src.* -> bronze/silver/gold: daily_sales, customer_value', '0 2 * * *',  3),
      ('events', 'CSV web events + Parquet shipments -> bronze/silver/gold: funnel_daily, delivery_sla', '30 2 * * *', 3)
    ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    op.execute("DELETE FROM app.pipelines WHERE name IN ('sales', 'events')")
    op.drop_index("ix_app_dq_alerts_run_id", table_name="dq_alerts", schema=S)
    op.drop_table("dq_alerts", schema=S)
    op.drop_table("drift_baselines", schema=S)
    op.drop_index("ix_app_dq_results_run_id", table_name="dq_results", schema=S)
    op.drop_table("dq_results", schema=S)
