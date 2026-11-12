"""agent support: llm_calls, query_audits, pipelines.params, approval actor labels

Revision ID: 0004
Revises: 0003
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.models import LLM_CALL_STATUSES, QUERY_STATUSES, _in

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None
S = "app"


def upgrade() -> None:
    op.add_column("pipelines", sa.Column("params", postgresql.JSONB(), server_default="{}", nullable=False), schema=S)
    op.add_column("approvals", sa.Column("requested_by_label", sa.String(64)), schema=S)
    op.add_column("approvals", sa.Column("decided_by_label", sa.String(64)), schema=S)
    op.drop_constraint("ck_approvals_decision_recorded", "approvals", schema=S)
    op.create_check_constraint(
        "ck_approvals_decision_recorded", "approvals",
        "status = 'pending' OR ((decided_by IS NOT NULL OR decided_by_label IS NOT NULL) AND decided_at IS NOT NULL)",
        schema=S)
    op.create_table(
        "llm_calls",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("agent_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey(f"{S}.agent_runs.id", ondelete="SET NULL")),
        sa.Column("agent", sa.String(32), nullable=False),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("model", sa.String(64)),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("latency_ms", sa.Integer()),
        sa.Column("prompt_tokens", sa.Integer()),
        sa.Column("completion_tokens", sa.Integer()),
        sa.Column("error", sa.String(300)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(_in("status", LLM_CALL_STATUSES), name="ck_llm_calls_status"),
        schema=S)
    op.create_index("ix_app_llm_calls_agent_run_id", "llm_calls", ["agent_run_id"], schema=S)
    op.create_table(
        "query_audits",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("agent_run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey(f"{S}.agent_runs.id", ondelete="SET NULL")),
        sa.Column("actor", sa.String(64), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("question", sa.Text()),
        sa.Column("generated_sql", sa.Text(), nullable=False),
        sa.Column("executed_sql", sa.Text()),
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("block_reasons", postgresql.JSONB(), server_default="[]", nullable=False),
        sa.Column("estimated_cost", sa.Float()),
        sa.Column("row_count", sa.Integer()),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(_in("status", QUERY_STATUSES), name="ck_query_audits_status"),
        schema=S)
    op.create_index("ix_app_query_audits_agent_run_id", "query_audits", ["agent_run_id"], schema=S)
    op.create_index("ix_app_query_audits_created_at", "query_audits", ["created_at"], schema=S)


def downgrade() -> None:
    op.drop_table("query_audits", schema=S)
    op.drop_table("llm_calls", schema=S)
    op.drop_constraint("ck_approvals_decision_recorded", "approvals", schema=S)
    op.create_check_constraint("ck_approvals_decision_recorded", "approvals",
                               "status = 'pending' OR (decided_by IS NOT NULL AND decided_at IS NOT NULL)", schema=S)
    op.drop_column("approvals", "decided_by_label", schema=S)
    op.drop_column("approvals", "requested_by_label", schema=S)
    op.drop_column("pipelines", "params", schema=S)
