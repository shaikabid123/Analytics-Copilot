"""login lockout fields, alert delivery bookkeeping, integration_calls

Revision ID: 0005
Revises: 0004
"""
import sqlalchemy as sa
from alembic import op

from app.models import INTEGRATION_NAMES, INTEGRATION_STATUSES, _in

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None
S = "app"


def upgrade() -> None:
    op.add_column("users", sa.Column("failed_attempts", sa.Integer(), server_default="0", nullable=False), schema=S)
    op.add_column("users", sa.Column("locked_until", sa.DateTime(timezone=True)), schema=S)
    op.add_column("users", sa.Column("last_login_at", sa.DateTime(timezone=True)), schema=S)
    op.add_column("dq_alerts", sa.Column("attempts", sa.Integer(), server_default="0", nullable=False), schema=S)
    op.add_column("dq_alerts", sa.Column("last_error", sa.String(300)), schema=S)
    op.add_column("dq_alerts", sa.Column("next_attempt_at", sa.DateTime(timezone=True)), schema=S)
    op.add_column("dq_alerts", sa.Column("ticket_key", sa.String(64)), schema=S)
    op.create_table(
        "integration_calls",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("integration", sa.String(16), nullable=False),
        sa.Column("operation", sa.String(32), nullable=False),
        sa.Column("target", sa.String(120), nullable=False),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("latency_ms", sa.Integer()),
        sa.Column("http_status", sa.Integer()),
        sa.Column("error", sa.String(300)),
        sa.Column("resource_type", sa.String(32)),
        sa.Column("resource_id", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(_in("integration", INTEGRATION_NAMES), name="ck_integration_calls_name"),
        sa.CheckConstraint(_in("status", INTEGRATION_STATUSES), name="ck_integration_calls_status"),
        schema=S)
    op.create_index("ix_app_integration_calls_created_at", "integration_calls", ["created_at"], schema=S)


def downgrade() -> None:
    op.drop_table("integration_calls", schema=S)
    for c in ("ticket_key", "next_attempt_at", "last_error", "attempts"):
        op.drop_column("dq_alerts", c, schema=S)
    for c in ("last_login_at", "locked_until", "failed_attempts"):
        op.drop_column("users", c, schema=S)
