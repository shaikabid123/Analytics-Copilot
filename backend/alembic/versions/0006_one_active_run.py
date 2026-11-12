"""at most one queued/running run per pipeline

Revision ID: 0006
Revises: 0005
"""
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Runs left 'running' by a crashed process more than an hour ago would block the index: mark them failed first.
    op.execute("""UPDATE app.pipeline_runs SET status = 'failed', finished_at = now(),
                  error_message = 'abandoned: runner stopped without finishing (marked by migration 0006)'
                  WHERE status = 'running' AND started_at < now() - interval '1 hour'""")
    op.execute("""CREATE UNIQUE INDEX uq_pipeline_runs_one_active ON app.pipeline_runs (pipeline_id)
                  WHERE status IN ('queued', 'running')""")


def downgrade() -> None:
    op.execute("DROP INDEX app.uq_pipeline_runs_one_active")
