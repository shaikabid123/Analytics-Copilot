"""catalog full-text search + dataset owner name

Revision ID: 0002
Revises: 0001
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.models import SEARCH_COLUMN, SEARCH_DATASET, SEARCH_GLOSSARY

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("catalog_datasets", sa.Column("owner_name", sa.String(128), nullable=True), schema="app")
    op.add_column("catalog_datasets", sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True), schema="app")
    for table, expr, ix in (
        ("catalog_datasets", SEARCH_DATASET, "ix_catalog_datasets_search"),
        ("catalog_columns", SEARCH_COLUMN, "ix_catalog_columns_search"),
        ("glossary_terms", SEARCH_GLOSSARY, "ix_glossary_terms_search"),
    ):
        op.add_column(table, sa.Column("search_vector", postgresql.TSVECTOR(),
                                       sa.Computed(expr, persisted=True), nullable=True), schema="app")
        op.create_index(ix, table, ["search_vector"], schema="app", postgresql_using="gin")


def downgrade() -> None:
    for table, ix in (("glossary_terms", "ix_glossary_terms_search"),
                      ("catalog_columns", "ix_catalog_columns_search"),
                      ("catalog_datasets", "ix_catalog_datasets_search")):
        op.drop_index(ix, table_name=table, schema="app", postgresql_using="gin")
        op.drop_column(table, "search_vector", schema="app")
    op.drop_column("catalog_datasets", "last_synced_at", schema="app")
    op.drop_column("catalog_datasets", "owner_name", schema="app")
