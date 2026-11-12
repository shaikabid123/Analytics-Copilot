from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from app.config import get_settings
from app.models import SCHEMA, Base

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata


# Created and versioned by LangGraph's PostgresSaver.setup(), not by Alembic.
LANGGRAPH_TABLES = {"checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations"}


def _include_object(obj, name, type_, reflected, compare_to):
    # Only manage the "app" schema; bronze/silver/gold belong to the ETL role.
    if type_ == "table":
        return obj.schema == SCHEMA and name not in LANGGRAPH_TABLES
    if type_ == "index" and getattr(obj, "table", None) is not None and obj.table.name in LANGGRAPH_TABLES:
        return False
    return True


def run_migrations_online() -> None:
    engine = create_engine(get_settings().database_url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_object=_include_object,
            version_table_schema=SCHEMA,
        )
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
