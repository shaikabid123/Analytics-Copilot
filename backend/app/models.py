"""SQLAlchemy models for the "app" schema (Phase 1 foundation).

Status vocabularies are enforced by CHECK constraints so the database itself
rejects invalid states, independent of application code.
"""
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Computed, DateTime, Float, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, func, text,
)
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

SCHEMA = "app"

ROLE_NAMES = ("analyst", "engineer", "admin")
SOURCE_TYPES = ("postgres", "mysql", "csv", "parquet")
LAYERS = ("source", "bronze", "silver", "gold")
RUN_STATUSES = ("queued", "running", "succeeded", "failed", "retrying")
RUN_TRIGGERS = ("scheduled", "manual", "retry")
APPROVAL_STATUSES = ("pending", "approved", "rejected")
AGENT_STATUSES = ("running", "awaiting_approval", "succeeded", "failed")
DQ_CHECK_TYPES = ("contract", "not_null", "uniqueness", "referential", "validity", "volume", "drift", "reconciliation")
DQ_SEVERITIES = ("error", "warn")          # error = blocks Gold publication; warn = alert only
DQ_STATUSES = ("passed", "warning", "failed")
ALERT_SEVERITIES = ("high", "medium", "low")
ALERT_STATUSES = ("pending", "sent", "failed")
BASELINE_KINDS = ("numeric", "categorical", "volume")
LLM_CALL_STATUSES = ("ok", "error", "timeout")
QUERY_STATUSES = ("executed", "blocked", "failed")
INTEGRATION_NAMES = ("slack", "tickets")
INTEGRATION_STATUSES = ("ok", "timeout", "failed", "rejected")


def _in(col: str, values: tuple[str, ...]) -> str:
    return f"{col} IN ({', '.join(repr(v) for v in values)})"


# Full-text search vectors (generated columns). Underscores become spaces so "order_id" matches "order".
SEARCH_DATASET = (
    "to_tsvector('english'::regconfig, replace(table_name, '_', ' ') || ' ' || schema_name || ' ' || "
    "coalesce(description, '') || ' ' || coalesce(owner_name, ''))")
SEARCH_COLUMN = (
    "to_tsvector('english'::regconfig, replace(name, '_', ' ') || ' ' || coalesce(description, ''))")
SEARCH_GLOSSARY = "to_tsvector('english'::regconfig, term || ' ' || definition)"


class Base(DeclarativeBase):
    pass


class Role(Base):
    __tablename__ = "roles"
    __table_args__ = ({"schema": SCHEMA},)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True)
    description: Mapped[str | None] = mapped_column(Text)


class User(Base):
    __tablename__ = "users"
    __table_args__ = ({"schema": SCHEMA},)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True)
    email: Mapped[str | None] = mapped_column(String(255))
    password_hash: Mapped[str] = mapped_column(String(255))  # hashing added with auth
    role_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.roles.id"))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    failed_attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    role: Mapped[Role] = relationship()


class DataSource(Base):
    __tablename__ = "data_sources"
    __table_args__ = (
        CheckConstraint(_in("source_type", SOURCE_TYPES), name="ck_data_sources_type"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    source_type: Mapped[str] = mapped_column(String(16))
    # Non-secret settings only (paths, host, db name). Credentials live in env vars.
    config: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    description: Mapped[str | None] = mapped_column(Text)
    owner_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CatalogDataset(Base):
    __tablename__ = "catalog_datasets"
    __table_args__ = (
        UniqueConstraint("source_id", "schema_name", "table_name", name="uq_catalog_dataset"),
        CheckConstraint(_in("layer", LAYERS), name="ck_catalog_datasets_layer"),
        Index("ix_catalog_datasets_search", "search_vector", postgresql_using="gin"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.data_sources.id"))
    schema_name: Mapped[str] = mapped_column(String(64))
    table_name: Mapped[str] = mapped_column(String(128))
    layer: Mapped[str] = mapped_column(String(16), server_default="source")
    description: Mapped[str | None] = mapped_column(Text)
    owner_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    owner_name: Mapped[str | None] = mapped_column(String(128))  # free text until users/auth exist
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    search_vector: Mapped[str | None] = mapped_column(TSVECTOR, Computed(SEARCH_DATASET, persisted=True))
    columns: Mapped[list["CatalogColumn"]] = relationship(back_populates="dataset")


class CatalogColumn(Base):
    __tablename__ = "catalog_columns"
    __table_args__ = (
        UniqueConstraint("dataset_id", "name", name="uq_catalog_column"),
        Index("ix_catalog_columns_search", "search_vector", postgresql_using="gin"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.catalog_datasets.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(128))
    data_type: Mapped[str] = mapped_column(String(64))
    is_nullable: Mapped[bool] = mapped_column(Boolean, server_default="true")
    is_pii: Mapped[bool] = mapped_column(Boolean, server_default="false")
    description: Mapped[str | None] = mapped_column(Text)
    ordinal: Mapped[int | None] = mapped_column(Integer)
    search_vector: Mapped[str | None] = mapped_column(TSVECTOR, Computed(SEARCH_COLUMN, persisted=True))
    dataset: Mapped[CatalogDataset] = relationship(back_populates="columns")


class GlossaryTerm(Base):
    __tablename__ = "glossary_terms"
    __table_args__ = (
        Index("ix_glossary_terms_search", "search_vector", postgresql_using="gin"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    term: Mapped[str] = mapped_column(String(128), unique=True)
    definition: Mapped[str] = mapped_column(Text)
    owner_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    search_vector: Mapped[str | None] = mapped_column(TSVECTOR, Computed(SEARCH_GLOSSARY, persisted=True))


class GlossaryTermColumn(Base):
    """Links a business term to the catalog columns that implement it."""
    __tablename__ = "glossary_term_columns"
    __table_args__ = ({"schema": SCHEMA},)
    term_id: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.glossary_terms.id", ondelete="CASCADE"), primary_key=True)
    column_id: Mapped[int] = mapped_column(
        ForeignKey(f"{SCHEMA}.catalog_columns.id", ondelete="CASCADE"), primary_key=True)


class Pipeline(Base):
    __tablename__ = "pipelines"
    __table_args__ = ({"schema": SCHEMA},)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    schedule_cron: Mapped[str | None] = mapped_column(String(64))
    max_retries: Mapped[int] = mapped_column(Integer, server_default="3")
    is_active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    # Approved overrides (set only by the approval flow): max_reject_pct, reset_baseline_next_run
    params: Mapped[dict] = mapped_column(JSONB, server_default="{}")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = (
        CheckConstraint(_in("status", RUN_STATUSES), name="ck_pipeline_runs_status"),
        CheckConstraint(_in("trigger", RUN_TRIGGERS), name="ck_pipeline_runs_trigger"),
        # At most one queued-or-running run per pipeline (enforced by the database, so concurrent requests cannot race).
        Index("uq_pipeline_runs_one_active", "pipeline_id", unique=True,
              postgresql_where=text("status IN ('queued', 'running')")),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.pipelines.id"), index=True)
    status: Mapped[str] = mapped_column(String(16), server_default="queued")
    trigger: Mapped[str] = mapped_column(String(16), server_default="manual")
    attempt: Mapped[int] = mapped_column(Integer, server_default="1")
    retry_of_run_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.pipeline_runs.id"))
    triggered_by: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rows_in: Mapped[int | None] = mapped_column(BigInteger)
    rows_out: Mapped[int | None] = mapped_column(BigInteger)
    error_message: Mapped[str | None] = mapped_column(Text)
    details: Mapped[dict] = mapped_column(JSONB, server_default="{}")


class AgentRun(Base):
    """UI-visible record of one agent-graph execution (LangGraph checkpoints are separate)."""
    __tablename__ = "agent_runs"
    __table_args__ = (
        CheckConstraint(_in("status", AGENT_STATUSES), name="ck_agent_runs_status"),
        {"schema": SCHEMA},
    )
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    thread_id: Mapped[str] = mapped_column(String(64), unique=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    request_text: Mapped[str] = mapped_column(Text)
    request_type: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(24), server_default="running")
    current_node: Mapped[str | None] = mapped_column(String(64))
    state: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Approval(Base):
    __tablename__ = "approvals"
    __table_args__ = (
        CheckConstraint(_in("status", APPROVAL_STATUSES), name="ck_approvals_status"),
        # A decided approval must name who decided and when.
        CheckConstraint(
            "status = 'pending' OR ((decided_by IS NOT NULL OR decided_by_label IS NOT NULL) AND decided_at IS NOT NULL)",
            name="ck_approvals_decision_recorded"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    request_type: Mapped[str] = mapped_column(String(32))  # e.g. "etl_fix"
    status: Mapped[str] = mapped_column(String(16), server_default="pending", index=True)
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey(f"{SCHEMA}.agent_runs.id"))
    pipeline_run_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.pipeline_runs.id"))
    summary: Mapped[str] = mapped_column(Text)
    proposed_change: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    decided_by: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decision_note: Mapped[str | None] = mapped_column(Text)
    requested_by_label: Mapped[str | None] = mapped_column(String(64))   # until real users exist
    decided_by_label: Mapped[str | None] = mapped_column(String(64))


class AuditEvent(Base):
    """Append-only. A database trigger rejects UPDATE, DELETE and TRUNCATE (see migration 0001)."""
    __tablename__ = "audit_events"
    __table_args__ = ({"schema": SCHEMA},)
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.users.id"))
    actor_label: Mapped[str] = mapped_column(String(64))  # username, "system" or agent name
    action: Mapped[str] = mapped_column(String(64), index=True)
    resource_type: Mapped[str | None] = mapped_column(String(64))
    resource_id: Mapped[str | None] = mapped_column(String(64))
    outcome: Mapped[str] = mapped_column(String(16), server_default="success")
    detail: Mapped[dict] = mapped_column(JSONB, server_default="{}")


class DqResult(Base):
    """One data-quality check outcome for one pipeline run."""
    __tablename__ = "dq_results"
    __table_args__ = (
        CheckConstraint(_in("check_type", DQ_CHECK_TYPES), name="ck_dq_results_type"),
        CheckConstraint(_in("severity", DQ_SEVERITIES), name="ck_dq_results_severity"),
        CheckConstraint(_in("status", DQ_STATUSES), name="ck_dq_results_status"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.pipeline_runs.id", ondelete="CASCADE"), index=True)
    dataset: Mapped[str] = mapped_column(String(96))
    check_name: Mapped[str] = mapped_column(String(96))
    check_type: Mapped[str] = mapped_column(String(16))
    severity: Mapped[str] = mapped_column(String(8))
    status: Mapped[str] = mapped_column(String(8))
    observed: Mapped[float | None] = mapped_column(Float)
    threshold: Mapped[float | None] = mapped_column(Float)
    detail: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DriftBaseline(Base):
    """Reference distribution (or row count) a later run is compared against."""
    __tablename__ = "drift_baselines"
    __table_args__ = (
        UniqueConstraint("pipeline_id", "dataset", "column_name", name="uq_drift_baseline"),
        CheckConstraint(_in("kind", BASELINE_KINDS), name="ck_drift_baselines_kind"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.pipelines.id", ondelete="CASCADE"))
    dataset: Mapped[str] = mapped_column(String(96))
    column_name: Mapped[str] = mapped_column(String(96))   # "__rows__" for the volume baseline
    kind: Mapped[str] = mapped_column(String(16))
    baseline: Mapped[dict] = mapped_column(JSONB)
    created_run_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.pipeline_runs.id", ondelete="SET NULL"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DqAlert(Base):
    """Alert raised by a failed/warning check. Delivery (webhook/ticket) arrives with the integrations phase."""
    __tablename__ = "dq_alerts"
    __table_args__ = (
        CheckConstraint(_in("severity", ALERT_SEVERITIES), name="ck_dq_alerts_severity"),
        CheckConstraint(_in("status", ALERT_STATUSES), name="ck_dq_alerts_status"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey(f"{SCHEMA}.pipeline_runs.id", ondelete="CASCADE"), index=True)
    dq_result_id: Mapped[int | None] = mapped_column(ForeignKey(f"{SCHEMA}.dq_results.id", ondelete="CASCADE"))
    severity: Mapped[str] = mapped_column(String(8))
    title: Mapped[str] = mapped_column(String(200))
    detail: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    status: Mapped[str] = mapped_column(String(8), server_default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))          # Slack delivery
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(300))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ticket_key: Mapped[str | None] = mapped_column(String(64))                        # ticketing system reference


class LlmCall(Base):
    """Every LLM request (success or failure): latency and token usage for cost/latency metrics."""
    __tablename__ = "llm_calls"
    __table_args__ = (
        CheckConstraint(_in("status", LLM_CALL_STATUSES), name="ck_llm_calls_status"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey(f"{SCHEMA}.agent_runs.id", ondelete="SET NULL"), index=True)
    agent: Mapped[str] = mapped_column(String(32))
    provider: Mapped[str] = mapped_column(String(16))
    model: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(8))
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(String(300))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class QueryAudit(Base):
    """One row per SQL attempt made on behalf of a user: what was generated, what the guard decided, what ran."""
    __tablename__ = "query_audits"
    __table_args__ = (
        CheckConstraint(_in("status", QUERY_STATUSES), name="ck_query_audits_status"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    agent_run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey(f"{SCHEMA}.agent_runs.id", ondelete="SET NULL"), index=True)
    actor: Mapped[str] = mapped_column(String(64))
    role: Mapped[str] = mapped_column(String(16))
    question: Mapped[str | None] = mapped_column(Text)
    generated_sql: Mapped[str] = mapped_column(Text)
    executed_sql: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(16))             # llm | template
    status: Mapped[str] = mapped_column(String(8))
    block_reasons: Mapped[list] = mapped_column(JSONB, server_default="[]")
    estimated_cost: Mapped[float | None] = mapped_column(Float)
    row_count: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class IntegrationCall(Base):
    """One logical call to an external system (after retries): outcome, attempts, latency. Never stores secrets."""
    __tablename__ = "integration_calls"
    __table_args__ = (
        CheckConstraint(_in("integration", INTEGRATION_NAMES), name="ck_integration_calls_name"),
        CheckConstraint(_in("status", INTEGRATION_STATUSES), name="ck_integration_calls_status"),
        {"schema": SCHEMA},
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    integration: Mapped[str] = mapped_column(String(16))
    operation: Mapped[str] = mapped_column(String(32))
    target: Mapped[str] = mapped_column(String(120))            # host only
    status: Mapped[str] = mapped_column(String(8))
    attempts: Mapped[int] = mapped_column(Integer)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    http_status: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(String(300))
    resource_type: Mapped[str | None] = mapped_column(String(32))
    resource_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
