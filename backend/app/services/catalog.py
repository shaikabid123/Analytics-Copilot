"""Catalog service: register sources, sync schemas into the catalog, apply curated metadata, search."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session

from app import audit
from app.models import (
    CatalogColumn, CatalogDataset, DataSource, GlossaryTerm, GlossaryTermColumn,
)
from app.services.connectors import build_connector
from app.services.source_config import SourceConfigError, validate_source_config

_WORD = re.compile(r"[A-Za-z0-9]+")


def _layer_for(schema_name: str) -> str:
    return schema_name if schema_name in ("bronze", "silver", "gold") else "source"


# --------------------------------------------------------------------------- sources
def register_source(session: Session, *, name: str, source_type: str, config: dict,
                    description: str | None = None, actor: str = "system") -> DataSource:
    """Validate and upsert a data source. Invalid/unsafe config raises SourceConfigError (nothing is stored)."""
    if not re.match(r"^[a-z][a-z0-9_]{1,63}$", name):
        raise SourceConfigError("name must be lowercase letters/digits/underscore, 2-64 chars, starting with a letter")
    validate_source_config(source_type, config)
    src = session.scalar(select(DataSource).where(DataSource.name == name))
    created = src is None
    if created:
        src = DataSource(name=name, source_type=source_type, config=config, description=description)
        session.add(src)
    else:
        src.source_type, src.config, src.description = source_type, config, description
    session.flush()
    audit.record(session, actor=actor, action="source.register" if created else "source.update",
                 resource_type="data_source", resource_id=src.id, detail={"name": name, "type": source_type})
    return src


def sync_source(session: Session, source: DataSource, actor: str = "system") -> dict:
    """Discover datasets/columns from the source and upsert into the catalog.
    Curated fields (descriptions, owner, PII flags) are preserved across syncs."""
    try:
        datasets = build_connector(source.source_type, source.config).list_datasets()
    except Exception as exc:
        audit.record(session, actor=actor, action="source.sync", resource_type="data_source",
                     resource_id=source.id, outcome="failure",
                     detail={"error": f"{type(exc).__name__}: {exc}"[:300]})
        session.commit()
        raise
    now = datetime.now(timezone.utc)
    added_ds = added_cols = removed_cols = 0
    for info in datasets:
        ds = session.scalar(select(CatalogDataset).where(
            CatalogDataset.source_id == source.id, CatalogDataset.schema_name == info.schema_name,
            CatalogDataset.table_name == info.table_name))
        if ds is None:
            ds = CatalogDataset(source_id=source.id, schema_name=info.schema_name,
                                table_name=info.table_name, layer=_layer_for(info.schema_name))
            session.add(ds)
            session.flush()
            added_ds += 1
        ds.layer, ds.row_count, ds.last_synced_at = _layer_for(info.schema_name), info.row_count, now
        existing = {c.name: c for c in session.scalars(select(CatalogColumn).where(CatalogColumn.dataset_id == ds.id))}
        seen = set()
        for col in info.columns:
            seen.add(col.name)
            row = existing.get(col.name)
            if row is None:
                session.add(CatalogColumn(dataset_id=ds.id, name=col.name, data_type=col.data_type,
                                          is_nullable=bool(col.is_nullable), ordinal=col.ordinal))
                added_cols += 1
            else:
                row.data_type, row.is_nullable, row.ordinal = col.data_type, bool(col.is_nullable), col.ordinal
        for name, row in existing.items():
            if name not in seen:
                session.delete(row)
                removed_cols += 1
    session.flush()
    summary = {"datasets": len(datasets), "new_datasets": added_ds, "new_columns": added_cols,
               "removed_columns": removed_cols, "rows_total": sum(d.row_count or 0 for d in datasets)}
    audit.record(session, actor=actor, action="source.sync", resource_type="data_source",
                 resource_id=source.id, detail={"name": source.name, **summary})
    session.commit()
    return summary


# --------------------------------------------------------------------------- curated metadata
class _ColMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    description: str = Field(max_length=500)
    pii: bool = False


class _DsMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    owner: str = Field(max_length=128)
    description: str = Field(max_length=1000)
    optional: bool = False          # skip (instead of erroring) while the dataset is not in the catalog yet
    columns: dict[str, _ColMeta] = {}


class _Term(BaseModel):
    model_config = ConfigDict(extra="forbid")
    term: str = Field(min_length=2, max_length=128)
    definition: str = Field(min_length=5, max_length=1000)
    columns: list[str] = []  # "source:schema.table.column"


class MetadataFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    datasets: dict[str, _DsMeta]  # key "source:schema.table"
    glossary: list[_Term] = []


def apply_metadata(session: Session, path: str | Path, actor: str = "system") -> dict:
    """Apply owners, descriptions, PII flags and glossary links. Idempotent. Unknown references are
    reported as errors (and nothing is committed) rather than silently ignored."""
    meta = MetadataFile.model_validate(json.loads(Path(path).read_text(encoding="utf-8")))
    errors: list[str] = []
    skipped: set[str] = set()

    def find_dataset(key: str) -> CatalogDataset | None:
        m = re.match(r"^([a-z0-9_]+):([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$", key)
        if not m:
            errors.append(f"bad dataset key {key!r}")
            return None
        return session.scalar(
            select(CatalogDataset).join(DataSource, DataSource.id == CatalogDataset.source_id).where(
                DataSource.name == m[1], CatalogDataset.schema_name == m[2], CatalogDataset.table_name == m[3]))

    for key, dm in meta.datasets.items():
        ds = find_dataset(key)
        if ds is None:
            if dm.optional:
                skipped.add(key)
            else:
                errors.append(f"dataset not in catalog (sync first?): {key}")
            continue
        ds.owner_name, ds.description = dm.owner, dm.description
        cols = {c.name: c for c in session.scalars(select(CatalogColumn).where(CatalogColumn.dataset_id == ds.id))}
        for cname, cm in dm.columns.items():
            if cname not in cols:
                errors.append(f"column not found: {key}.{cname}")
                continue
            cols[cname].description, cols[cname].is_pii = cm.description, cm.pii

    linked = 0
    for t in meta.glossary:
        term = session.scalar(select(GlossaryTerm).where(GlossaryTerm.term == t.term))
        if term is None:
            term = GlossaryTerm(term=t.term, definition=t.definition)
            session.add(term)
        else:
            term.definition = t.definition
        session.flush()
        session.execute(delete(GlossaryTermColumn).where(GlossaryTermColumn.term_id == term.id))
        for ref in t.columns:
            dkey, _, cname = ref.rpartition(".")
            if dkey in skipped:
                continue
            ds = find_dataset(dkey)
            col = session.scalar(select(CatalogColumn).where(
                CatalogColumn.dataset_id == ds.id, CatalogColumn.name == cname)) if ds else None
            if col is None:
                errors.append(f"glossary '{t.term}' references unknown column {ref}")
                continue
            session.add(GlossaryTermColumn(term_id=term.id, column_id=col.id))
            linked += 1
    if errors:
        session.rollback()
        raise SourceConfigError("; ".join(errors))
    audit.record(session, actor=actor, action="catalog.apply_metadata",
                 detail={"datasets": len(meta.datasets), "terms": len(meta.glossary), "links": linked})
    session.commit()
    return {"datasets": len(meta.datasets) - len(skipped), "skipped_optional": sorted(skipped),
            "terms": len(meta.glossary), "links": linked}


# --------------------------------------------------------------------------- search
def build_prefix_tsquery(q: str) -> str:
    """Sanitise free text into a safe prefix tsquery ('cust:* & email:*'). Only [A-Za-z0-9] survives."""
    words = _WORD.findall(q)[:8]
    return " & ".join(f"{w.lower()}:*" for w in words)


_SEARCH_SQL = text("""
SELECT * FROM (
  SELECT 'dataset' AS kind, d.id AS id, d.id AS dataset_id,
         d.schema_name || '.' || d.table_name AS name, d.description AS snippet,
         ts_rank(d.search_vector, q) AS rank
  FROM app.catalog_datasets d, to_tsquery('english', :q) q WHERE d.search_vector @@ q
  UNION ALL
  SELECT 'column', c.id, d.id, d.table_name || '.' || c.name,
         coalesce(c.description, c.data_type), ts_rank(c.search_vector, q)
  FROM app.catalog_columns c JOIN app.catalog_datasets d ON d.id = c.dataset_id,
       to_tsquery('english', :q) q WHERE c.search_vector @@ q
  UNION ALL
  SELECT 'glossary', g.id, NULL, g.term, g.definition, ts_rank(g.search_vector, q)
  FROM app.glossary_terms g, to_tsquery('english', :q) q WHERE g.search_vector @@ q
) r ORDER BY rank DESC, name LIMIT :limit
""")


def search(session: Session, q: str, limit: int = 20) -> list[dict]:
    tsq = build_prefix_tsquery(q)
    if not tsq:
        return []
    rows = session.execute(_SEARCH_SQL, {"q": tsq, "limit": max(1, min(limit, 100))})
    return [dict(r._mapping) for r in rows]
