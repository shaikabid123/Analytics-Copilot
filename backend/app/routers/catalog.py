"""READ-ONLY catalog endpoints. Any authenticated user may read metadata (no row data, no source config,
no credentials). State-changing catalog operations are operator CLI only."""
from fastapi import APIRouter, Depends, HTTPException, Query

from app.security import current_user
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import get_session
from app.models import (
    CatalogColumn, CatalogDataset, DataSource, GlossaryTerm, GlossaryTermColumn,
)
from app.services import catalog as svc

router = APIRouter(tags=["catalog"], dependencies=[Depends(current_user)])


class SourceOut(BaseModel):
    id: int
    name: str
    source_type: str
    description: str | None


class ColumnOut(BaseModel):
    id: int
    name: str
    data_type: str
    is_nullable: bool
    is_pii: bool
    description: str | None


class DatasetOut(BaseModel):
    id: int
    source: str
    schema_name: str
    table_name: str
    layer: str
    owner_name: str | None
    description: str | None
    row_count: int | None
    last_synced_at: str | None = None


class DatasetDetail(DatasetOut):
    columns: list[ColumnOut]


class SearchHit(BaseModel):
    kind: str
    id: int
    dataset_id: int | None
    name: str
    snippet: str | None
    rank: float


class GlossaryOut(BaseModel):
    id: int
    term: str
    definition: str


class LinkedColumn(BaseModel):
    dataset: str
    column: str


class GlossaryDetail(GlossaryOut):
    columns: list[LinkedColumn]


def _ds_out(ds: CatalogDataset, source_name: str) -> dict:
    return dict(id=ds.id, source=source_name, schema_name=ds.schema_name, table_name=ds.table_name,
                layer=ds.layer, owner_name=ds.owner_name, description=ds.description, row_count=ds.row_count,
                last_synced_at=ds.last_synced_at.isoformat() if ds.last_synced_at else None)


@router.get("/sources", response_model=list[SourceOut])
def list_sources(session: Session = Depends(get_session)):
    rows = session.scalars(select(DataSource).where(DataSource.is_active).order_by(DataSource.name))
    return [SourceOut(id=s.id, name=s.name, source_type=s.source_type, description=s.description) for s in rows]


@router.get("/catalog/datasets", response_model=list[DatasetOut])
def list_datasets(layer: str | None = Query(None, pattern="^(source|bronze|silver|gold)$"),
                  source: str | None = Query(None, pattern="^[a-z][a-z0-9_]{1,63}$"),
                  limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
                  session: Session = Depends(get_session)):
    stmt = select(CatalogDataset, DataSource.name).join(DataSource, DataSource.id == CatalogDataset.source_id)
    if layer:
        stmt = stmt.where(CatalogDataset.layer == layer)
    if source:
        stmt = stmt.where(DataSource.name == source)
    stmt = stmt.order_by(DataSource.name, CatalogDataset.schema_name, CatalogDataset.table_name).limit(limit).offset(offset)
    return [_ds_out(ds, name) for ds, name in session.execute(stmt)]


@router.get("/catalog/datasets/{dataset_id}", response_model=DatasetDetail)
def get_dataset(dataset_id: int, session: Session = Depends(get_session)):
    row = session.execute(select(CatalogDataset, DataSource.name).join(
        DataSource, DataSource.id == CatalogDataset.source_id).where(CatalogDataset.id == dataset_id)).first()
    if row is None:
        raise HTTPException(404, "dataset not found")
    ds, name = row
    cols = session.scalars(select(CatalogColumn).where(CatalogColumn.dataset_id == ds.id)
                           .order_by(CatalogColumn.ordinal))
    return DatasetDetail(**_ds_out(ds, name), columns=[
        ColumnOut(id=c.id, name=c.name, data_type=c.data_type, is_nullable=c.is_nullable,
                  is_pii=c.is_pii, description=c.description) for c in cols])


@router.get("/catalog/search", response_model=list[SearchHit])
def search_catalog(q: str = Query(..., min_length=2, max_length=100), limit: int = Query(20, ge=1, le=100),
                   session: Session = Depends(get_session)):
    return svc.search(session, q, limit)


@router.get("/glossary", response_model=list[GlossaryOut])
def list_glossary(q: str | None = Query(None, min_length=2, max_length=100), limit: int = Query(100, ge=1, le=200),
                  session: Session = Depends(get_session)):
    if q:
        tsq = svc.build_prefix_tsquery(q)
        if not tsq:
            return []
        stmt = select(GlossaryTerm).where(GlossaryTerm.search_vector.op("@@")(func.to_tsquery("english", tsq)))
    else:
        stmt = select(GlossaryTerm)
    return [GlossaryOut(id=t.id, term=t.term, definition=t.definition)
            for t in session.scalars(stmt.order_by(GlossaryTerm.term).limit(limit))]


@router.get("/glossary/{term_id}", response_model=GlossaryDetail)
def get_term(term_id: int, session: Session = Depends(get_session)):
    t = session.get(GlossaryTerm, term_id)
    if t is None:
        raise HTTPException(404, "term not found")
    rows = session.execute(
        select(CatalogDataset.schema_name, CatalogDataset.table_name, CatalogColumn.name)
        .join(CatalogColumn, CatalogColumn.dataset_id == CatalogDataset.id)
        .join(GlossaryTermColumn, GlossaryTermColumn.column_id == CatalogColumn.id)
        .where(GlossaryTermColumn.term_id == t.id).order_by(CatalogDataset.table_name, CatalogColumn.name))
    return GlossaryDetail(id=t.id, term=t.term, definition=t.definition,
                          columns=[LinkedColumn(dataset=f"{s}.{tb}", column=c) for s, tb, c in rows])
