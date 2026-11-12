import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.main import app
from app.models import AuditEvent, CatalogColumn, CatalogDataset, DataSource, GlossaryTerm, GlossaryTermColumn
from app.services import catalog
from app.services.connectors import ConnectorError
from app.services.source_config import SourceConfigError

SEED = Path(__file__).resolve().parents[2] / "seed"
if not SEED.exists():
    SEED = Path("/seed")

EXPECTED_COLS = {"customers": 9, "products": 8, "orders": 6, "order_items": 5, "web_events": 7, "shipments": 6}


@pytest.fixture(scope="module")
def session(app_engine):
    with Session(app_engine) as s:
        for src in json.loads((SEED / "sources.json").read_text()):
            catalog.register_source(s, name=src["name"], source_type=src["source_type"], config=src["config"],
                                    description=src["description"], actor="pytest")
        s.commit()
        for src in s.scalars(select(DataSource).where(DataSource.name.in_(
                ["retail_pg", "web_events_csv", "shipments_parquet", "gold_pg"]))):
            catalog.sync_source(s, src, actor="pytest")
        catalog.apply_metadata(s, SEED / "catalog_metadata.json", actor="pytest")
        yield s


def _audit_count(s):
    return s.scalar(select(func.count()).select_from(AuditEvent))


def test_catalog_has_expected_datasets_and_columns(session):
    rows = session.execute(select(CatalogDataset.table_name, func.count(CatalogColumn.id)).join(
        CatalogColumn, CatalogColumn.dataset_id == CatalogDataset.id).where(CatalogDataset.layer == "source")
        .group_by(CatalogDataset.table_name)).all()
    assert dict(rows) == EXPECTED_COLS
    gold = session.execute(select(CatalogDataset.table_name, func.count(CatalogColumn.id)).join(
        CatalogColumn, CatalogColumn.dataset_id == CatalogDataset.id).where(CatalogDataset.layer == "gold")
        .group_by(CatalogDataset.table_name)).all()
    # Gold appears only after the ETL pipelines have run (the metadata for it is optional until then).
    assert dict(gold) in ({}, {"daily_sales": 12, "customer_value": 10, "funnel_daily": 12, "delivery_sla": 9})


def test_catalog_row_counts_match_actual_source_counts(session, etl_engine):
    got = {d.table_name: d.row_count for d in session.scalars(select(CatalogDataset))}
    with etl_engine.connect() as c:
        for t in ("customers", "products", "orders", "order_items"):
            assert got[t] == c.execute(text(f"SELECT count(*) FROM src.{t}")).scalar_one()
    assert got["web_events"] == 50_000
    assert sum(got.values()) >= 100_000


def test_curated_metadata_applied(session):
    email = session.scalar(select(CatalogColumn).join(CatalogDataset).where(
        CatalogDataset.table_name == "customers", CatalogColumn.name == "email"))
    assert email.is_pii and "email" in email.description.lower()
    ds = session.scalar(select(CatalogDataset).where(CatalogDataset.table_name == "orders"))
    assert ds.owner_name == "Sales Operations" and ds.description
    undocumented = session.scalar(select(func.count()).select_from(CatalogColumn).where(CatalogColumn.description.is_(None)))
    assert undocumented == 0                      # every column has a description


def test_glossary_terms_link_to_columns(session):
    terms = session.scalar(select(func.count()).select_from(GlossaryTerm))
    assert terms >= 10
    margin = session.scalar(select(GlossaryTerm).where(GlossaryTerm.term == "Gross margin"))
    links = session.scalar(select(func.count()).select_from(GlossaryTermColumn).where(GlossaryTermColumn.term_id == margin.id))
    assert links == 2


def test_search_finds_columns_datasets_and_terms(session):
    hits = catalog.search(session, "customer email")
    assert any(h["kind"] == "column" and h["name"] == "customers.email" for h in hits)
    assert any(h["kind"] == "dataset" and h["name"] == "src.orders" for h in catalog.search(session, "ord"))   # prefix
    assert any(h["kind"] == "glossary" and h["name"] == "Gross margin" for h in catalog.search(session, "margin"))
    assert any(h["name"] == "shipments.delivered_at" for h in catalog.search(session, "delivered"))
    ranks = [h["rank"] for h in hits]
    assert ranks == sorted(ranks, reverse=True)


@pytest.mark.parametrize("q", ["", "   ", "!!!", "'; DROP TABLE app.users; --", "a" * 500, "((&|!:*"])
def test_search_is_safe_on_hostile_input(session, q):
    assert isinstance(catalog.search(session, q), list)        # never raises
    assert session.scalar(text("SELECT count(*) FROM app.roles")) == 3


def test_sync_is_idempotent_and_preserves_curation(session):
    def snapshot():
        return (session.scalar(select(func.count()).select_from(CatalogDataset)),
                session.scalar(select(func.count()).select_from(CatalogColumn)),
                session.scalar(select(func.count()).select_from(GlossaryTermColumn)))
    before = snapshot()
    for src in session.scalars(select(DataSource).where(DataSource.name == "retail_pg")):
        summary = catalog.sync_source(session, src, actor="pytest")
        assert summary["new_datasets"] == 0 and summary["new_columns"] == 0 and summary["removed_columns"] == 0
    assert snapshot() == before
    assert session.scalar(select(func.count()).select_from(CatalogColumn).where(CatalogColumn.description.is_(None))) == 0


def test_invalid_registration_stores_nothing_and_valid_one_is_audited(session):
    n_sources, n_audit = session.scalar(select(func.count()).select_from(DataSource)), _audit_count(session)
    with pytest.raises(SourceConfigError):
        catalog.register_source(session, name="evil", source_type="postgres",
                                config={"host": "evil.example.com", "port": 5432, "database": "d", "username": "u",
                                        "password_env": "DATABASE_URL", "schemas": ["x"]})
    with pytest.raises(SourceConfigError):
        catalog.register_source(session, name="Bad Name!", source_type="csv", config={"path": "files/x.csv"})
    session.rollback()
    assert session.scalar(select(func.count()).select_from(DataSource)) == n_sources
    catalog.register_source(session, name="tmp_audit_src", source_type="csv", config={"path": "files/web_events.csv"}, actor="pytest")
    session.commit()
    assert _audit_count(session) == n_audit + 1
    last = session.scalar(select(AuditEvent).order_by(AuditEvent.id.desc()))
    assert (last.action, last.actor_label, last.resource_type) == ("source.register", "pytest", "data_source")
    session.delete(session.scalar(select(DataSource).where(DataSource.name == "tmp_audit_src")))
    session.commit()


def test_failed_sync_is_audited_as_failure(session):
    src = catalog.register_source(session, name="tmp_broken_src", source_type="csv",
                                  config={"path": "files/does_not_exist.csv"}, actor="pytest")
    session.commit()
    try:
        with pytest.raises(ConnectorError):
            catalog.sync_source(session, src, actor="pytest")
        last = session.scalar(select(AuditEvent).order_by(AuditEvent.id.desc()))
        assert last.action == "source.sync" and last.outcome == "failure"
    finally:
        session.delete(session.scalar(select(DataSource).where(DataSource.name == "tmp_broken_src")))
        session.commit()


def test_metadata_with_unknown_references_is_rejected_atomically(session, tmp_path):
    before = session.scalar(select(CatalogDataset.description).where(CatalogDataset.table_name == "orders"))
    bad = {"datasets": {"retail_pg:src.orders": {"owner": "Hacker", "description": "CHANGED", "columns": {
        "no_such_column": {"description": "x"}}}}, "glossary": []}
    f = tmp_path / "bad.json"
    f.write_text(json.dumps(bad))
    with pytest.raises(SourceConfigError, match="column not found"):
        catalog.apply_metadata(session, f, actor="pytest")
    session.expire_all()
    assert session.scalar(select(CatalogDataset.description).where(CatalogDataset.table_name == "orders")) == before


# ---------------------------------------------------------------- API
@pytest.fixture()
def client(session, login):
    """Authenticated client: the catalog API requires a valid token (any role)."""
    _, headers = login("analyst")
    c = TestClient(app)
    c.headers.update(headers)
    return c


def test_api_search_browse_and_glossary(client):
    assert any(h["name"] == "customers.email" for h in client.get("/catalog/search", params={"q": "customer email"}).json())
    ds = client.get("/catalog/datasets", params={"source": "retail_pg"}).json()
    assert {d["table_name"] for d in ds} == {"customers", "products", "orders", "order_items"}
    detail = client.get(f"/catalog/datasets/{ds[0]['id']}").json()
    assert detail["columns"] and all(c["description"] for c in detail["columns"])
    term = client.get("/glossary", params={"q": "margin"}).json()[0]
    assert len(client.get(f"/glossary/{term['id']}").json()["columns"]) == 2


def test_api_validation_and_not_found(client):
    assert client.get("/catalog/search", params={"q": "a"}).status_code == 422
    assert client.get("/catalog/search", params={"q": "x" * 101}).status_code == 422
    assert client.get("/catalog/datasets", params={"layer": "evil"}).status_code == 422
    assert client.get("/catalog/datasets", params={"limit": 9999}).status_code == 422
    assert client.get("/catalog/datasets/999999").status_code == 404
    assert client.get("/glossary/999999").status_code == 404


def test_api_never_exposes_config_or_credentials(client):
    body = client.get("/sources").text.lower()
    for forbidden in ("password", "host", "src_pg", "source_ro", "config", "5432"):
        assert forbidden not in body
