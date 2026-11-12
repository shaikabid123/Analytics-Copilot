import logging

from fastapi import Depends, FastAPI, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_session
from app.observability import RequestContextMiddleware, configure_logging
from app.routers import admin, agents, approvals, auth, integrations, metrics, pipelines, query
from app.routers import catalog as catalog_router
from app.schemas import LiveResponse, ReadyResponse

SERVICE_NAME = "copilot-api"
VERSION = "0.1.0"

settings = get_settings()
configure_logging(settings.log_level)
log = logging.getLogger("copilot")

app = FastAPI(title="AI Data Engineering & Analytics Copilot API", version=VERSION)
app.add_middleware(RequestContextMiddleware)
for r in (auth.router, catalog_router.router, query.router, agents.router, approvals.router, pipelines.router, integrations.router, metrics.router, admin.router):
    app.include_router(r)


@app.get("/health", response_model=LiveResponse, tags=["health"])
def health() -> LiveResponse:
    """Liveness: the process is up. Does not touch the database."""
    return LiveResponse(status="ok", service=SERVICE_NAME, version=VERSION)


@app.get("/health/ready", response_model=ReadyResponse, tags=["health"])
def ready(response: Response, session: Session = Depends(get_session)) -> ReadyResponse:
    """Readiness: database reachable and migrations applied."""
    try:
        session.execute(text("SELECT 1"))
        revision = session.execute(text("SELECT version_num FROM app.alembic_version")).scalar()
    except Exception as exc:  # noqa: BLE001 - report any failure as "not ready"
        log.error("readiness check failed: %s", type(exc).__name__)
        response.status_code = 503
        return ReadyResponse(
            status="unavailable", database="down",
            detail="database unreachable or migrations not applied",
        )
    return ReadyResponse(status="ok", database="up", schema_revision=revision)
