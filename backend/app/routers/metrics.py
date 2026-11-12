"""Execution metrics (engineer/admin). Durable metrics come from the database; API latency is this process only."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_session
from app.observability import API_METRICS
from app.security import require_roles
from app.services import metrics

router = APIRouter(prefix="/metrics", tags=["metrics"], dependencies=[Depends(require_roles("engineer", "admin"))])


@router.get("/summary")
def summary(days: int = Query(7, ge=1, le=90), session: Session = Depends(get_session)):
    return {**metrics.summary(session, days), "api": API_METRICS.summary()}
