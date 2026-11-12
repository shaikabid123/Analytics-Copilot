"""Direct SQL for authenticated users. Everything goes through the AST guard, the cost check and the read-only role;
the caller's role (from the database, via the token) decides which tables may be read. Every attempt is audited."""
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from app import audit
from app.agents import tools as T
from app.db import get_engine
from app.llm import NullLLM
from app.security import Principal, current_user

router = APIRouter(prefix="/query", tags=["query"])


class SqlIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sql: str = Field(min_length=1, max_length=4000)


def _ctx(user: Principal) -> T.ToolContext:
    return T.ToolContext(run_id="", actor=user.username, role=user.role, llm=NullLLM())


def _audit(user: Principal, action: str, outcome: str, detail: dict) -> None:
    with Session(get_engine()) as s:
        audit.record(s, actor=user.username, action=action, resource_type="query", outcome=outcome, detail=detail)
        s.commit()


@router.post("/validate")
def validate(body: SqlIn, user: Principal = Depends(current_user)):
    r = T.sql_validate(_ctx(user), body.sql, source="user")
    return {"ok": r["ok"], "sql": r["sql"], "errors": r["errors"], "tables": r["tables"]}


@router.post("/execute")
def execute(body: SqlIn, user: Principal = Depends(current_user)):
    r = T.sql_execute(_ctx(user), body.sql, source="user")
    if not r["ok"]:
        _audit(user, "query.api", "blocked" if r["kind"] == "blocked" else "failure", {"kind": r["kind"]})
        return JSONResponse(status_code=400 if r["kind"] == "failed" else 403,
                            content={"error": r["kind"], "reasons": r["errors"]})
    _audit(user, "query.api", "success", {"rows": r["row_count"]})
    return {k: r[k] for k in ("sql", "columns", "rows", "row_count", "truncated", "duration_ms", "estimated_cost")}
