"""Agent workflow over HTTP. The caller's identity and role come ONLY from the verified token; nothing in the
request body can set them."""
import uuid
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.agents import service
from app.security import Principal, current_user

router = APIRouter(prefix="/agents", tags=["agents"])
PRIVILEGED = {"engineer", "admin"}


class AskIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(min_length=1, max_length=500)
    pipeline: Literal["sales", "events"] | None = None


@router.post("/ask")
def ask(body: AskIn, user: Principal = Depends(current_user)):
    out = service.run_request(body.question, user.username, user.role, pipeline=body.pipeline)
    return {"run_id": out["run_id"], "status": out["status"], "intent": out["intent"], "answer": out["answer"],
            "approval_id": out["approval_id"]}


@router.get("/runs/{run_id}")
def get_run(run_id: uuid.UUID, user: Principal = Depends(current_user)):
    run = service.get_run(str(run_id))
    if run is None or (user.role not in PRIVILEGED and (run["state"] or {}).get("actor") != user.username):
        raise HTTPException(404, "run not found")        # same answer for 'missing' and 'not yours'
    return run
