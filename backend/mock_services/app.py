"""MOCK external services for local development and tests (clearly NOT real Slack or Jira):
    POST /slack/webhook          Slack-style incoming webhook
    POST /tickets/api/issues     Jira-style issue creation (bearer token, Idempotency-Key)
    POST /control                make a service misbehave: mode = ok | fail | slow | flaky
    GET  /control/state          inspect what was received
Failure modes let the tests and demos show success, timeout and failure handling against real HTTP."""
import asyncio
import os

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel


class Control(BaseModel):
    service: str                 # slack | tickets
    mode: str = "ok"             # ok | fail | slow | flaky
    fail_first: int = 0          # flaky: fail this many requests, then recover
    sleep_s: float = 5.0         # slow: delay before answering


def create_app() -> FastAPI:
    app = FastAPI(title="Mock integrations (not real services)")
    token = os.environ.get("MOCK_TICKET_TOKEN", "mock-token")
    fresh = lambda: {"mode": "ok", "fail_first": 0, "sleep_s": 5.0, "requests": 0}   # noqa: E731
    state = {"slack": {**fresh(), "received": []}, "tickets": {**fresh(), "issues": {}, "keys": 0}}

    async def misbehave(svc: str):
        st = state[svc]
        st["requests"] += 1
        if st["mode"] == "slow":
            await asyncio.sleep(st["sleep_s"])
        if st["mode"] == "fail" or (st["mode"] == "flaky" and st["requests"] <= st["fail_first"]):
            raise HTTPException(500, "mock failure")

    @app.post("/slack/webhook")
    async def slack(request: Request):
        await misbehave("slack")
        payload = await request.json()
        state["slack"]["received"].append(payload)
        return "ok"

    @app.post("/tickets/api/issues", status_code=201)
    async def create_issue(request: Request, authorization: str | None = Header(default=None),
                           idempotency_key: str | None = Header(default=None)):
        if authorization != f"Bearer {token}":
            raise HTTPException(401, "bad token")
        await misbehave("tickets")
        st = state["tickets"]
        if idempotency_key and idempotency_key in st["issues"]:
            return st["issues"][idempotency_key]                     # same key -> same ticket, no duplicate
        st["keys"] += 1
        issue = {"key": f"OPS-{st['keys']}", "payload": await request.json()}
        if idempotency_key:
            st["issues"][idempotency_key] = issue
        else:
            st["issues"][f"anon-{st['keys']}"] = issue
        return issue

    @app.post("/control")
    def control(c: Control):
        if c.service not in ("slack", "tickets") or c.mode not in ("ok", "fail", "slow", "flaky"):
            raise HTTPException(422, "unknown service or mode")
        state[c.service].update(mode=c.mode, fail_first=c.fail_first, sleep_s=c.sleep_s, requests=0)
        return {"ok": True}

    @app.get("/control/state")
    def get_state():
        return {"slack": {"mode": state["slack"]["mode"], "received": state["slack"]["received"],
                          "requests": state["slack"]["requests"]},
                "tickets": {"mode": state["tickets"]["mode"], "issues": list(state["tickets"]["issues"].values()),
                            "requests": state["tickets"]["requests"]}}

    return app


app = create_app()
