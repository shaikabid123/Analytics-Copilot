"""Operator CLI for the agent workflow. NOTE: --actor/--role are asserted by the caller until authentication
exists; the role checks themselves are enforced in code (tools, approvals).

  python -m app.agents.cli ask "total sales by month" --actor alice --role analyst
  python -m app.agents.cli ask "why did the sales pipeline fail" --actor bob --role engineer
  python -m app.agents.cli approvals
  python -m app.agents.cli decide 7 approved --actor bob --role engineer
  python -m app.agents.cli show RUN_ID
"""
import argparse
import json
import sys

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agents import service
from app.agents.tools import PermissionDenied
from app.db import get_engine
from app.models import Approval


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="app.agents.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("ask"); p.add_argument("question"); p.add_argument("--actor", required=True)
    p.add_argument("--role", required=True); p.add_argument("--pipeline")
    sub.add_parser("approvals")
    p = sub.add_parser("decide"); p.add_argument("approval_id", type=int); p.add_argument("decision", choices=["approved", "rejected"])
    p.add_argument("--actor", required=True); p.add_argument("--role", required=True); p.add_argument("--note", default="")
    p = sub.add_parser("show"); p.add_argument("run_id")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "ask":
            out = service.run_request(a.question, a.actor, a.role, pipeline=a.pipeline)
            print(f"run {out['run_id']}  status={out['status']}  intent={out['intent']}\n\n{out['answer'] or '(waiting for approval)'}")
            if out["status"] == "awaiting_approval":
                print(f"\nApproval #{out['approval_id']} is pending. Decide with: decide {out['approval_id']} approved|rejected --actor NAME --role engineer")
        elif a.cmd == "approvals":
            with Session(get_engine()) as s:
                for ap_ in s.scalars(select(Approval).where(Approval.status == "pending").order_by(Approval.id)):
                    print(f"#{ap_.id} requested_by={ap_.requested_by_label} {ap_.summary}")
        elif a.cmd == "decide":
            out = service.resume_approval(a.approval_id, a.decision, a.actor, a.role, a.note)
            print(f"run {out['run_id']}  status={out['status']}\n\n{out['answer']}")
        else:
            print(json.dumps(service.get_run(a.run_id), indent=2, default=str))
    except (PermissionDenied, service.ApprovalError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
