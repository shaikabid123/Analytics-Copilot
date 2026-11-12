"""Operator CLI (runs inside the api container). State-changing catalog operations live here, not
behind HTTP, until authentication/RBAC lands.

  python -m app.cli register-sources --file /seed/sources.json
  python -m app.cli test-source retail_pg
  python -m app.cli sync-source retail_pg        (or --all)
  python -m app.cli apply-metadata --file /seed/catalog_metadata.json
  python -m app.cli preview retail_pg src orders --limit 5
  python -m app.cli create-user --username alice --role admin --password-env NEW_USER_PASSWORD
"""
import argparse
import json
import sys

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_engine
from app.models import DataSource
from app.services import catalog
from app.services.connectors import ConnectorError, build_connector
from app.services.source_config import SourceConfigError

ACTOR = "cli"


def _source(session: Session, name: str) -> DataSource:
    src = session.scalar(select(DataSource).where(DataSource.name == name))
    if src is None:
        raise SystemExit(f"error: unknown source '{name}'")
    return src


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="app.cli")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("register-sources"); p.add_argument("--file", required=True)
    p = sub.add_parser("test-source"); p.add_argument("name")
    p = sub.add_parser("sync-source"); p.add_argument("name", nargs="?"); p.add_argument("--all", action="store_true")
    p = sub.add_parser("apply-metadata"); p.add_argument("--file", required=True)
    p = sub.add_parser("preview"); p.add_argument("name"); p.add_argument("schema"); p.add_argument("table")
    p.add_argument("--limit", type=int, default=5)
    p = sub.add_parser("create-user"); p.add_argument("--username", required=True)
    p.add_argument("--role", required=True, choices=["analyst", "engineer", "admin"])
    p.add_argument("--password-env", required=True, help="NAME of an environment variable holding the password (never pass it as an argument)")
    args = ap.parse_args(argv)

    get_engine()
    from app import db
    try:
        with Session(db._engine) as session:
            if args.cmd == "register-sources":
                for s in json.load(open(args.file, encoding="utf-8")):
                    catalog.register_source(session, name=s["name"], source_type=s["source_type"],
                                            config=s["config"], description=s.get("description"), actor=ACTOR)
                    print(f"registered {s['name']} ({s['source_type']})")
                session.commit()
            elif args.cmd == "test-source":
                src = _source(session, args.name)
                build_connector(src.source_type, src.config).test_connection()
                print(f"OK: {args.name} reachable")
            elif args.cmd == "sync-source":
                names = [s.name for s in session.scalars(select(DataSource).order_by(DataSource.id))] \
                    if args.all else [args.name]
                if not args.all and not args.name:
                    raise SystemExit("error: give a source name or --all")
                for n in names:
                    print(n, json.dumps(catalog.sync_source(session, _source(session, n), actor=ACTOR)))
            elif args.cmd == "apply-metadata":
                print(json.dumps(catalog.apply_metadata(session, args.file, actor=ACTOR)))
            elif args.cmd == "create-user":
                import os
                from app.services.users import UserError, create_user
                pw = os.environ.get(args.password_env)
                if not pw:
                    raise SystemExit(f"error: environment variable {args.password_env} is not set")
                try:
                    u = create_user(session, args.username, pw, args.role, actor=ACTOR)
                except UserError as exc:
                    raise SystemExit(f"error: {exc}")
                print(f"created user {u.username} ({args.role})")
            elif args.cmd == "preview":
                src = _source(session, args.name)
                rows = build_connector(src.source_type, src.config).sample(args.schema, args.table, args.limit)
                print(json.dumps(rows, indent=2, default=str))
    except (SourceConfigError, ConnectorError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
