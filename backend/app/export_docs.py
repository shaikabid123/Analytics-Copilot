"""Write API documentation artefacts from the live app:  python -m app.export_docs ../docs
  openapi.json              OpenAPI 3 document (also served at /openapi.json and /docs)
  postman_collection.json   Postman v2.1 collection: log in once, the token is stored in {{token}} and used by every request."""
import json
import sys
from pathlib import Path

from app.main import app

EXAMPLES = {
    ("POST", "/auth/login"): {"username": "alice", "password": "<your password>"},
    ("POST", "/query/validate"): {"sql": "SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel"},
    ("POST", "/query/execute"): {"sql": "SELECT channel, SUM(orders) AS n FROM gold.daily_sales GROUP BY channel"},
    ("POST", "/agents/ask"): {"question": "total sales by month in 2026"},
    ("POST", "/approvals/{approval_id}/decision"): {"decision": "approved", "note": "reviewed"},
    ("POST", "/admin/users"): {"username": "bob", "password": "<12+ chars>", "role": "engineer"},
    ("PATCH", "/admin/users/{username}"): {"is_active": True},
}
PARAMS = {"dataset_id": "1", "term_id": "1", "run_id": "1", "approval_id": "1", "username": "bob"}
PUBLIC = {("GET", "/health"), ("GET", "/health/ready"), ("POST", "/auth/login")}


def build(spec: dict) -> dict:
    folders: dict[str, list] = {}
    for path, item in spec["paths"].items():
        for method, op in item.items():
            if method not in ("get", "post", "patch", "put", "delete"):
                continue
            key = (method.upper(), path)
            concrete = "/".join(PARAMS.get(seg[1:-1], "1") if seg.startswith("{") else seg for seg in path.split("/"))
            req = {"method": method.upper(), "header": [{"key": "Content-Type", "value": "application/json"}],
                   "url": {"raw": "{{base_url}}" + concrete, "host": ["{{base_url}}"], "path": concrete.strip("/").split("/")}}
            if key not in PUBLIC:
                req["auth"] = {"type": "bearer", "bearer": [{"key": "token", "value": "{{token}}", "type": "string"}]}
            if key in EXAMPLES:
                req["body"] = {"mode": "raw", "raw": json.dumps(EXAMPLES[key], indent=2)}
            entry = {"name": f"{method.upper()} {path}", "request": req}
            if key == ("POST", "/auth/login"):
                entry["event"] = [{"listen": "test", "script": {"type": "text/javascript", "exec": [
                    "pm.collectionVariables.set('token', pm.response.json().access_token);"]}}]
            folders.setdefault((op.get("tags") or ["other"])[0], []).append(entry)
    return {"info": {"name": spec["info"]["title"], "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "variable": [{"key": "base_url", "value": "http://localhost:8000"}, {"key": "token", "value": ""}],
            "item": [{"name": tag, "item": items} for tag, items in folders.items()]}


def main(out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    spec = app.openapi()
    (out / "openapi.json").write_text(json.dumps(spec, indent=2))
    (out / "postman_collection.json").write_text(json.dumps(build(spec), indent=2))
    print(f"wrote {out/'openapi.json'} and {out/'postman_collection.json'} ({sum(len(f['item']) for f in build(spec)['item'])} requests)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "../docs")
