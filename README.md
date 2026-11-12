# AI Data Engineering & Analytics Copilot

Phases 1-6: FastAPI + Streamlit + PostgreSQL, migrations, least-privilege DB roles, source connectors
(Postgres, MySQL interface, CSV, Parquet), searchable catalog + glossary, synthetic data (~412k records),
two PySpark Bronze/Silver/Gold pipelines with data-quality gates and drift checks,
a LangGraph multi-agent workflow (metadata, SQL, data quality, ETL recommendation) with an approval gate,
JWT authentication + RBAC on every API route, a persisted approval queue, two integrations (documented mocks), and a role-aware Streamlit app with pipeline controls, quality reports and monitoring.
(Auth/RBAC, ETL, agents and SQL guard arrive in later phases; see `HANDOFF.md`.)

## Run it (recommended: GitHub Codespaces; the terminal there is Linux bash, not CMD)

1. On github.com create a new empty **private** repo, then **Code > Codespaces > Create codespace** (2-core).
2. Drag `copilot_phase1.zip` into the Codespaces file Explorer, then in the terminal:
```bash
unzip copilot_phase1.zip && cd copilot
cp .env.example .env
nano .env                      # change ALL passwords (letters+digits only), Ctrl+O, Enter, Ctrl+X
docker --version               # confirm Docker is available in your codespace
docker compose up -d --build
docker compose ps              # postgres should be "healthy", api and ui "running"
curl localhost:8000/health
curl localhost:8000/health/ready
docker compose exec api pytest -q
```
**Upgrading from Phase 1?** Phase 2 adds a database role (`source_ro`) in the init script, which only runs on a new volume:
add `SOURCE_RO_PASSWORD=...` to your `.env` (see `.env.example`), then `docker compose down -v` before `up` (this wipes the DB).

Expected: `/health` -> `{"status":"ok",...}`, `/health/ready` -> `"database":"up","schema_revision":"0001"`, pytest -> `18 passed`.
Open the UI from the **Ports** tab (port 8501) and the API docs at port 8000, path `/docs`.

**Windows CMD** is only needed for local steps, e.g. zipping or committing: `cd copilot`, `git init`, `git add .`, `git commit -m "phase 1"`.
Do not run `docker compose` on your 4 GB PC.

## Phase 2: load data, build the catalog, run all tests
```bash
docker compose exec api python /seed/generate_data.py                                   # ~412k records (src.* tables + 2 files)
docker compose exec api python -m app.cli register-sources --file /seed/sources.json
docker compose exec api python -m app.cli test-source retail_pg
docker compose exec api python -m app.cli sync-source --all                             # discover schemas + exact row counts
docker compose exec api python -m app.cli apply-metadata --file /seed/catalog_metadata.json
docker compose exec api pytest -q                                                       # expected: 76 passed
docker compose exec api python -m app.cli preview retail_pg src orders --limit 3        # row preview (CLI only, by design)
```
Open the UI (port 8501) and use the **Catalog** page; API docs at `/docs` (`/catalog/search?q=customer email`).
Re-running `generate_data.py` resets the data deterministically; follow it with `sync-source --all`.

| Dataset | Where | Records |
|---|---|---|
| customers / products / orders / order_items | Postgres `src` | 20,000 / 2,000 / 80,000 / 191,043 |
| web_events | CSV `data/files/web_events.csv` | 50,000 |
| shipments | Parquet `data/files/shipments.parquet` | 68,811 |

Source configs hold **no secrets**: a SQL source names an env var (`password_env`, must start with `SRC_`);
files must live under `DATA_DIR`; hosts must be in `ALLOWED_SOURCE_HOSTS`. See `seed/sources.json`.
MySQL: copy `seed/mysql_source.example.json`; the connector exists but has **not been tested against a live MySQL server**.

## Phase 3: PySpark ETL, data quality, drift
Needs the Phase 2 seed data. The api container applies migration 0003 on start; the `etl` service is built on first use
(Java + PySpark, a few minutes) and is not started by `docker compose up`.
```bash
docker compose up -d --build
docker compose run --rm etl python -m etl.runner run sales      # Postgres src.* -> bronze/silver/gold (daily_sales, customer_value)
docker compose run --rm etl python -m etl.runner run events     # CSV + Parquet   -> bronze/silver/gold (funnel_daily, delivery_sla)
docker compose run --rm etl python -m etl.runner status         # recent runs
docker compose run --rm etl python -m etl.runner diagnose 1     # replace 1 with a run id: error, failed checks, reject reasons, alerts
docker compose run --rm etl python -m pytest -q etl/tests       # ETL tests (about 15 min on a small machine; real-output tests need both runs above)
docker compose exec api pytest -q                               # backend tests (76)
```
Failure and recovery demos:
```bash
docker compose run --rm etl python -m etl.runner run sales --simulate-transient 2 --backoff 2   # 2 simulated outages, retried, then succeeds
docker compose run --rm etl python -m etl.runner retry 5                                          # re-run a failed run id (new run, linked to the old one)
mv data/files/shipments.parquet data/files/shipments.bak && docker compose run --rm etl python -m etl.runner run events   # fails fast, no retry
mv data/files/shipments.bak data/files/shipments.parquet
```
How a run behaves
- **Bronze** raw copy plus `_run_id`, `_ingested_at`, `_source`. The input schema contract is checked BEFORE anything is written.
- **Silver** trim/normalise, deduplicate (keep most complete row), validate; bad rows go to `silver.sales_rejects` / `silver.events_rejects` with a reason.
- **Gates** (`error` severity blocks): reject rate per dataset (default 2%), row-count vs baseline (below 50%), Gold-vs-Silver reconciliation, key/referential invariants. Drift (PSI 0.10 warning, 0.25 failed) and duplicates only raise alerts.
- **Gold** is built in staging tables and published in ONE transaction, only after every gate passes. A failed run never changes Gold.
- **Retry**: only transient infrastructure errors are retried (bounded, exponential backoff). Contract and quality failures are final.
- **Baselines** are created by the first successful run (never by a failed one); `--reset-baseline` accepts a new normal.
- Alerts are stored in `app.dq_alerts` (status `pending`); delivery arrives with the integrations phase.

## Phase 4: LangGraph agents (details: `docs/agents.md`)
Needs the Phase 3 pipelines to have run (Gold tables). Order matters the first time:
```bash
docker compose up -d --build
docker compose exec api python -m app.cli sync-source --all                                # picks up the Gold tables
docker compose exec api python -m app.cli apply-metadata --file /seed/catalog_metadata.json
docker compose exec api pytest -q                                                          # backend + agent tests
# Ask questions (actor/role are asserted by you until the auth phase; the role checks are enforced in code)
docker compose exec api python -m app.agents.cli ask "total sales by month in 2026" --actor alice --role analyst
docker compose exec api python -m app.agents.cli ask "top 5 customers by lifetime value" --actor alice --role analyst   # refused: analysts cannot read customer_value
docker compose exec api python -m app.agents.cli ask "top 5 customers by lifetime value" --actor bob --role engineer
docker compose exec api python -m app.agents.cli ask "what is gross margin" --actor alice --role analyst
docker compose exec api python -m app.agents.cli ask "show data quality for sales" --actor bob --role engineer
docker compose exec api python -m app.agents.cli ask "why did the events pipeline fail" --actor bob --role engineer
docker compose exec api python -m app.agents.cli approvals                                 # pending proposals
docker compose exec api python -m app.agents.cli decide 1 approved --actor bob --role engineer
docker compose exec api python -m app.agents.cli show RUN_ID                               # persisted run state
```
Without an LLM (the default, `LLM_PROVIDER=none`) SQL questions use deterministic templates for common questions and say so.
Optional local LLM (needs several GB of RAM, so Codespaces, not your 4 GB PC):
```bash
docker compose --profile llm up -d ollama
docker compose exec ollama ollama pull qwen2.5-coder:3b
# set LLM_PROVIDER=ollama in .env, then
docker compose up -d
```
Approval demo on real data (makes the events pipeline fail its quality gate, then approves a threshold change):
```bash
cp data/files/web_events.csv /tmp/web_events.orig
awk -F, 'BEGIN{OFS=","} NR>1 && NR%30==0 {$4="teleport"} {print}' /tmp/web_events.orig > data/files/web_events.csv   # ~3.3% invalid rows
docker compose run --rm etl python -m etl.runner run events                       # fails the reject-rate gate; Gold unchanged
docker compose exec api python -m app.agents.cli ask "why did the events pipeline fail" --actor bob --role engineer   # proposes max_reject_pct 5, creates approval
docker compose exec api python -m app.agents.cli decide 1 approved --actor bob --role engineer                       # id from the previous output
docker compose run --rm etl python -m etl.runner run events                       # now succeeds; run details show the override
cp /tmp/web_events.orig data/files/web_events.csv                                 # restore
```

## Phase 5: authentication, RBAC, approval queue, integrations (details: `docs/security.md`)
Set `JWT_SECRET` (32+ characters) and `MOCK_TICKET_TOKEN` in `.env` (see `.env.example`), then:
```bash
docker compose up -d --build                     # starts postgres, api, ui and the mocks service; api applies migrations 0001-0005
export ADMIN_PW='choose-a-strong-password-1'     # never put passwords on the command line
docker compose exec -e ADMIN_PW api python -m app.cli create-user --username admin --role admin --password-env ADMIN_PW
TOKEN=$(curl -s localhost:8000/auth/login -H 'Content-Type: application/json' -d "{\"username\":\"admin\",\"password\":\"$ADMIN_PW\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
curl -s localhost:8000/auth/me -H "Authorization: Bearer $TOKEN"
# create an analyst and an engineer (admin only), then log in as them the same way
curl -s localhost:8000/admin/users -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"username":"ann","password":"another-strong-pass-2","role":"analyst"}'
curl -s localhost:8000/query/execute -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"sql":"DROP TABLE gold.daily_sales"}'   # 403 blocked + audited
curl -s localhost:8000/approvals -H "Authorization: Bearer $TOKEN"                        # the persisted approval queue
curl -s -X POST localhost:8000/integrations/dispatch -H "Authorization: Bearer $TOKEN"    # deliver pending alerts (Slack message + ticket for HIGH)
curl -s localhost:9000/control/state                                                      # what the mock Slack/ticket services received
curl -s localhost:9000/control -H 'Content-Type: application/json' -d '{"service":"slack","mode":"fail"}'   # simulate an outage; dispatch then keeps alerts queued
docker compose exec api pytest -q                                                         # 475 tests
```
Interactive API docs: http://localhost:8000/docs (use the Authorize button with the token). `docs/openapi.json` and
`docs/postman_collection.json` are generated from the live app (log in once; the token is stored automatically).
Approval separation of duties: the person who triggered a proposal cannot approve it, so use two engineer/admin accounts.

## Phase 6: Streamlit app, pipeline execution, observability (details: `docs/observability.md`)
```bash
docker compose up -d --build                                   # postgres, api, ui, mocks
docker compose --profile etl up -d --build etl_worker          # the worker that executes runs queued from the UI/API (builds the Spark image)
# open the UI (port 8501), log in with a user created earlier (see Phase 5), and use the role-based pages
docker compose exec api pip install -q -r requirements-uitest.txt           # extra packages for the UI tests (streamlit, pandas)
docker compose exec api pytest -q                                           # backend tests
docker compose exec api pytest tests_ui -q                                  # UI workflow + "UI cannot bypass the backend" tests
docker compose run --rm etl python -m pytest -q etl/tests                   # ETL + worker tests
docker compose logs api | tail                                              # structured JSON access logs with request ids
```
Typical operator flow in the UI: **Pipelines** -> *Run now* (queued; the worker picks it up) -> refresh -> **Quality & drift** for the run ->
if it failed its gate, **Ask & SQL** -> "why did the events pipeline fail" -> a second engineer decides it in **Approval queue** ->
*Run now* again. **Monitoring** shows workflow latency (p50/p95), error rates, tokens/cost per task, pipeline freshness SLA and integration health.

## Reset everything (wipes the database, re-runs the role/schema init script)
```bash
docker compose down -v
docker compose up -d --build
```
The init script `docker/initdb/01_roles.sh` runs **only when the `pgdata` volume is first created**, so changing
passwords in `.env` after the first start requires the reset above.

## Database design
One Postgres, schemas `app` (backend), `src`, `bronze`, `silver`, `gold` (ETL), three roles:

| Role | Can do | Cannot do |
|---|---|---|
| `app_rw` | owns `app`; runs migrations; backend runtime | touch data layers |
| `etl_rw` | owns src/bronze/silver/gold | touch `app` |
| `source_ro` | SELECT on `src` (connector credential for the demo source) | write, read anything else |
| `analytics_ro` | SELECT on `gold` only; read-only transactions, 10 s timeout | write, create, read anything else |

`app.audit_events` is insert-only: UPDATE/DELETE/TRUNCATE raise an error via database triggers.

## Migrations
```bash
docker compose exec api alembic current
docker compose exec api alembic check      # models and migration must agree
```
