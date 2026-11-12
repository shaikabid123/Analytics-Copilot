# HANDOFF: end of Phase 6 (Streamlit app, pipeline execution, observability)

## Status
Verified in my Linux sandbox (real PostgreSQL 16, PySpark 4.0.1, LangGraph, Streamlit 1.66 test harness), NOT through Docker and NOT in a real browser.
From a clean extract with dependencies installed from `requirements-uitest.txt` / `etl/requirements.txt` into fresh virtualenvs (the ETL venv has no web framework, on purpose):
- 6 migrations from empty, `alembic check` clean. **Backend tests: 529 passed. UI tests (`tests_ui`): 66 passed. ETL tests: 53 passed.**
- **UI-driven end-to-end scenario on real data** (real pages -> real API -> real Spark worker): "Run now" queued run #270 and the button disabled itself; the worker ran it (success);
  a corrupted events file failed the quality gate (#271, reject rate 3.332% > 2%, Gold unchanged); the quality page showed the blocking failure and rejection reasons; the agent
  diagnosed it via the UI and created approval #133; the requester and an analyst were refused, a second engineer approved; the UI re-run (#272) honoured `max_reject_pct=5.0`
  (1,666 rows rejected, accepted); an engineer was refused when reverting the override, an admin reverted it; the Monitoring page showed success rate and SLA.
  (A first attempt was killed by a sandbox restart; the rerun above is the one that completed all nine steps.)

## What was built
- **Streamlit app** (`ui/Home.py`, `ui/views/*`): login gate + role-based navigation; catalog/glossary, data sources, Ask & SQL (evidence, fallbacks shown, SQL console), Gold analytics (5 dashboards, CSV),
  Pipelines (status, freshness SLA, Run now, history, Retry/Cancel/Abandon, override revert), Quality & drift (checks, rejection reasons, PSI, alerts, trend, deliver alerts), Approval queue, Monitoring, Users & audit.
- **UI cannot bypass the backend:** the UI has no DB driver, credentials or backend code (static tests + compose check); every action is an API call with the user's token; nothing it sends can assert a role/identity;
  privileged pages forced to render for the wrong role show the server's refusal and no data/actions; the session role is re-read from `/auth/me` each interaction.
- **Pipeline execution:** API enqueues, `etl_worker` executes (`FOR UPDATE SKIP LOCKED`), DB-level one-active-run-per-pipeline index; endpoints for run/retry/cancel/abandon(admin)/override-revert(admin), overview with SLA, quality trend.
- **Observability:** JSON logs with request ids (`app/logging_config.py`, stdlib only), request-id correlation UI<->API, sanitised 500s, no secrets/bodies/URLs in logs; in-process API latency metrics; durable metrics from the DB
  (`GET /metrics/summary`: workflow p50/p95, error rate, LLM tokens + avg cost/task, SQL, pipeline success/duration/freshness SLA, integrations, alerts). See `docs/observability.md`.

## Bugs found by this phase's tests (and fixed)
- **Fallback SQL templates for delivery time and on-time delivery were broken** (`round(double precision, integer)` does not exist); Phase 4 only validated templates, never executed them. Now every template is executed against real Gold in tests.
- The worker image would have crashed on startup (it imported FastAPI through the logging module); logging moved to a stdlib-only module and an import test guards it (verified to fail with the old import).
- Test-only: a vacuous assertion, a too-strict "no errors" helper, an audit query colliding on bare resource ids.

## NOT verified
- Docker end to end (new `etl_worker`, UI image with `views/`, `mocks`, mounts). Your Docker output from Phases 1-5 is still outstanding.
- A real browser: UI tests use Streamlit's AppTest (real pages, real HTTP, no JS rendering).
- Real Slack/Jira, a real Ollama model (SQL accuracy unmeasured).

## OPEN ISSUE (unresolved): intermittent cost-limit test failure
`tests/test_agent_graph.py::test_cost_limit_blocks_and_repair_recovers` (and once its sibling in `test_sql_exec`) has failed 3 times in fresh/first runs and never in ~3,000 repeated runs; I could not capture the message.
Evidence: those runs were ~5-12 s slower, matching the 5 s statement timeout. Hypotheses tested and rejected: threshold edge (cost is 8,383x over the limit), stale/empty planner statistics (cost stays ~358 M).
Still plausible: the EXPLAIN or query waiting on a lock/timeout right after a restart or data load. Safety is not affected (execution is bounded by the 5 s timeout and the read-only role), but the cost cap's behaviour in that case is unproven.
The tests now print the recorded audit rows/exception on failure, so the next occurrence explains itself.

## Known gaps and decisions
1. No scheduler: cron expressions are stored but nothing triggers runs; the UI offers Run now/Retry only.
2. One worker process; a crashed worker leaves a run `running` until an admin abandons it or the next start reaps it after 1 hour. The worker reuses one Spark session.
3. API latency metrics are per process and reset on restart; no Prometheus/time-series store.
4. UI: manual Refresh (no auto-refresh); catalog glossary makes one call per term; tables are limited by API page sizes; no visual design work beyond what workflows need.
5. Users cannot be deleted once they triggered runs (FK); deactivate instead (audit integrity).
6. UI tests need `pip install -r backend/requirements-uitest.txt` (streamlit, pandas); `REPO_ROOT` points them at `ui/`, compose and docker files.
7. Earlier gaps still apply: operator CLIs are unauthenticated admin tooling; no refresh tokens/logout; 401s not audited; mocks only for Slack/Jira; MySQL connector untested live.
8. Not built: 20-question NL-to-SQL evaluation, BRD/SRS and diagrams (architecture, ERD, sequence, agent graph image), GitHub Actions CI, cloud deployment notes.

## Next (suggested order)
1. Run the README steps in Codespaces and paste back `docker compose ps` + test counts (Docker is the largest unverified area).
2. 20-question NL-to-SQL evaluation with Ollama (qwen2.5-coder:3b) vs the template fallback; report accuracy, p50/p95, tokens honestly.
3. Scheduler (cron -> enqueue) in the worker, with a UI toggle.
4. Documentation set: BRD/SRS, user stories + acceptance matrix, diagrams, evaluation report, demo script; optional CI.
