# Observability, operations and the UI (Phase 6)

## Streamlit app (`ui/`)
`Home.py` is the entry point: login form -> role-based navigation. Pages live in `ui/views/`:

| Page | Roles | What it does (all through the secured API) |
|---|---|---|
| Catalog & glossary | all | search, browse, column details, glossary |
| Data sources | all | sources with dataset counts, row totals, last sync (no connection settings) |
| Ask & SQL | all | plain-language question with SQL + evidence + fallbacks shown; SQL console (validate / run / CSV) |
| Gold analytics | all | five dashboards from fixed read-only queries (customer-level board refused for analysts by the server) |
| Pipelines | engineer, admin | status + freshness SLA, **Run now**, run history, **Retry / Cancel**, **Abandon stuck run (admin)**, approved overrides + **Revert (admin)** |
| Quality & drift | engineer, admin | per-run checks, rejected-row reasons, PSI drift table, alerts, deliver alerts, trend charts |
| Approval queue | engineer, admin | proposals with rationale/risk, approve/reject with note, history |
| Monitoring | engineer, admin | workflow/LLM/SQL/pipeline/integration metrics and API latency |
| Users & audit | admin | users, roles, activate/deactivate, audit events, SQL audit |

**Authorization model.** The UI container has no database driver, no credentials and no backend code (tests enforce this). Every page
action is an HTTP call carrying the *user's own token*; nothing the UI sends can assert a role or identity. Navigation hides pages the
role cannot use, but that is cosmetic: tests render privileged pages for an analyst (even with a forged session role) and the backend
refuses every call. The role shown in the sidebar is re-read from `/auth/me` on every interaction, so role changes, deactivation and token
expiry take effect on the next click. Tokens live only in the server-side Streamlit session (never in the URL, never rendered, never logged).

## Pipeline execution and recovery
The API never runs Spark. **Run now / Retry** enqueue a run (`queued`); the `etl_worker` container claims it (`FOR UPDATE SKIP LOCKED`) and
executes it with the same retry/failure handling as the CLI. A partial unique index guarantees at most one queued-or-running run per
pipeline even under concurrent clicks. Recovery controls: retry a failed run (linked via `retry_of`), cancel a queued run, abandon a run
stuck in `running` after a worker crash (admin; stale runs older than 1 hour are also reaped automatically at the next start), revert an
approved override (admin). Every control is audited and refused by the server for roles that may not use it.

## Structured logging
- One JSON object per line: `ts, level, logger, msg, request_id` plus fields (`method, path` as the route *template*, `status, duration_ms, user`).
- `X-Request-ID`: accepted from the client only if it is 8-64 safe characters, otherwise generated; echoed in the response, in every log line
  of that request and in error bodies. The Streamlit client generates one per call, so a UI action and the API access log share an id.
- Never logged: request bodies, query strings, `Authorization` headers, passwords, tokens, integration URLs (a webhook URL is a secret;
  HTTP client libraries are silenced below WARNING). Auth failures log at WARNING, server errors at ERROR with the stack trace.
- Unhandled exceptions return `{"error":"internal_error","message":...,"request_id":...}`; the client never sees internals.

## Metrics (`GET /metrics/summary?days=N`, engineer/admin; shown on the Monitoring page)
Durable metrics are computed from the database with `percentile_cont`; empty windows return nulls, nothing is estimated.
- **Agent workflow:** runs, failed, awaiting approval, error rate, latency p50/p95 (created -> finished), by intent.
- **LLM:** calls, errors, timeouts, latency p50/p95, prompt/completion tokens, average tokens per task, **average cost per task** (`LLM_PROMPT_COST_PER_1K`
  / `LLM_COMPLETION_COST_PER_1K`; 0 for local Ollama).
- **SQL safety:** attempts by status, blocked rate, executed p50/p95, average planner cost.
- **Pipelines:** finished runs, success rate, failed, retried attempts, queued/running, duration p50/p95, last success, hours since success,
  **freshness SLA** (`PIPELINE_SLA_HOURS`, default 26) with a breach flag.
- **Integrations:** calls, success rate, timeouts, latency p50/p95, last failure; **alerts** by status.
- **API latency** (in memory, this process only, rolling 5,000 requests; resets on restart): overall and per-route count, 4xx/5xx, p50/p95.
Not built: a time-series store or Prometheus endpoint; for durable API latency, aggregate the JSON access logs.
