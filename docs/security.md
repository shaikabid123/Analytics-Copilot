# Security design (Phase 5)

## Authentication
- `POST /auth/login` (username + password) returns a short-lived HS256 JWT (30 min). Passwords use scrypt with per-user salts and
  constant-time comparison; unknown users burn the same CPU as real ones (no timing oracle) and get the same 401 as a wrong password.
- After 5 consecutive failures an account is locked for 15 minutes (also for the correct password); every attempt is audited without secrets.
- The token algorithm is pinned (`alg: none` and algorithm-confusion tokens are rejected); `exp`, `iat`, `sub`, `iss` are required.
- **Every request re-reads the user from the database.** Deactivation and role changes apply immediately, and the role used for
  authorization is the database role: the `role` claim inside a token is ignored.
- `JWT_SECRET` (>= 32 chars) comes from the environment; without it login returns 503 (there is no fallback secret).

## Authorization (enforced in FastAPI dependencies, not in the UI)
| Area | analyst | engineer | admin |
|---|---|---|---|
| health, login | public | public | public |
| catalog, glossary, `/auth/me`, `/query/*`, `/agents/ask`, own `/agents/runs/*` | yes | yes | yes |
| all agent runs | own only | all | all |
| `/approvals` queue + decisions, `/pipelines/*`, `/integrations/*` | no (403) | yes | yes |
| `/admin/users`, `/audit/*` | no | no | yes |
`backend/tests/test_rbac.py` reads the live OpenAPI document: every operation must be classified, every one is called with no token,
a garbage token, and each role. Denials are audited (`authz.denied`). Unknown request fields are rejected, so a client cannot
supply a role or identity.

## Human approvals
Agent proposals that would change a pipeline create a row in `app.approvals` (the persisted queue) and pause the LangGraph run.
Only engineers/admins can decide; **a requester cannot decide their own request** (`ALLOW_SELF_APPROVAL=true` turns this off for
single-operator setups); a decision is atomic and single-use (concurrent decisions: exactly one wins); the stored proposal is hashed
and any edit after the request is refused; only allow-listed actions with bounded values can run. Each step is audited with actor and time.

## SQL security (defence in depth; nothing relies on regex)
1. Request validation (length, types, unknown fields).
2. sqlglot AST guard: single statement, plain SELECT, no DML/DDL/COPY/SET/INTO/locks/set operations/table functions, comments rejected,
   table allowlist per role (`gold` only), **column allowlist per table** (aliases, CTEs and subqueries resolved on the AST),
   function allowlist, no wildcards, internal `_` columns blocked, join cap, forced `LIMIT <= 1000`. The guard never raises: any
   internal error rejects the query (fail closed); a 600-case fuzz test checks it.
3. `EXPLAIN` cost cap (100,000) before execution, 5 s statement timeout, read-only transaction, row cap.
4. Database credentials: the executor connects as `analytics_ro` (SELECT on `gold` only, read-only default, role-level timeout).
   Tests prove writes fail at the database layer even if every application check were bypassed.
5. Every attempt (executed, blocked or failed) is written to `app.query_audits` with actor, role, SQL and reasons.

## Integrations (documented MOCKS; real HTTP)
- **Alert webhook** (Slack-style `POST {"text": ...}`) and **ticket API** (Jira-style `POST /issues`, bearer token, `Idempotency-Key`).
  The bundled `mocks` service stands in for them and can be switched to `fail`, `slow` or `flaky` through `POST /control`.
  Any Slack-compatible webhook URL or REST ticket API should work with the same adapters, but only the mocks were exercised here.
- Timeouts (3 s), bounded retries with exponential back-off (timeouts, connection errors and 5xx only; 4xx are never retried),
  every call recorded in `app.integration_calls` (host only, no secrets).
- Alerts stay queued with back-off and a bounded number of attempts; dispatch stops early when the integration is clearly down;
  an alert is 'sent' only when every channel it needs has succeeded; tickets are idempotent so a retry cannot duplicate them;
  workflow notifications are best-effort and never block or fail the workflow.

## Known limits
- No rate limiting beyond per-account lockout; no refresh tokens or logout/blocklist (tokens expire after 30 minutes; deactivate the user to revoke at once).
- The operator CLIs (`app.cli`, `app.agents.cli`, `etl.runner`) run with database credentials and are not authenticated: treat them as admin tooling, not a network surface.
- Streamlit has no login page yet; the API is the enforcement point and is fully protected.
