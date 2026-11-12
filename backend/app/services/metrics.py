"""Durable execution metrics computed from the database (the API's own latency metrics live in observability.py).
All percentiles use percentile_cont; nothing here is estimated or invented: empty windows return nulls/zeros."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import get_settings


def _f(v, nd=1):
    return None if v is None else round(float(v), nd)


def summary(session: Session, days: int = 7) -> dict:
    cfg = get_settings()
    p = {"d": days}
    q = lambda sql, **kw: session.execute(text(sql), {**p, **kw}).mappings().all()   # noqa: E731

    runs = q("""SELECT count(*) AS total, count(*) FILTER (WHERE status='failed') AS failed,
                       count(*) FILTER (WHERE status='awaiting_approval') AS awaiting,
                       count(*) FILTER (WHERE status='succeeded') AS succeeded,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at-created_at)*1000) FILTER (WHERE finished_at IS NOT NULL) AS p50,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at-created_at)*1000) FILTER (WHERE finished_at IS NOT NULL) AS p95
                FROM app.agent_runs WHERE created_at >= now() - make_interval(days => :d)""")[0]
    by_intent = q("""SELECT coalesce(request_type,'unknown') AS intent, count(*) AS runs,
                            count(*) FILTER (WHERE status='failed') AS failed,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at-created_at)*1000) FILTER (WHERE finished_at IS NOT NULL) AS p50,
                            percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at-created_at)*1000) FILTER (WHERE finished_at IS NOT NULL) AS p95
                     FROM app.agent_runs WHERE created_at >= now() - make_interval(days => :d) GROUP BY 1 ORDER BY 2 DESC""")
    llm = q("""SELECT count(*) AS calls, count(*) FILTER (WHERE status <> 'ok') AS errors,
                      count(*) FILTER (WHERE status = 'timeout') AS timeouts,
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE status='ok') AS p50,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE status='ok') AS p95,
                      coalesce(sum(prompt_tokens),0) AS pt, coalesce(sum(completion_tokens),0) AS ct,
                      count(DISTINCT agent_run_id) AS tasks
               FROM app.llm_calls WHERE created_at >= now() - make_interval(days => :d)""")[0]
    queries = q("""SELECT status, count(*) AS n,
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE status='executed') AS p50,
                          percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE status='executed') AS p95,
                          avg(estimated_cost) FILTER (WHERE status='executed') AS avg_cost
                   FROM app.query_audits WHERE created_at >= now() - make_interval(days => :d) GROUP BY status""")
    pipelines = q("""SELECT p.name,
                            count(r.id) FILTER (WHERE r.status IN ('succeeded','failed')) AS finished,
                            count(r.id) FILTER (WHERE r.status = 'succeeded') AS succeeded,
                            count(r.id) FILTER (WHERE r.status = 'failed') AS failed,
                            count(r.id) FILTER (WHERE r.status = 'retrying') AS retried_attempts,
                            count(r.id) FILTER (WHERE r.status = 'queued') AS queued,
                            count(r.id) FILTER (WHERE r.status = 'running') AS running,
                            percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM r.finished_at-r.started_at)) FILTER (WHERE r.status='succeeded') AS p50_s,
                            percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM r.finished_at-r.started_at)) FILTER (WHERE r.status='succeeded') AS p95_s,
                            max(r.finished_at) FILTER (WHERE r.status='succeeded') AS last_success
                     FROM app.pipelines p LEFT JOIN app.pipeline_runs r
                       ON r.pipeline_id = p.id AND r.queued_at >= now() - make_interval(days => :d)
                     WHERE p.name IN ('sales','events') GROUP BY p.name ORDER BY p.name""")
    last_success = {r["name"]: r["last_success"] for r in q(
        """SELECT p.name, max(r.finished_at) AS last_success FROM app.pipelines p
           LEFT JOIN app.pipeline_runs r ON r.pipeline_id=p.id AND r.status='succeeded' WHERE p.name IN ('sales','events') GROUP BY p.name""")}
    integ = q("""SELECT integration, count(*) AS calls, count(*) FILTER (WHERE status='ok') AS ok,
                        count(*) FILTER (WHERE status='timeout') AS timeouts,
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                        max(created_at) FILTER (WHERE status <> 'ok') AS last_failure
                 FROM app.integration_calls WHERE created_at >= now() - make_interval(days => :d) GROUP BY integration""")
    alerts = q("SELECT status, count(*) AS n FROM app.dq_alerts GROUP BY status")

    now = datetime.now(timezone.utc)
    tokens = int(llm["pt"]) + int(llm["ct"])
    cost = (int(llm["pt"]) * cfg.llm_prompt_cost_per_1k + int(llm["ct"]) * cfg.llm_completion_cost_per_1k) / 1000
    tasks = int(llm["tasks"])
    pipe_out = []
    for r in pipelines:
        ls = last_success.get(r["name"])
        hours = (now - ls).total_seconds() / 3600 if ls else None
        fin = int(r["finished"])
        pipe_out.append({"pipeline": r["name"], "finished_runs": fin, "success_rate": round(int(r["succeeded"]) / fin, 4) if fin else None,
                         "failed": int(r["failed"]), "retried_attempts": int(r["retried_attempts"]), "queued": int(r["queued"]),
                         "running": int(r["running"]), "duration_p50_s": _f(r["p50_s"]), "duration_p95_s": _f(r["p95_s"]),
                         "last_success_at": ls.isoformat() if ls else None, "hours_since_success": _f(hours, 2),
                         "sla_hours": cfg.pipeline_sla_hours, "sla_breached": hours is None or hours > cfg.pipeline_sla_hours})
    total_q = sum(int(r["n"]) for r in queries)
    return {
        "generated_at": now.isoformat(), "window_days": days,
        "agent_workflow": {"runs": int(runs["total"]), "succeeded": int(runs["succeeded"]), "failed": int(runs["failed"]),
                           "awaiting_approval": int(runs["awaiting"]),
                           "error_rate": round(int(runs["failed"]) / int(runs["total"]), 4) if int(runs["total"]) else 0.0,
                           "latency_p50_ms": _f(runs["p50"]), "latency_p95_ms": _f(runs["p95"]),
                           "by_intent": [{"intent": r["intent"], "runs": int(r["runs"]), "failed": int(r["failed"]),
                                          "p50_ms": _f(r["p50"]), "p95_ms": _f(r["p95"])} for r in by_intent]},
        "llm": {"calls": int(llm["calls"]), "errors": int(llm["errors"]), "timeouts": int(llm["timeouts"]),
                "error_rate": round(int(llm["errors"]) / int(llm["calls"]), 4) if int(llm["calls"]) else 0.0,
                "latency_p50_ms": _f(llm["p50"]), "latency_p95_ms": _f(llm["p95"]), "prompt_tokens": int(llm["pt"]),
                "completion_tokens": int(llm["ct"]), "tasks_with_llm": tasks,
                "avg_tokens_per_task": round(tokens / tasks, 1) if tasks else None,
                "avg_cost_usd_per_task": round(cost / tasks, 6) if tasks else None,
                "cost_note": "rates are 0 unless LLM_*_COST_PER_1K are set (local Ollama has no per-token cost)"},
        "sql": {"attempts": total_q, "by_status": {r["status"]: int(r["n"]) for r in queries},
                "blocked_rate": round(sum(int(r["n"]) for r in queries if r["status"] == "blocked") / total_q, 4) if total_q else 0.0,
                "executed_p50_ms": next((_f(r["p50"]) for r in queries if r["status"] == "executed"), None),
                "executed_p95_ms": next((_f(r["p95"]) for r in queries if r["status"] == "executed"), None),
                "avg_estimated_cost": next((_f(r["avg_cost"]) for r in queries if r["status"] == "executed"), None)},
        "pipelines": pipe_out,
        "integrations": [{"integration": r["integration"], "calls": int(r["calls"]), "ok": int(r["ok"]), "timeouts": int(r["timeouts"]),
                          "success_rate": round(int(r["ok"]) / int(r["calls"]), 4), "latency_p50_ms": _f(r["p50"]),
                          "latency_p95_ms": _f(r["p95"]), "last_failure_at": r["last_failure"].isoformat() if r["last_failure"] else None}
                         for r in integ],
        "alerts": {r["status"]: int(r["n"]) for r in alerts},
    }
