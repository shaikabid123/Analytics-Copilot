"""Runner behaviour with stub pipelines: retries, backoff, non-retryable failures, audit. No Spark needed."""
import uuid

import pytest
from sqlalchemy import text

from etl.common import ContractError, DQGateError, PipelineConfig, TransientError
from etl.runner import PipelineResult, execute
from etl.tests.conftest import drop_test_artifacts


@pytest.fixture()
def cfg(etl_engine, app_engine):
    name = f"unit_stub_{uuid.uuid4().hex[:8]}"
    yield PipelineConfig(pipeline_name=name)
    drop_test_artifacts(etl_engine, app_engine, "unit\\_stub\\_%")


def _runs(app_engine, name):
    with app_engine.connect() as c:
        return c.execute(text("SELECT r.id, r.status, r.attempt, r.trigger, r.retry_of_run_id, r.error_message "
                              "FROM app.pipeline_runs r JOIN app.pipelines p ON p.id=r.pipeline_id "
                              "WHERE p.name=:n ORDER BY r.id"), {"n": name}).all()


def _ok(spark, cfg, ctx):
    return PipelineResult(10, 5, {"stub": True})


def test_success_is_recorded(cfg, app_engine):
    out = execute(cfg, _ok, sleep=lambda s: None)
    assert out.status == "succeeded" and out.attempts == 1
    [(_, status, attempt, trigger, retry_of, err)] = _runs(app_engine, cfg.pipeline_name)
    assert (status, attempt, trigger, retry_of, err) == ("succeeded", 1, "manual", None, None)


def test_transient_failures_are_retried_with_exponential_backoff_then_succeed(cfg, app_engine):
    calls, sleeps = [], []

    def flaky(spark, cfg_, ctx):
        calls.append(ctx.run_id)
        if len(calls) < 3:
            raise TransientError("database connection lost")
        return PipelineResult(1, 1, {})

    out = execute(cfg, flaky, max_retries=3, backoff_s=2, sleep=sleeps.append)
    assert out.status == "succeeded" and out.attempts == 3 and sleeps == [2, 4]
    runs = _runs(app_engine, cfg.pipeline_name)
    assert [r.status for r in runs] == ["retrying", "retrying", "succeeded"]
    assert [r.attempt for r in runs] == [1, 2, 3]
    assert runs[1].trigger == runs[2].trigger == "retry"
    assert runs[1].retry_of_run_id == runs[0].id and runs[2].retry_of_run_id == runs[1].id   # chain is auditable


def test_retries_are_bounded(cfg, app_engine):
    def always(spark, cfg_, ctx):
        raise TransientError("still down")
    out = execute(cfg, always, max_retries=2, backoff_s=0, sleep=lambda s: None)
    assert out.status == "failed" and out.attempts == 3                    # 1 try + 2 retries, then stop
    assert [r.status for r in _runs(app_engine, cfg.pipeline_name)] == ["retrying", "retrying", "failed"]


@pytest.mark.parametrize("exc", [ContractError("missing column channel"), DQGateError("reject_rate too high"),
                                 ValueError("bug in transform")])
def test_non_retryable_failures_stop_immediately(cfg, app_engine, exc):
    def bad(spark, cfg_, ctx):
        raise exc
    out = execute(cfg, bad, max_retries=3, backoff_s=0, sleep=lambda s: pytest.fail("must not back off"))
    assert out.status == "failed" and out.attempts == 1
    [run] = _runs(app_engine, cfg.pipeline_name)
    assert run.status == "failed" and type(exc).__name__ in run.error_message


def test_raw_connection_errors_are_classified_as_transient(cfg, app_engine):
    import psycopg
    n = []

    def conn_err(spark, cfg_, ctx):
        n.append(1)
        if len(n) == 1:
            raise psycopg.OperationalError("connection to server at db failed: Connection refused")
        return PipelineResult(0, 0, {})
    assert execute(cfg, conn_err, max_retries=2, backoff_s=0, sleep=lambda s: None).status == "succeeded"
    assert len(n) == 2


def test_simulated_outage_flag_exercises_retry_path(cfg, app_engine):
    out = execute(cfg, _ok, max_retries=3, backoff_s=0, simulate_transient=2, sleep=lambda s: None)
    assert out.status == "succeeded" and out.attempts == 3
    runs = _runs(app_engine, cfg.pipeline_name)
    assert "[simulated]" in runs[0].error_message


def test_second_concurrent_run_is_refused_and_stale_runs_are_reaped(cfg, app_engine):
    from etl import runlog
    from etl.common import PipelineError
    p = runlog.ensure_pipeline(cfg.pipeline_name)
    rid = runlog.start_run(p.id, cfg.pipeline_name, "manual", 1, None)      # left 'running'
    with pytest.raises(PipelineError, match="already running"):
        execute(cfg, _ok, sleep=lambda s: None)
    with app_engine.begin() as c:                                           # simulate a crash two hours ago
        c.execute(text("UPDATE app.pipeline_runs SET started_at = now() - interval '2 hours' WHERE id=:i"), {"i": rid})
    assert execute(cfg, _ok, sleep=lambda s: None).status == "succeeded"
    runs = _runs(app_engine, cfg.pipeline_name)
    assert runs[0].status == "failed" and "abandoned" in runs[0].error_message


def test_audit_trail_records_run_lifecycle(cfg, app_engine):
    def bad(spark, cfg_, ctx):
        raise ContractError("x")
    out = execute(cfg, bad, backoff_s=0, sleep=lambda s: None)
    with app_engine.connect() as c:
        rows = c.execute(text("SELECT action, outcome FROM app.audit_events WHERE actor_label='etl' "
                              "AND resource_id=:r ORDER BY id"), {"r": str(out.run_ids[0])}).all()
    assert [r[0] for r in rows] == ["pipeline.run.start", "pipeline.run.failed"] and rows[1][1] == "failure"


def _set_params(app_engine, name, params):
    import json
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.pipelines SET params = CAST(:p AS jsonb) WHERE name = :n"), {"p": json.dumps(params), "n": name})


def _params(app_engine, name):
    with app_engine.connect() as c:
        return c.execute(text("SELECT params FROM app.pipelines WHERE name = :n"), {"n": name}).scalar()


def test_approved_overrides_are_applied_and_reset_flag_is_consumed_only_on_success(cfg, app_engine):
    from etl import runlog
    runlog.ensure_pipeline(cfg.pipeline_name)
    _set_params(app_engine, cfg.pipeline_name, {"max_reject_pct": 6.0, "reset_baseline_next_run": True})
    seen = {}

    def spy(spark, cfg_, ctx):
        seen.update(max_reject_pct=cfg_.max_reject_pct, reset=ctx.reset_baseline)
        raise ContractError("fails: the flag must survive")

    assert execute(cfg, spy, sleep=lambda s: None).status == "failed"
    assert seen == {"max_reject_pct": 6.0, "reset": True}
    assert _params(app_engine, cfg.pipeline_name) == {"max_reject_pct": 6.0, "reset_baseline_next_run": True}

    def ok(spark, cfg_, ctx):
        seen.update(reset=ctx.reset_baseline)
        return PipelineResult(1, 1, {})
    assert execute(cfg, ok, sleep=lambda s: None).status == "succeeded"
    assert seen["reset"] is True
    assert _params(app_engine, cfg.pipeline_name) == {"max_reject_pct": 6.0}          # one-shot flag consumed, threshold kept
    with app_engine.connect() as c:
        d = c.execute(text("SELECT details FROM app.pipeline_runs r JOIN app.pipelines p ON p.id=r.pipeline_id "
                           "WHERE p.name=:n AND r.status='succeeded'"), {"n": cfg.pipeline_name}).scalar()
    assert d["overrides"] == {"max_reject_pct": 6.0, "reset_baseline_next_run": True}   # transparent in run history


@pytest.mark.parametrize("bad", [{"max_reject_pct": 50}, {"max_reject_pct": -1}, {"max_reject_pct": "5"},
                                 {"max_reject_pct": True}, {"reset_baseline_next_run": "yes"}, {"unknown": 1}])
def test_out_of_range_or_unknown_overrides_are_ignored(cfg, app_engine, bad):
    from etl import runlog
    runlog.ensure_pipeline(cfg.pipeline_name)
    _set_params(app_engine, cfg.pipeline_name, bad)
    seen = {}

    def spy(spark, cfg_, ctx):
        seen.update(max_reject_pct=cfg_.max_reject_pct, reset=ctx.reset_baseline)
        return PipelineResult(1, 1, {})
    execute(cfg, spy, sleep=lambda s: None)
    assert seen == {"max_reject_pct": 2.0, "reset": False}
