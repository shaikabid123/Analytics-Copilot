"""Queue worker semantics (stub pipelines, no Spark): claiming, ordering, concurrency, failure handling."""
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etl import runlog, worker
from etl.common import ContractError, PipelineConfig, TransientError
from etl.runner import PipelineResult
from etl.tests.conftest import drop_test_artifacts


@pytest.fixture()
def pipes(etl_engine, app_engine):
    names = []

    def make(n=1):
        out = []
        for _ in range(n):
            name = f"unit_stub_{uuid.uuid4().hex[:8]}"
            runlog.ensure_pipeline(name)
            names.append(name)
            out.append(name)
        return out
    yield make
    drop_test_artifacts(etl_engine, app_engine, "unit\\_stub\\_%")


def enqueue(app_engine, name, trigger="manual", retry_of=None):
    with app_engine.begin() as c:
        return c.execute(text("INSERT INTO app.pipeline_runs (pipeline_id, status, trigger, attempt, retry_of_run_id) "
                              "SELECT id, 'queued', :t, 1, :r FROM app.pipelines WHERE name=:n RETURNING id"),
                         {"t": trigger, "r": retry_of, "n": name}).scalar()


def loader_for(fn):
    return lambda name: (lambda: PipelineConfig(pipeline_name=name), fn)


def rows(app_engine, name):
    with app_engine.connect() as c:
        return c.execute(text("SELECT r.id, r.status, r.trigger, r.attempt, r.retry_of_run_id FROM app.pipeline_runs r "
                              "JOIN app.pipelines p ON p.id=r.pipeline_id WHERE p.name=:n ORDER BY r.id"), {"n": name}).all()


ok = lambda spark, cfg, ctx: PipelineResult(3, 2, {"stub": True})   # noqa: E731


def test_empty_queue_returns_none(pipes):
    (name,) = pipes()
    assert worker.process_next(loader_for(ok), only_pipelines=[name]) is None


def test_claimed_run_is_executed_in_place_not_duplicated(pipes, app_engine):
    (name,) = pipes()
    rid = enqueue(app_engine, name)
    out = worker.process_next(loader_for(ok), only_pipelines=[name], sleep=lambda s: None)
    assert out.status == "succeeded" and out.run_ids == [rid]
    assert rows(app_engine, name) == [(rid, "succeeded", "manual", 1, None)]            # the SAME row, no second one
    with app_engine.connect() as c:
        acts = c.execute(text("SELECT actor_label, action FROM app.audit_events WHERE resource_type='pipeline_run' AND resource_id=:r ORDER BY id"), {"r": str(rid)}).all()
    assert acts == [("etl-worker", "pipeline.run.start"), ("etl", "pipeline.run.succeeded")]


def test_trigger_and_retry_link_survive_the_queue(pipes, app_engine):
    (name,) = pipes()
    first = enqueue(app_engine, name)
    worker.process_next(loader_for(lambda s, c, x: (_ for _ in ()).throw(ContractError("bad input"))), only_pipelines=[name])
    second = enqueue(app_engine, name, trigger="retry", retry_of=first)
    out = worker.process_next(loader_for(ok), only_pipelines=[name])
    assert out.status == "succeeded"
    assert rows(app_engine, name) == [(first, "failed", "manual", 1, None), (second, "succeeded", "retry", 1, first)]


def test_oldest_queued_run_goes_first(pipes, app_engine):
    a, b = pipes(2)
    ra, rb = enqueue(app_engine, a), enqueue(app_engine, b)
    seen = []
    def spy(spark, cfg, ctx):
        seen.append(cfg.pipeline_name)
        return PipelineResult(1, 1, {})
    worker.process_next(loader_for(spy), only_pipelines=[a, b])
    worker.process_next(loader_for(spy), only_pipelines=[a, b])
    assert seen == [a, b] and ra < rb


def test_failures_use_the_normal_retry_rules(pipes, app_engine):
    (name,) = pipes()
    rid = enqueue(app_engine, name)
    calls = []
    def flaky(spark, cfg, ctx):
        calls.append(ctx.run_id)
        if len(calls) < 2:
            raise TransientError("db hiccup")
        return PipelineResult(1, 1, {})
    out = worker.process_next(loader_for(flaky), only_pipelines=[name], backoff_s=0, sleep=lambda s: None)
    assert out.status == "succeeded" and out.attempts == 2 and out.run_ids[0] == rid
    assert [r[1] for r in rows(app_engine, name)] == ["retrying", "succeeded"]          # claimed row, then a linked retry row
    (name2,) = pipes()
    enqueue(app_engine, name2)
    bad = worker.process_next(loader_for(lambda s, c, x: (_ for _ in ()).throw(ContractError("x"))), only_pipelines=[name2])
    assert bad.status == "failed" and bad.attempts == 1                                  # non-retryable: one attempt


def test_unloadable_pipeline_marks_the_run_failed_instead_of_crashing_the_worker(pipes, app_engine):
    (name,) = pipes()
    rid = enqueue(app_engine, name)
    def broken_loader(n):
        raise ModuleNotFoundError("no implementation")
    out = worker.process_next(broken_loader, only_pipelines=[name])
    assert out.status == "failed" and "cannot load pipeline" in out.error
    with app_engine.connect() as c:
        assert c.execute(text("SELECT status, error_message FROM app.pipeline_runs WHERE id=:i"), {"i": rid}).one()[0] == "failed"


def test_two_workers_never_claim_the_same_run(pipes, app_engine):
    (name,) = pipes()
    rid = enqueue(app_engine, name)
    with ThreadPoolExecutor(4) as ex:
        results = list(ex.map(lambda _: runlog.claim_queued_run([name]), range(4)))
    claimed = [r for r in results if r]
    assert len(claimed) == 1 and claimed[0]["id"] == rid                                 # SKIP LOCKED: exactly one winner
    a, b = pipes(2)
    ra, rb = enqueue(app_engine, a), enqueue(app_engine, b)
    with ThreadPoolExecutor(2) as ex:
        got = [r for r in ex.map(lambda _: runlog.claim_queued_run([a, b]), range(2)) if r]
    assert sorted(r["id"] for r in got) == sorted([ra, rb])                              # two workers, two different runs


def test_database_allows_only_one_queued_or_running_run_per_pipeline(pipes, app_engine):
    (name,) = pipes()
    enqueue(app_engine, name)
    with pytest.raises(IntegrityError):
        enqueue(app_engine, name)
    with app_engine.begin() as c:
        c.execute(text("UPDATE app.pipeline_runs SET status='running' WHERE pipeline_id=(SELECT id FROM app.pipelines WHERE name=:n)"), {"n": name})
    with pytest.raises(IntegrityError):
        enqueue(app_engine, name)


def test_worker_and_runner_entrypoints_import_without_any_web_framework():
    """The Spark/ETL image has no FastAPI/Starlette; importing the worker must not need them (checked in a clean subprocess)."""
    import subprocess, sys
    code = ("import sys, etl.worker, etl.runner; import app.logging_config, app.integrations.dispatch; "
            "bad = [m for m in ('fastapi', 'starlette', 'uvicorn', 'jwt') if m in sys.modules]; "
            "print('LEAK', bad) if bad else print('CLEAN')")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert out.stdout.strip() == "CLEAN", (out.stdout, out.stderr[-400:])
