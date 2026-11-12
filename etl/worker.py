"""Queue worker. The API only ENQUEUES runs (status 'queued'); this process claims them and executes the pipeline with
the same retry/failure handling as the CLI. One Spark session is reused across runs.

  python -m etl.worker            run forever (docker compose --profile etl up -d etl_worker)
  python -m etl.worker --once     process at most one queued run and exit
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from typing import Callable

from app.logging_config import configure_logging
from etl import runlog
from etl.runner import Outcome, _load, execute

log = logging.getLogger("etl.worker")


def process_next(loader: Callable = _load, spark=None, *, backoff_s: float = 5.0, sleep=time.sleep,
                 only_pipelines: list[str] | None = None) -> Outcome | None:
    """Claim and run ONE queued run. Returns its Outcome, or None when the queue is empty."""
    claimed = runlog.claim_queued_run(only_pipelines)
    if claimed is None:
        return None
    try:
        make_cfg, run_fn = loader(claimed["pipeline"])
    except Exception as exc:  # noqa: BLE001 - e.g. a pipeline name with no implementation
        runlog.finish_run(claimed["id"], "failed", error=f"cannot load pipeline '{claimed['pipeline']}': {type(exc).__name__}")
        return Outcome("failed", [claimed["id"]], f"cannot load pipeline: {type(exc).__name__}")
    out = execute(make_cfg(), run_fn, spark=spark, trigger=claimed["trigger"], retry_of=claimed["retry_of"],
                  first_run_id=claimed["id"], backoff_s=backoff_s, sleep=sleep)
    try:                                         # deliver this run's alerts; an integration outage never fails the run
        from app.integrations.dispatch import dispatch_pending_alerts
        dispatch_pending_alerts(only_runs=out.run_ids)
    except Exception as exc:  # noqa: BLE001
        log.warning("alert dispatch skipped: %s", type(exc).__name__)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="etl.worker")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--poll", type=float, default=3.0, help="seconds between queue checks")
    a = ap.parse_args(argv)
    configure_logging("INFO")
    stop = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))
    from etl.common import get_spark
    spark = get_spark("etl-worker")
    try:
        while not stop["now"]:
            out = process_next(spark=spark)
            if out is not None:
                log.info("run finished", extra={"status": out.status, "run_ids": out.run_ids, "error": out.error})
            if a.once:
                break
            if out is None:
                time.sleep(a.poll)
    except KeyboardInterrupt:
        pass
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
