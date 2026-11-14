"""
Manual only: release replays from the cold store back into Garage (s3://raw/replays/{match_id}.dem)
on a timed schedule, for reprocessing and replay or burst tests. This is not an ingest path; live
replays still come from the API through dota_daily_download. See docs/cold-storage.md.

Takes the next `count` manifest entries in end_time order (after start_after_match_id if set)
that are neither in replays/ nor parsed, all from one seed run, so back-to-back runs (1x, then 2x)
get fresh matches. Releases them one at a time:
  uniform  one replay every 1 / (rate_per_min * speed) minutes
  event    the real gaps between end times, scaled so the mean rate is rate_per_min * speed
rate_per_min defaults to 2, the measured rate of live acquisition through the APIs.

Every release is logged as it happens to {cold_prefix}restore_log/{run_id}.jsonl in the cold
bucket. A replay that fails is logged and the rest still run; the task fails at the end if any
failed, and then emits no asset event. The hourly parse_pending sweep picks restored replays up
like any other.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Asset, Param, dag, get_current_context, task

from dota.alerts import ALERT_CALLBACKS
from dota.cold import COLD_PREFIX, MAX_RUN_S, SPACINGS
from dota.config import BUCKET

# The hot replay prefix the restore writes to. Nothing is scheduled on it yet; it belongs in
# dota/assets.py once something is.
REPLAYS_RESTORED = Asset(f"s3://{BUCKET}/replays")


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=ALERT_CALLBACKS,
    params={
        "count": Param(30, type="integer", minimum=1, description="replays to release"),
        "speed": Param(1.0, type="number", exclusiveMinimum=0, description="multiplies the rate"),
        "rate_per_min": Param(2.0, type="number", exclusiveMinimum=0,
                              description="base rate, replays per minute (live acquisition is about 2)"),
        "spacing": Param("event", enum=list(SPACINGS), description="event: real end-time gaps; uniform: even"),
        "start_after_match_id": Param(None, type=["null", "integer"],
                                      description="start after this match in end_time order"),
        "cold_bucket": Param(BUCKET, type="string", minLength=1),
        "cold_prefix": Param(COLD_PREFIX, type="string", minLength=1),
    },
    tags=["orchestration"],
)
def cold_restore():
    # No retries: a retry would restart the schedule from the beginning and overwrite the run's
    # log. Per-replay failures are already recorded and leave the rest of the run going.
    @task(execution_timeout=timedelta(seconds=MAX_RUN_S), retries=0, outlets=[REPLAYS_RESTORED])
    def release() -> dict[str, int]:
        from dota import cold, storage

        context = get_current_context()
        p = context["params"]
        start_after = p.get("start_after_match_id")
        rows = cold.restore(
            storage.client(), count=int(p["count"]), speed=float(p["speed"]),
            rate_per_min=float(p["rate_per_min"]), spacing=p["spacing"],
            start_after_match_id=int(start_after) if start_after is not None else None,
            cold_bucket=p["cold_bucket"], cold_prefix=p["cold_prefix"], hot_bucket=BUCKET,
            run_id=context["run_id"])
        print(f"restore finished: {cold.status_counts(rows)}")
        return cold.raise_if_failed(rows)

    release()


cold_restore()
