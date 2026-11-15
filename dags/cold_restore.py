"""
Manual only: release replays from the cold store back into Garage (s3://raw/replays/{match_id}.dem)
on a timed schedule, for reprocessing and replay or burst tests. This is not an ingest path; live
replays still come from the API through download_pending. See docs/cold-storage.md.

plan takes the next `count` manifest entries in end_time order (after start_after_match_id if set)
that are neither in replays/ nor parsed, all from one seed run, so back-to-back runs (1x, then 2x)
get fresh matches. It fixes the start T0 a little after now and each replay's release time:
  uniform  one replay every 1 / (rate_per_min * speed) minutes
  event    the real gaps between end times, scaled so the mean rate is rate_per_min * speed
rate_per_min defaults to 2, the measured rate of live acquisition through the APIs.

  plan -> release_one (one per replay: wait -> release) -> REPLAYS_ARRIVED, once per replay

wait defers to the triggerer until the replay's time, so waiting holds no worker slot. Each release
emits REPLAYS_ARRIVED, which wakes parse_pending, and writes its own log row under
{cold_prefix}restore_log/{run_id}/ next to the plan. A replay already in replays/ is skipped; a
failed release fails only its own task.
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync
from airflow.sdk import Param, dag, get_current_context, task, task_group

from dota.alerts import ALERT_CALLBACKS
from dota.assets import REPLAYS_ARRIVED
from dota.cold import COLD_PREFIX, MAX_PLAN_S, SPACINGS
from dota.config import BUCKET

PARALLEL_RELEASES = 4  # releases that may overlap when replays are due close together
MATCH_LABEL = "{{ match_label }}"  # set by the task itself, so mapped tasks are listed by match id


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
    @task(retries=0)
    def plan() -> list[dict]:
        from dota import cold, storage

        context = get_current_context()
        p = context["params"]
        start_after = p.get("start_after_match_id")
        planned = cold.plan_restore(
            storage.client(), count=int(p["count"]), speed=float(p["speed"]),
            rate_per_min=float(p["rate_per_min"]), spacing=p["spacing"],
            start_after_match_id=int(start_after) if start_after is not None else None,
            cold_bucket=p["cold_bucket"], cold_prefix=p["cold_prefix"], hot_bucket=BUCKET,
            run_id=context["run_id"])
        return [{"scheduled_at": item["scheduled_at"], "item": item} for item in planned["items"]]

    @task_group
    def release_one(scheduled_at: str, item: dict):
        wait = DateTimeSensorAsync(task_id="wait", target_time=scheduled_at,
                                   timeout=timedelta(seconds=MAX_PLAN_S) + timedelta(hours=1))

        # No retries: a late second try would blur the timing the run is there to measure.
        @task(retries=0, outlets=[REPLAYS_ARRIVED], max_active_tis_per_dagrun=PARALLEL_RELEASES,
              map_index_template=MATCH_LABEL)
        def release(item: dict) -> dict:
            from airflow.sdk.exceptions import AirflowSkipException

            from dota import cold, storage
            from dota.errors import PermanentError

            get_current_context()["match_label"] = str(item["match_id"])
            row = cold.release_item(storage.client(), item, hot_bucket=BUCKET)
            print(f"match {row['match_id']}: {row['status']}, scheduled {row['scheduled_at']}, "
                  f"released {row['released_at']}")
            # a skipped task emits no asset event, so parse_pending only wakes for real arrivals
            if row["status"] == "skipped":
                raise AirflowSkipException(f"replays/{row['match_id']}.dem is already in Garage")
            if row["status"] == "failed":
                raise PermanentError(f"match {row['match_id']} not restored: {row['error']}")
            return row

        wait >> release(item)

    release_one.expand_kwargs(plan())


cold_restore()
