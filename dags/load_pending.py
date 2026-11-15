"""
Load sweep: load every cleaned match's Parquet from Garage into Postgres schema `clean`.

It runs whenever clean_pending produces CLEAN_READY, and at :45 past every hour as a fallback for
a missed trigger. Like the other sweeps it works from what is in Garage, not from which run put it
there: a match is skipped only once its clean/_loaded marker exists, and that marker is written
AFTER the match's DB load commits. A load that died before the marker runs again, which is safe
because load_match is idempotent (delete the match's rows, insert, one transaction).

Needs the warehouse Postgres (the `warehouse` service in docker-compose; WAREHOUSE_* env vars).

  find_unloaded + apply_schema -> load_and_retain (one per match: load_one -> retain_dem) -> CLEAN_LOADED

apply_schema and load_one share the one-slot pool warehouse_write with dbt_build and the Steam load,
so a load waits for a running build and a build sees no half-loaded batch.

load_one runs load_match, then writes the _loaded marker. retain_dem then applies the Airflow
Variable `dem_retention` (keep | delete, default keep) to the match's raw replays/{id}.dem, but
only if retention.eligible: the match is loaded and its clean output re-passes the clean contract.
"""

from datetime import datetime, timedelta

from airflow.sdk import (AssetOrTimeSchedule, CronTriggerTimetable, Variable, dag, get_current_context,
                        task, task_group)

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CLEAN_LOADED, CLEAN_READY
from dota.config import LOAD_BATCH

BATCH = LOAD_BATCH  # matches per run; the rest wait for the next trigger
# The one-slot warehouse_write pool runs loads one at a time and never during a dbt build, which
# compares clean.* with marts built earlier in the same run. This cap only matters if it widens.
PARALLEL_LOADS = 4
# label mapped tasks with their match id (in the UI and in the alert log) instead of 0, 1, 2...
MATCH_LABEL = "{{ match_label }}"  # set by the task itself (TaskFlow tasks can't template their mapped args)


@dag(
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("45 * * * *", timezone="UTC"),
                                 assets=CLEAN_READY),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def load_pending():
    @task(pool="warehouse_write")
    def apply_schema() -> list[str]:
        # dbt also applies repeat-safe init SQL, but it runs AFTER loading. Apply
        # additions here first so older volumes can receive the new credit fields.
        from dota import warehouse
        from dota.config import WAREHOUSE_INIT_DIR

        conn = warehouse.connect()
        try:
            return warehouse.apply_init_sql(WAREHOUSE_INIT_DIR, conn)
        finally:
            conn.close()

    @task
    def find_unloaded() -> list[int]:
        from dota import storage, warehouse

        pending = warehouse.unloaded(storage.list_keys("clean/_complete/"),
                                     storage.list_keys("clean/_loaded/"))
        print(f"{len(pending)} matches unloaded; loading {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task_group
    def load_and_retain(match_id: int):
        @task(map_index_template=MATCH_LABEL, max_active_tis_per_dagrun=PARALLEL_LOADS, pool="warehouse_write",
              outlets=[CLEAN_LOADED])
        def load_one(match_id: int) -> dict[str, int]:
            from dota import clean_contract, storage, warehouse

            get_current_context()["match_label"] = str(match_id)

            # Recheck old completion markers against the current contract too.
            clean_contract.check_parsed(match_id, storage.read, storage.list_keys)
            conn = warehouse.connect()  # one connection per mapped task instance
            try:
                counts = warehouse.load_match(match_id, storage.read, storage.list_keys, conn)
                storage.put(warehouse.loaded_marker_key(match_id))  # only after the load committed
                return counts
            finally:
                conn.close()

        @task(map_index_template=MATCH_LABEL)
        def retain_dem(match_id: int) -> str:
            from dota import retention, storage

            get_current_context()["match_label"] = str(match_id)

            mode = Variable.get("dem_retention", "keep")
            if not retention.eligible(match_id, storage.read, storage.list_keys, storage.exists):
                print(f"{match_id}: not eligible for retention, .dem left alone")
                return "ineligible"
            outcome = retention.apply_retention(match_id, mode, exists=storage.exists,
                                                delete_key=storage.delete_key)
            print(f"{match_id}: dem_retention={mode.strip().lower()} -> {outcome}")
            return outcome

        load_one(match_id) >> retain_dem(match_id)

    schema_ready = apply_schema()
    loads = load_and_retain.expand(match_id=find_unloaded())
    schema_ready >> loads


load_pending()
