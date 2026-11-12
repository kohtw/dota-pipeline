"""
Load sweep: load every cleaned match's Parquet from Garage into Postgres schema `clean`.

Asset-triggered: it runs whenever clean_pending produces CLEAN_READY. Like the other sweeps it
works from what is in Garage, not from which run put it there: a match is skipped only once its
clean/_loaded marker exists, and that marker is written AFTER the match's DB load commits. A load
that died before the marker runs again, which is safe because load_match is idempotent (delete
the match's rows, insert, one transaction).

Needs the warehouse Postgres (the `warehouse` service in docker-compose; WAREHOUSE_* env vars).

  find_unloaded -> load_and_retain (one per match: load_one -> retain_dem) -> CLEAN_LOADED

load_one runs load_match, then writes the _loaded marker. retain_dem then applies the Airflow
Variable `dem_retention` (keep | delete, default keep) to the match's raw replays/{id}.dem, but
only if retention.eligible: the match is loaded and its clean output re-passes the clean contract.
"""

from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, get_current_context, task, task_group

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CLEAN_LOADED, CLEAN_READY

BATCH = 20  # matches per run; the rest wait for the next trigger
PARALLEL_LOADS = 4  # load is I/O-bound, so a few at once is fine
# label mapped tasks with their match id (in the UI and in the alert log) instead of 0, 1, 2...
MATCH_LABEL = "{{ match_label }}"  # set by the task itself (TaskFlow tasks can't template their mapped args)


@dag(
    schedule=[CLEAN_READY],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def load_pending():
    @task
    def find_unloaded() -> list[int]:
        from dota import storage, warehouse

        pending = warehouse.unloaded(storage.list_keys("clean/_complete/"),
                                     storage.list_keys("clean/_loaded/"))
        print(f"{len(pending)} matches unloaded; loading {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task_group
    def load_and_retain(match_id: int):
        @task(map_index_template=MATCH_LABEL, max_active_tis_per_dagrun=PARALLEL_LOADS, outlets=[CLEAN_LOADED])
        def load_one(match_id: int) -> dict[str, int]:
            from dota import storage, warehouse

            get_current_context()["match_label"] = str(match_id)

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

    load_and_retain.expand(match_id=find_unloaded())


load_pending()
