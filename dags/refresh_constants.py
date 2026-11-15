"""
Daily reference data refresh: heroes, items and patches from Valve's dota2.com datafeed, game modes
from OpenDota, into the warehouse with history. They change between patches (a hero renamed, an item
reworked), so every version observed is kept and dbt derives validity windows from it.
See docs/constants-lane.md.

  fetch -> one raw snapshot in Garage, constants/fetched_at={UTC}/{resource}.json.gz. Everything is
           parsed before anything is written, so a feed whose shape changed fails the run and
           saves nothing.
        -> load: read the snapshot back, parse it, append to stg.constant_* the rows that differ
           from each key's latest version (dota.constants_load). Emits CONSTANTS_LOADED, which
           wakes dbt_build; skipped when nothing changed (a skipped task emits no asset event).

Needs the DDL file warehouse/init/06_constants_tables.sql mounted like steam_summary_elt's (see
dota.config.WAREHOUSE_INIT_DIR). The DAG starts paused; until it runs, the dims are the seed
baseline in dbt/seeds/baseline_*.csv.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CONSTANTS_LOADED
from dota.config import API_RETRY, WAREHOUSE_INIT_DIR

DDL_PATH = WAREHOUSE_INIT_DIR / "06_constants_tables.sql"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def refresh_constants():
    # No pool: the datafeed is not shared with other DAGs, and the single OpenDota call is paced by
    # dota.opendota. Holding opendota_api for the ~3 min of herodata calls would stall downloads.
    @task(execution_timeout=timedelta(minutes=30), **API_RETRY)
    def fetch() -> str:
        from dota import constants_load, storage

        raw = constants_load.fetch_raw()
        prefix = constants_load.save_snapshot(raw, datetime.now(timezone.utc), storage.put)
        print(f"saved {prefix} ({len(raw['herodata'])} heroes)")
        return prefix

    # dbt reads stg.constant_*, so this load shares warehouse_write with the other loads and dbt_build
    @task(pool="warehouse_write", outlets=[CONSTANTS_LOADED])
    def load(prefix: str) -> dict:
        from dota import constants_load, storage, warehouse

        if not DDL_PATH.is_file():
            raise FileNotFoundError(
                f"{DDL_PATH} not found: mount ./warehouse/init at /opt/airflow/warehouse/init "
                "(docker-compose.yml) or set WAREHOUSE_INIT_DIR")
        conn = warehouse.connect()
        try:
            counts = constants_load.load_saved_snapshot(prefix, storage.read, conn, ddl=DDL_PATH.read_text())
        finally:
            conn.close()
        print(f"loaded {prefix}: new versions {counts}")
        if not any(counts.values()):
            raise AirflowSkipException("no reference data changed")
        return counts

    load(fetch())


refresh_constants()
