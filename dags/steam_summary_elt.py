"""
Steam load: flatten the raw Steam pages that the daily download saved in Garage (steam_raw/) and
load them into the warehouse tables stg.steam_matches / stg.steam_players. Python only flattens and
inserts; every rule (ranked All Pick, minimum duration, rates) lives in dbt.

Runs daily (no asset trigger). Like the other sweeps it works from what is in Garage: a page is
skipped only once its steam_raw/_loaded/ marker exists, and the marker is written AFTER that page's
load commits. A page that died before its marker is loaded again, which is safe because load_steam
is idempotent (delete the batch's matches, insert, one transaction). Pages go in sequence order so
the later page wins where pages overlap. It emits STEAM_LOADED when at least one page was loaded.

A page that fails (corrupt file, database error) is logged, left unmarked and retried next run; it
does not block the pages after it. The run still ends failed so the alert callbacks fire.

Needs the warehouse Postgres (WAREHOUSE_* env vars) and the DDL file warehouse/init/04_steam_tables.sql,
mounted into the Airflow services by docker-compose.yml (./warehouse/init at
/opt/airflow/warehouse/init; dota.config.WAREHOUSE_INIT_DIR, override with the env var of that name).
Without the mount the load task fails on every run.

  find_pages -> load_pages (DDL read, then per page: read -> gunzip -> parse -> load_steam -> marker)
             -> emit_loaded (outlet STEAM_LOADED; skipped when nothing loaded)
             -> report_failures (fails the run if any page failed; runs even when nothing loaded)
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from dota.alerts import ALERT_CALLBACKS
from dota.assets import STEAM_LOADED
from dota.config import WAREHOUSE_INIT_DIR

BATCH = 20  # pages per run; the rest wait for the next run
DDL_PATH = WAREHOUSE_INIT_DIR / "04_steam_tables.sql"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def steam_summary_elt():
    @task
    def find_pages() -> list[str]:
        from dota import steam_summary, storage

        pending = steam_summary.unloaded_pages(storage.list_keys(steam_summary.RAW_PREFIX),
                                               storage.list_keys(steam_summary.LOADED_PREFIX))
        print(f"{len(pending)} Steam pages unloaded; loading {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task
    def load_pages(page_keys: list[str]) -> dict:
        from dota import steam_summary, storage, warehouse

        if not DDL_PATH.is_file():  # not a per-page failure: fail the task outright
            raise FileNotFoundError(
                f"{DDL_PATH} not found: mount ./warehouse/init at /opt/airflow/warehouse/init "
                "(docker-compose.yml) or set WAREHOUSE_INIT_DIR")
        ddl = DDL_PATH.read_text()
        if not page_keys:
            return {"loaded": [], "failed": {}}
        conn = warehouse.connect()
        try:
            loaded, failed = steam_summary.load_pages_isolated(
                page_keys, storage.read,
                lambda matches, players: steam_summary.load_steam(matches, players, conn, ddl=ddl),
                lambda key: storage.put(steam_summary.loaded_marker_key(key)))
        finally:
            conn.close()
        return {"loaded": loaded, "failed": failed}

    @task(outlets=[STEAM_LOADED])
    def emit_loaded(result: dict) -> int:
        if not result["loaded"]:
            raise AirflowSkipException("no Steam pages loaded")  # a skipped task emits no asset event
        return len(result["loaded"])

    @task(trigger_rule="all_done")
    def report_failures(result: dict) -> None:
        if result["failed"]:
            raise AirflowFailException(f"{len(result['failed'])} Steam pages failed: {result['failed']}")

    result = load_pages(find_pages())
    emit_loaded(result)
    report_failures(result)


steam_summary_elt()
