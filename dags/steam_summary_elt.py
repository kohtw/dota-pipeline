"""
Steam load: flatten the raw Steam pages that the daily download saved in Garage (steam_raw/) and
load them into the warehouse tables stg.steam_matches / stg.steam_players. Python only flattens and
inserts; every rule (ranked All Pick, minimum duration, rates) lives in dbt.

Runs daily (no asset trigger). Like the other sweeps it works from what is in Garage: a page is
skipped only once its steam_raw/_loaded/ marker exists, and the marker is written AFTER that page's
load commits. A page that died before its marker is loaded again, which is safe because load_steam
is idempotent (delete the batch's matches, insert, one transaction). Pages go in sequence order so
the later page wins where pages overlap. It emits STEAM_LOADED when at least one page was loaded.

Needs the warehouse Postgres (WAREHOUSE_* env vars) and the DDL file warehouse/init/04_steam_tables.sql;
the Airflow container must mount it (default location is the repo's warehouse/init, override with
STEAM_DDL_PATH).

  find_pages -> load_pages (per page: read -> gunzip -> parse_page -> load_steam -> marker) -> STEAM_LOADED
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from dota.alerts import ALERT_CALLBACKS
from dota.assets import STEAM_LOADED

BATCH = 20  # pages per run; the rest wait for the next run
DDL_PATH = Path(os.environ.get(
    "STEAM_DDL_PATH", Path(__file__).resolve().parents[1] / "warehouse" / "init" / "04_steam_tables.sql"))


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

    @task(outlets=[STEAM_LOADED])
    def load_pages(page_keys: list[str]) -> int:
        import gzip
        import json

        from dota import steam_summary, storage, warehouse

        if not page_keys:
            raise AirflowSkipException("no unloaded Steam pages")  # a skipped task emits no asset event
        ddl = DDL_PATH.read_text()
        conn = warehouse.connect()
        try:
            for key in page_keys:  # in order: the later page wins on overlap
                matches, players = steam_summary.parse_page(json.loads(gzip.decompress(storage.read(key))))
                steam_summary.load_steam(matches, players, conn, ddl=ddl)
                storage.put(steam_summary.loaded_marker_key(key))  # only after the load committed
        finally:
            conn.close()
        return len(page_keys)

    load_pages(find_pages())


steam_summary_elt()
