"""
dbt stage: build the marts schema from the warehouse `clean` schema with `dbt build`.

Asset-triggered: it runs whenever a load produces CLEAN_LOADED (fresh rows in the warehouse `clean`
schema), STEAM_LOADED (fresh stg.steam_* rows) or CONSTANTS_LOADED (new reference data versions in
stg.constant_*), so the marts refresh follows the load without polling on a timer. This is the last
stage of the chain:

  parse -> RAW_PARSED -> clean -> CLEAN_READY -> load -> CLEAN_LOADED -> dbt_build (here)

It needs the `dbt/` project and `warehouse/init` mounted into the Airflow container and the warehouse
Postgres reachable with the WAREHOUSE_* env vars that `dbt/profiles.yml`'s prod target reads;
docker-compose.yml provides all three.

  apply_schema -> build

apply_schema runs every warehouse/init/*.sql first. Init SQL runs on its own only on an empty
warehouse volume, and some models read tables that a loader creates later (stg.steam_matches), so
on an older volume the build would fail until that loader had run. The same goes for
stg.constant_*, which the dims read even before refresh_constants has ever run.

Both tasks take the one-slot pool warehouse_write, shared with the loads in load_pending and
steam_summary_elt. dbt tests compare live clean.* tables with marts built earlier in the same run,
so a load committing mid-build failed them.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CLEAN_LOADED, CONSTANTS_LOADED, STEAM_LOADED
from dota.config import WAREHOUSE_INIT_DIR

# the dbt project + profiles live here inside the container; docker-compose mounts ./dbt there
DBT_DIR = os.environ.get("DBT_DIR", "/opt/airflow/dbt")
# keep dbt's writable output (compiled artifacts, run_results, logs) off the repo / read-only mount
DBT_TARGET_PATH = "/data/dbt/target"
DBT_LOG_PATH = "/data/dbt/logs"


@dag(
    schedule=(CLEAN_LOADED | STEAM_LOADED | CONSTANTS_LOADED),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def dbt_build():
    @task(pool="warehouse_write")
    def apply_schema() -> list[str]:
        from dota import warehouse

        conn = warehouse.connect()
        try:
            applied = warehouse.apply_init_sql(WAREHOUSE_INIT_DIR, conn)
        finally:
            conn.close()
        print(f"applied {applied} from {WAREHOUSE_INIT_DIR}")
        return applied

    @task(pool="warehouse_write")
    def build() -> None:
        # inherit the container env (carries the WAREHOUSE_* vars from compose) and point dbt's
        # writable output at /data so nothing is written into the repo / profiles mount
        env = {**os.environ, "DBT_TARGET_PATH": DBT_TARGET_PATH, "DBT_LOG_PATH": DBT_LOG_PATH}

        result = subprocess.run(
            ["dbt", "build", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR, "--target", "prod"],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        print(result.stderr)
        # exit 1 is a model or test failure, which a rerun repeats; 2 is an error such as a lost connection
        if result.returncode == 1:
            raise AirflowFailException("dbt build failed (exit 1)")
        if result.returncode != 0:
            raise RuntimeError(f"dbt build failed (exit {result.returncode})")

    apply_schema() >> build()


dbt_build()
