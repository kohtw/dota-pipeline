"""
dbt stage: build the marts schema from the warehouse `clean` schema with `dbt build`.

Asset-triggered: it runs whenever the load DAG produces CLEAN_LOADED (fresh rows in the warehouse
`clean` schema), so the marts refresh follows the load without polling on a timer. This is the last
stage of the chain:

  parse -> RAW_PARSED -> clean -> CLEAN_READY -> load -> CLEAN_LOADED -> dbt_build (here)

Running it for real needs two things that are Chris's to wire up in docker-compose (CONTRIBUTING.md
rule 14): the `dbt/` project mounted into the Airflow container (at DBT_DIR), and the warehouse
Postgres reachable with the WAREHOUSE_* env vars that `dbt/profiles.yml`'s prod target reads. Until
both exist this DAG cannot run -- that is expected, not a failure. The task below is built and ready,
not live.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CLEAN_LOADED, STEAM_LOADED

# the dbt project + profiles live here inside the container; mounted by compose (Chris's area)
DBT_DIR = os.environ.get("DBT_DIR", "/opt/airflow/dbt")
# keep dbt's writable output (compiled artifacts, run_results, logs) off the repo / read-only mount
DBT_TARGET_PATH = "/data/dbt/target"
DBT_LOG_PATH = "/data/dbt/logs"


@dag(
    schedule=(CLEAN_LOADED | STEAM_LOADED),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def dbt_build():
    @task
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
        if result.returncode != 0:
            raise RuntimeError(f"dbt build failed (exit {result.returncode})")

    build()


dbt_build()
