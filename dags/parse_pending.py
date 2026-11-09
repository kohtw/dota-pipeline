"""
Hourly sweep: parse every replay in Garage that has no complete parsed output yet, then check
each match's output against docs/data-contract.md.

It works from what is in Garage, not from which DAG put it there, so it picks up replays from
dota_daily_download, a manual upload, or a bulk import alike. A replay is only skipped once its
parsed/_complete marker exists (written after all tables), so re-running parses nothing twice and
a half-finished parse runs again.

  find_unparsed -> parse_and_check (one per match: parse_replay -> check_contract)
                -> spark_clean -> dbt_build   (placeholders until those parts exist)
"""

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, get_current_context, task, task_group

from dota.alerts import ALERT_CALLBACKS
from dota.config import BUCKET, DOCKER_NETWORK, S3_ENV_VARS

BATCH = 20  # replays per run; the rest wait for the next hour
PARALLEL_PARSES = 2  # each parser container holds a whole replay in memory
# label mapped tasks with their match id (in the UI and in the alert log) instead of 0, 1, 2...
MATCH_LABEL = "{{ match_label }}"  # set by the task itself (TaskFlow tasks can't template their mapped args)
PARSE_LABEL = "{{ task.command[0].rsplit('/', 1)[-1].removesuffix('.dem') }}"  # s3a://raw/replays/{id}.dem


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def parse_pending():
    @task
    def find_unparsed() -> list[int]:
        from dota import contract, storage

        pending = contract.unparsed(storage.list_keys("replays/"),
                                    storage.list_keys(f"parsed/{contract.COMPLETE}/"))
        print(f"{len(pending)} replays unparsed; parsing {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task_group
    def parse_and_check(match_id: int):
        @task(map_index_template=MATCH_LABEL)
        def parser_args(match_id: int) -> list[str]:
            get_current_context()["match_label"] = str(match_id)
            return [f"s3a://{BUCKET}/replays/{match_id}.dem", f"s3a://{BUCKET}/parsed"]

        parse = DockerOperator(
            task_id="parse_replay",
            image="dota-parser",
            command=parser_args(match_id),
            docker_url="tcp://docker-proxy:2375",
            network_mode=DOCKER_NETWORK,
            private_environment={k: os.environ.get(k, "") for k in S3_ENV_VARS},  # hidden in the UI
            mount_tmp_dir=False,
            auto_remove="success",
            max_active_tis_per_dagrun=PARALLEL_PARSES,
            map_index_template=PARSE_LABEL,
        )

        # contract failures are not transient, so no retries
        @task(retries=0, map_index_template=MATCH_LABEL)
        def check_contract(match_id: int) -> dict[str, int]:
            from dota import contract, storage

            get_current_context()["match_label"] = str(match_id)

            return contract.check_parsed(match_id, storage.read, storage.list_keys)

        parse >> check_contract(match_id)

    spark_clean = EmptyOperator(task_id="spark_clean", doc_md="Placeholder for the Spark raw -> clean job.")
    dbt_build = EmptyOperator(task_id="dbt_build", doc_md="Placeholder for loading Postgres and `dbt build`.")

    parse_and_check.expand(match_id=find_unparsed()) >> spark_clean >> dbt_build


parse_pending()
