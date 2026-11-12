"""
Clean sweep: run the Spark clean job on every match that has finished parsing but has no complete
clean output yet, then check each match's clean Parquet against docs/clean-contract.md.

Asset-triggered: it runs whenever parse_pending produces RAW_PARSED (new parsed output), so it
follows parsing without polling on a timer. Like parse_pending it works from what is in Garage,
not from which run put it there, and a match is only skipped once its clean/_complete marker
exists (written last, after all 9 tables), so a half-finished clean runs again and nothing is
cleaned twice. Airflow 3.3.2 is asset-only here (no combined asset+cron schedule is forced); the
fallback for a missed trigger is a manual DAG trigger (a future scheduled sweep could be added).

The clean container image `dota-spark-clean` is Maximus's (CONTRIBUTING.md §13) and does not exist
yet, so this DAG holds until he builds it — that is expected, not a failure.

  find_uncleaned -> clean_and_check (one per match: spark_clean -> check_clean) -> CLEAN_READY
                    and, per match, spark_clean -> quarantine_rejected (runs even if spark_clean failed)

A match the clean job rejects (exit 2, report with errors, no _complete marker) is quarantined: its
spark_clean task stays red once (and alerts once), quarantine_rejected writes
clean/_rejected/match_id={id}/_REJECTED, and find_uncleaned skips it from then on. To clean such a
match again (after a fix), delete that marker object. A missing report (the container died before
writing one) writes no marker, and fails quarantine_rejected, so a real crash is retried next run.
"""

import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, get_current_context, task, task_group

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CLEAN_READY, RAW_PARSED
from dota.config import BUCKET, DOCKER_NETWORK, S3_ENV_VARS

BATCH = 20  # matches per run; the rest wait for the next trigger
PARALLEL_CLEANS = 2  # each Spark clean container is heavy, so cap concurrent ones per run
# label mapped tasks with their match id (in the UI and in the alert log) instead of 0, 1, 2...
MATCH_LABEL = "{{ match_label }}"  # set by the task itself (TaskFlow tasks can't template their mapped args)
# the DockerOperator runs no Python, so nothing sets match_label in its context; derive its label
# from the command instead (command = [input_base, output_base, match_id], so command[2] is the id)
CLEAN_LABEL = "{{ task.command[2] }}"


@dag(
    schedule=[RAW_PARSED],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def clean_pending():
    @task
    def find_uncleaned() -> list[int]:
        from dota import clean_contract, storage

        pending = clean_contract.uncleaned(storage.list_keys("parsed/_complete/"),
                                           storage.list_keys(f"clean/{clean_contract.COMPLETE}/"),
                                           storage.list_keys(f"clean/{clean_contract.REJECTED}/"))
        print(f"{len(pending)} matches uncleaned; cleaning {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task_group
    def clean_and_check(match_id: int):
        @task(map_index_template=MATCH_LABEL)
        def clean_args(match_id: int) -> list[str]:
            get_current_context()["match_label"] = str(match_id)
            return [f"s3a://{BUCKET}/parsed", f"s3a://{BUCKET}/clean", str(match_id)]

        spark_clean = DockerOperator(
            task_id="spark_clean",
            image="dota-spark-clean",
            command=clean_args(match_id),
            docker_url="tcp://docker-proxy:2375",
            network_mode=DOCKER_NETWORK,
            private_environment={k: os.environ.get(k, "") for k in S3_ENV_VARS},  # hidden in the UI
            mount_tmp_dir=False,
            auto_remove="success",
            max_active_tis_per_dagrun=PARALLEL_CLEANS,
            map_index_template=CLEAN_LABEL,
        )

        # contract failures are not transient, so no retries
        @task(retries=0, map_index_template=MATCH_LABEL, outlets=[CLEAN_READY])
        def check_clean(match_id: int) -> dict[str, int]:
            from dota import clean_contract, storage

            get_current_context()["match_label"] = str(match_id)

            return clean_contract.check_parsed(match_id, storage.read, storage.list_keys)

        # Runs after spark_clean succeeds or fails. It ends green when it wrote a marker or had
        # nothing to do (the match was cleaned); the rejection itself stays visible as the red
        # spark_clean task. No report means the container crashed, so it fails and writes no marker.
        @task(trigger_rule="all_done", retries=0, map_index_template=MATCH_LABEL)
        def quarantine_rejected(match_id: int) -> bool:
            import json

            from dota import clean_contract, storage

            get_current_context()["match_label"] = str(match_id)
            report_key = f"clean/_review/match_id={match_id}/report.json"
            if not storage.list_keys(report_key):
                raise AirflowFailException(f"no clean report at {report_key}: clean job did not finish")
            if not clean_contract.is_rejected(json.loads(storage.read(report_key))):
                return False
            storage.put(f"clean/{clean_contract.REJECTED}/match_id={match_id}/_REJECTED")
            print(f"match {match_id} rejected by the clean job; quarantined (delete the marker to retry)")
            return True

        spark_clean >> [check_clean(match_id), quarantine_rejected(match_id)]

    clean_and_check.expand(match_id=find_uncleaned())


clean_pending()
