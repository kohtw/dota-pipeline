"""
Clean sweep: run the Spark clean job on every match that has finished parsing but has no complete
clean output yet, then check each match's clean Parquet against docs/clean-contract.md.

It runs whenever parse_pending produces RAW_PARSED (new parsed output), and at :30 past every
hour as a fallback for a missed trigger. Like parse_pending it works from what is in Garage,
not from which run put it there, and a match is only skipped once its clean/_complete marker
exists (written last, after all 9 tables), so a half-finished clean runs again and nothing is
cleaned twice.

The clean container image `dota-spark-clean` is Maximus's (CONTRIBUTING.md §13) and does not exist
yet, so this DAG holds until he builds it — that is expected, not a failure.

  find_uncleaned -> clean_and_check (one per match: clear_stale_report -> spark_clean -> check_clean)
                    -> CLEAN_READY; and, per match, spark_clean -> quarantine_rejected (runs even if
                    spark_clean failed)

A match the clean job rejects (exit 2, report with errors, no _complete marker) is quarantined: its
spark_clean task stays red once (and alerts once), quarantine_rejected writes
clean/_rejected/match_id={id}/_REJECTED, and find_uncleaned skips it from then on. To clean such a
match again (after a fix), delete that marker object. clear_stale_report deletes the match's old
report.json before every clean, so a report found afterwards was written by that run. A missing
report (the container died before writing one) writes no marker and fails quarantine_rejected, so a
real crash is retried next run.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import (AssetOrTimeSchedule, CronTriggerTimetable, dag, get_current_context, task,
                        task_group)
from airflow.sdk.exceptions import AirflowFailException

from dota.alerts import ALERT_CALLBACKS
from dota.assets import CLEAN_READY, RAW_PARSED
from dota.config import BUCKET, DOCKER_NETWORK, JOB_MEM_LIMIT, S3_ENV_VARS
from dota.jobs import JobOperator

BATCH = 20  # matches per run; the rest wait for the next trigger
PARALLEL_CLEANS = 2  # each Spark clean container is heavy, so cap concurrent ones per run
# label mapped tasks with their match id (in the UI and in the alert log) instead of 0, 1, 2...
MATCH_LABEL = "{{ match_label }}"  # set by the task itself (TaskFlow tasks can't template their mapped args)
# the DockerOperator runs no Python, so nothing sets match_label in its context; derive its label
# from the command instead (command = [input_base, output_base, match_id], so command[2] is the id)
CLEAN_LABEL = "{{ task.command[2] }}"


@dag(
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("30 * * * *", timezone="UTC"),
                                 assets=RAW_PARSED),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def clean_pending():
    @task
    def find_uncleaned() -> list[int]:
        from dota import attempts, clean_contract, storage

        pending = clean_contract.uncleaned(storage.list_keys("parsed/_complete/"),
                                           storage.list_keys(f"clean/{clean_contract.COMPLETE}/"),
                                           storage.list_keys(f"clean/{clean_contract.REJECTED}/"),
                                           storage.list_keys(f"clean/{attempts.FAILED}/"))
        print(f"{len(pending)} matches uncleaned; cleaning {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task_group
    def clean_and_check(match_id: int):
        @task(map_index_template=MATCH_LABEL)
        def clean_args(match_id: int) -> list[str]:
            get_current_context()["match_label"] = str(match_id)
            return [f"s3a://{BUCKET}/parsed", f"s3a://{BUCKET}/clean", str(match_id)]

        # the clean job rewrites report.json only late in a run, so drop last run's before this one
        @task(map_index_template=MATCH_LABEL)
        def clear_stale_report(match_id: int) -> bool:
            from dota import clean_contract, storage

            get_current_context()["match_label"] = str(match_id)
            key = clean_contract.report_key(match_id)
            if storage.list_keys(key):
                storage.delete_key(key)
            return True

        cleared = clear_stale_report(match_id)

        spark_clean = JobOperator(
            task_id="spark_clean",
            image="dota-spark-clean",
            command=clean_args(match_id),
            docker_url="tcp://docker-proxy:2375",
            network_mode=DOCKER_NETWORK,
            private_environment={k: os.environ.get(k, "") for k in S3_ENV_VARS},  # hidden in the UI
            mount_tmp_dir=False,
            mem_limit=JOB_MEM_LIMIT,
            pool="spark",
            retries=1,  # JobOperator retries only a kill for memory, so a rejection still fails at once
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
        # spark_clean task. No report means the container crashed, so it records a failed attempt
        # and fails, but writes no _REJECTED marker.
        @task(trigger_rule="all_done", retries=0, map_index_template=MATCH_LABEL)
        def quarantine_rejected(match_id: int, cleared: bool | None) -> bool:
            import json

            from dota import attempts, clean_contract, storage

            context = get_current_context()
            context["match_label"] = str(match_id)
            if cleared is not True:  # clear_stale_report failed, so any report may be a previous run's
                raise AirflowFailException(f"stale report of match {match_id} was not cleared")
            report_key = clean_contract.report_key(match_id)
            if not storage.list_keys(report_key):
                storage.put(attempts.failure_key("clean", match_id, context["run_id"]))
                raise AirflowFailException(f"no clean report at {report_key}: clean job did not finish")
            if not clean_contract.is_rejected(json.loads(storage.read(report_key))):
                return False
            storage.put(clean_contract.rejected_key(match_id))
            print(f"match {match_id} rejected by the clean job; quarantined (delete the marker to retry)")
            return True

        cleared >> spark_clean
        spark_clean >> [check_clean(match_id), quarantine_rejected(match_id, cleared)]

    clean_and_check.expand(match_id=find_uncleaned())


clean_pending()
