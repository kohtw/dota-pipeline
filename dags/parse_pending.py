"""
Parse sweep: parse every replay in Garage that has no complete parsed output yet, then check
each match's output against docs/data-contract.md.

It runs whenever download_pending or cold_restore produces REPLAYS_ARRIVED (new replays), and at
:00 past every hour as a fallback for a missed trigger.

It works from what is in Garage, not from which DAG put it there, so it picks up replays from
download_pending, a manual upload, or a bulk import alike. A replay is only skipped once its
parsed/_complete marker exists (written after all tables), so re-running parses nothing twice and
a half-finished parse runs again.
A replay whose parser container crashed in 3 runs (a non-zero exit other than 137, counted by
record_parse_failure under parsed/_failed/match_id={id}/) is skipped; delete those keys to retry it.

  find_unparsed -> parse_and_check (one per match: parse_replay -> check_contract) -> RAW_PARSED

This is the FIRST stage of the asset chain: a successful parse+check produces RAW_PARSED, which
wakes clean_pending, and so on down the chain (clean -> load -> dbt).
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import AssetOrTimeSchedule, CronTriggerTimetable, dag, get_current_context, task, task_group

from dota.alerts import ALERT_CALLBACKS
from dota.assets import RAW_PARSED, REPLAYS_ARRIVED
from dota.config import BUCKET, DOCKER_NETWORK, JOB_MEM_LIMIT, S3_ENV_VARS
from dota.jobs import EXIT_CODE_KEY, JobOperator

BATCH = 20  # replays per run; the rest wait for the next run
PARALLEL_PARSES = 2  # each parser container holds a whole replay in memory
# label mapped tasks with their match id (in the UI and in the alert log) instead of 0, 1, 2...
MATCH_LABEL = "{{ match_label }}"  # set by the task itself (TaskFlow tasks can't template their mapped args)
PARSE_LABEL = "{{ task.command[0].rsplit('/', 1)[-1].removesuffix('.dem') }}"  # s3a://raw/replays/{id}.dem


@dag(
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
                                 assets=REPLAYS_ARRIVED),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), **ALERT_CALLBACKS},
    tags=["orchestration"],
)
def parse_pending():
    @task
    def find_unparsed() -> list[int]:
        from dota import attempts, contract, storage

        pending = contract.unparsed(storage.list_keys("replays/"),
                                    storage.list_keys(f"parsed/{contract.COMPLETE}/"),
                                    storage.list_keys(f"parsed/{attempts.FAILED}/"))
        print(f"{len(pending)} replays unparsed; parsing {min(len(pending), BATCH)} this run")
        return pending[:BATCH]

    @task_group
    def parse_and_check(match_id: int):
        @task(map_index_template=MATCH_LABEL)
        def parser_args(match_id: int) -> list[str]:
            get_current_context()["match_label"] = str(match_id)
            return [f"s3a://{BUCKET}/replays/{match_id}.dem", f"s3a://{BUCKET}/parsed"]

        parse = JobOperator(
            task_id="parse_replay",
            image="dota-parser",
            command=parser_args(match_id),
            docker_url="tcp://docker-proxy:2375",
            network_mode=DOCKER_NETWORK,
            private_environment={k: os.environ.get(k, "") for k in S3_ENV_VARS},  # hidden in the UI
            mount_tmp_dir=False,
            mem_limit=JOB_MEM_LIMIT,
            pool="spark",
            retries=1,
            max_active_tis_per_dagrun=PARALLEL_PARSES,
            map_index_template=PARSE_LABEL,
        )

        # contract failures are not transient, so no retries
        @task(retries=0, map_index_template=MATCH_LABEL, outlets=[RAW_PARSED])
        def check_contract(match_id: int) -> dict[str, int]:
            from dota import contract, storage

            get_current_context()["match_label"] = str(match_id)

            return contract.check_parsed(match_id, storage.read, storage.list_keys)

        # Runs after parse_replay succeeds or fails. A run that left no _complete marker counts as
        # one failed attempt, however many times it was retried, but only if the parser crashed:
        # a missing image or an unreachable docker-proxy is not the replay's fault.
        @task(trigger_rule="all_done", map_index_template=MATCH_LABEL)
        def record_parse_failure(match_id: int) -> bool:
            from dota import attempts, contract, storage

            context = get_current_context()
            context["match_label"] = str(match_id)
            if storage.list_keys(contract.partition_prefix(contract.COMPLETE, match_id)):
                return False
            ti = context["ti"]
            # without map_indexes, Airflow 3 returns this key from every mapped parse_replay
            exit_code = ti.xcom_pull(task_ids=parse.task_id, key=EXIT_CODE_KEY, map_indexes=ti.map_index)
            if not attempts.counts_as_crash(exit_code, "parsed"):
                print(f"match {match_id} did not parse (exit code {exit_code}); not counted as a crash")
                return False
            key = attempts.failure_key("parsed", match_id, context["run_id"])
            storage.put(key)
            print(f"match {match_id} did not parse; recorded {key}")
            return True

        parse >> [check_contract(match_id), record_parse_failure(match_id)]

    parse_and_check.expand(match_id=find_unparsed())


parse_pending()
