"""
Hourly: retry the replays dota_daily_download could not get yet (ingest.download_queue).

A queued match is tried once its next_try_at has passed, at the same OpenDota pace as the daily
download. On success it is marked done and parse_pending picks the replay up from Garage. Still not
ready: it is tried again an hour later (the task shows as skipped). After ingest.GIVE_UP_AFTER in the
queue the match is given up on and its task fails, which raises an alert.

  find_due -> retry_download (one per match)
"""

from datetime import datetime, timezone

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from dota.alerts import ALERT_CALLBACKS

BATCH = 50  # queued matches per run; the rest wait for the next hour
MATCH_LABEL = "{{ match_label }}"  # set by the task itself, so the UI lists mapped tasks by match id


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=ALERT_CALLBACKS,
)
def download_pending():
    @task
    def find_due() -> list[int]:
        from dota import ingest, warehouse

        conn = warehouse.connect()
        try:
            due = ingest.due(conn, limit=BATCH)
        finally:
            conn.close()
        print(f"{len(due)} queued replays due")
        return due

    # no task retries: the queue itself schedules the next try
    @task(pool="opendota_api", retries=0, map_index_template=MATCH_LABEL)
    def retry_download(match_id: int) -> str:
        from dota import ingest, opendota, storage, warehouse

        get_current_context()["match_label"] = str(match_id)
        ok, message = ingest.try_download(match_id, replay_url=opendota.replay_url, exists=storage.exists,
                                          upload=storage.upload_replay)
        conn = warehouse.connect()
        try:
            if ok:
                ingest.mark_done(conn, match_id)
                print(message)
                return message
            gave_up = ingest.mark_failed(conn, match_id, message)
        finally:
            conn.close()
        if gave_up:
            raise AirflowFailException(f"gave up on match {match_id} after {ingest.GIVE_UP_AFTER.days} days: {message}")
        raise AirflowSkipException(f"not downloadable yet, next try in an hour: {message}")

    retry_download.expand(match_id=find_due())


download_pending()
