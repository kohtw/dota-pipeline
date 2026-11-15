"""
Download the queued replays (ingest.download_queue) into Garage (s3://raw/replays/{match_id}.dem).

Hourly, and at once when dota_daily_download queues the day's picks (the DOWNLOADS_QUEUED asset).
Takes up to LIMIT matches whose next try is due and downloads them in batches of BATCH, one mapped
task per batch, at the OpenDota pace; the task log lists what happened to each match.
  downloaded (or already in Garage)  marked done; parse_pending picks the replay up from Garage
  not ready yet (no replay_url, a rate limit)  tried again an hour later
  in the queue for ingest.GIVE_UP_AFTER, or a permanent error (e.g. not a Dota 2 replay)
                                     given up on; the task fails at the end of its batch, which
                                     raises an alert

  find_due -> download_replays (one per batch)
"""

from datetime import datetime, timezone

from airflow.sdk import AssetOrTimeSchedule, CronTriggerTimetable, dag, get_current_context, task

from dota.alerts import ALERT_CALLBACKS
from dota.assets import DOWNLOADS_QUEUED, REPLAYS_ARRIVED

LIMIT = 50  # queued matches per run; the rest wait for the next hour
BATCH = 25  # matches per download task
BATCH_LABEL = "{{ batch_label }}"  # set by the task itself, so the UI lists mapped tasks by their match ids


@dag(
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
                                 assets=DOWNLOADS_QUEUED),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=ALERT_CALLBACKS,
)
def download_pending():
    @task
    def find_due() -> list[list[int]]:
        from dota import ingest, warehouse

        conn = warehouse.connect()
        try:
            due = ingest.due(conn, limit=LIMIT)
        finally:
            conn.close()
        print(f"{len(due)} queued replays due")
        return [due[i:i + BATCH] for i in range(0, len(due), BATCH)]

    # no task retries: the queue itself schedules the next try
    @task(pool="opendota_api", retries=0, map_index_template=BATCH_LABEL, outlets=[REPLAYS_ARRIVED])
    def download_replays(match_ids: list[int]) -> str:
        from dota import ingest, opendota, storage, warehouse
        from dota.errors import PermanentError

        get_current_context()["batch_label"] = f"{match_ids[0]}..{match_ids[-1]}"
        downloaded, present, waiting, gave_up = 0, 0, 0, []
        conn = warehouse.connect()
        try:
            for match_id in match_ids:
                try:
                    ok, message = ingest.try_download(match_id, replay_url=opendota.replay_url,
                                                      exists=storage.exists, upload=storage.upload_replay)
                    permanent = False
                except PermanentError as e:
                    ok, message, permanent = False, str(e), True
                if ok:
                    ingest.mark_done(conn, match_id)
                    if message.endswith("already in Garage"):  # see ingest.try_download
                        present += 1
                    else:
                        downloaded += 1
                    print(f"{match_id}: {message}")
                elif ingest.mark_failed(conn, match_id, message, permanent=permanent):
                    gave_up.append(match_id)
                    print(f"{match_id}: GAVE UP {message}")
                else:
                    waiting += 1
                    print(f"{match_id}: not ready, next try in an hour: {message}")
        finally:
            conn.close()
        summary = (f"{downloaded} downloaded, {present} already in Garage, {waiting} not ready, "
                   f"{len(gave_up)} given up of {len(match_ids)}")
        print(summary)
        if gave_up:
            raise RuntimeError(f"{summary}; gave up on: {gave_up}")
        return summary

    download_replays.expand(match_ids=find_due())


download_pending()
