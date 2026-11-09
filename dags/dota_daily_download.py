"""
Daily at 00:00 SGT: download N_MATCHES ranked All Pick replays that ended around
the previous 00:00 SGT into Garage (s3://raw/replays/{match_id}.dem).

Valve numbers matches in the order they END (match_seq_num). Each run saves the
newest seq num as a bookmark; the next run lists matches from it, i.e. games that
ended ~24h ago: every game length is included and replays are ready.
The first run only saves the bookmark.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable, dag, task

from dota.alerts import ALERT_CALLBACKS  # standard library only, cheap at parse time

# dota.* (requests, boto3, ...) is imported inside tasks, not at parse time
N_MATCHES = 50
BOOKMARK = "dota_seq_bookmark"  # Airflow Variable


@dag(
    schedule="0 16 * * *",  # 16:00 UTC = 00:00 SGT
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=ALERT_CALLBACKS,
)
def dota_daily_download():
    @task
    def get_matches() -> list[int]:
        from dota import steam

        bookmark = Variable.get(BOOKMARK, default=None)
        ids = steam.ranked_all_pick_from(int(bookmark), N_MATCHES) if bookmark else []
        if bookmark and not ids:
            raise RuntimeError(f"no ranked matches after seq {bookmark}")
        Variable.set(BOOKMARK, str(steam.newest_seq()))
        return ids

    # retries cover OpenDota not having the replay salt yet
    @task(retries=5, retry_delay=timedelta(minutes=1))
    def download_replay(match_id: int) -> str:
        from dota import opendota, storage

        key = f"replays/{match_id}.dem"
        if not storage.exists(key):
            storage.upload_replay(opendota.replay_url(match_id), key)
        return key

    download_replay.expand(match_id=get_matches())


dota_daily_download()
