"""
Daily at 00:00 SGT: download N_MATCHES ranked All Pick replays that ended around
the previous 00:00 SGT into Garage (s3://raw/replays/{match_id}.dem).

Valve numbers matches in the order they END (match_seq_num). Each run saves the
newest seq num as a bookmark; the next run lists matches from it, i.e. games that
ended ~24h ago: every game length is included and replays are ready.
The first run only saves the bookmark.
"""

import gzip
import json
from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable, dag, task

from dota.alerts import ALERT_CALLBACKS  # standard library only, cheap at parse time
from dota.config import API_RETRY

# dota.* (requests, boto3, ...) is imported inside tasks, not at parse time
N_MATCHES = 50  # default replays per run
MATCHES_VAR = "matches_per_run"  # Airflow Variable: overrides N_MATCHES (e.g. a small test run, or a bigger quota)
BOOKMARK = "dota_seq_bookmark"  # Airflow Variable
PAGES_VAR = "steam_pages_per_run"  # Airflow Variable: ceiling on Steam pages per run; the loop stops once it has N_MATCHES ids (usually ~2 pages)


@dag(
    schedule="0 16 * * *",  # 16:00 UTC = 00:00 SGT
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=ALERT_CALLBACKS,
)
def dota_daily_download():
    # an hour fits 20 pages plus newest_seq even if every call times out twice before it succeeds
    @task(pool="steam_api", execution_timeout=timedelta(hours=1), **API_RETRY)
    def get_matches() -> list[int]:
        from dota import steam, storage

        def save_page(start_seq: int, page: dict) -> None:
            storage.put(f"steam_raw/seq_{start_seq}.json.gz", gzip.compress(json.dumps(page).encode()))

        bookmark = Variable.get(BOOKMARK, default=None)
        pages = int(Variable.get(PAGES_VAR, default=steam.MAX_PAGES))
        n_matches = int(Variable.get(MATCHES_VAR, default=N_MATCHES))
        ids = steam.ranked_all_pick_from(int(bookmark), n_matches, save_page, max_pages=pages) if bookmark else []
        if bookmark and not ids:
            raise RuntimeError(f"no ranked matches after seq {bookmark}")
        Variable.set(BOOKMARK, str(steam.newest_seq()))
        return ids

    # retries cover OpenDota not having the replay salt yet
    @task(pool="opendota_api", **API_RETRY)
    def download_replay(match_id: int) -> str:
        from dota import opendota, storage

        key = f"replays/{match_id}.dem"
        if not storage.exists(key):
            storage.upload_replay(opendota.replay_url(match_id), key)
        return key

    download_replay.expand(match_id=get_matches())


dota_daily_download()
