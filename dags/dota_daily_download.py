"""
Download one day's sample of ranked All Pick replays into Garage (s3://raw/replays/{match_id}.dem).

Started by record_bookmark (the BOOKMARKED asset) at 00:00 SGT. A day's window runs from the previous
bookmark to the day's own in the warehouse (ingest.bookmarks), so it holds the matches that ended in
the last ~24h, and a rerun of a day gets the same window. N_MATCHES matches are sampled evenly across
the window. A replay that cannot be downloaded yet (OpenDota has no replay_url, a rate limit) is
queued in ingest.download_queue for download_pending instead of failing the run; its task shows as
skipped.

To redo a past day, trigger with config {"day": "YYYY-MM-DD"}. Triggering without one works on today.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from dota.alerts import ALERT_CALLBACKS  # standard library only, cheap at parse time
from dota.assets import BOOKMARKED, STEAM_RAW
from dota.config import API_RETRY

# dota.* (requests, boto3, ...) is imported inside tasks, not at parse time
N_MATCHES = 50  # default replays per day
MATCHES_VAR = "matches_per_run"  # Airflow Variable: overrides N_MATCHES (e.g. a small test run, or a bigger quota)
MATCH_LABEL = "{{ match_label }}"  # set by the task itself, so the UI lists mapped tasks by match id


def _day() -> str:
    from dota import ingest

    context = get_current_context()
    return ingest.day_of(context["dag_run"].run_after, context["params"].get("day")).isoformat()


@dag(
    schedule=BOOKMARKED,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={"day": Param(None, type=["null", "string"], format="date",
                         description="Day to download (YYYY-MM-DD); empty for today")},
    default_args=ALERT_CALLBACKS,
)
def dota_daily_download():
    # an hour fits 50 paced pages even if every call times out twice before it succeeds
    @task(pool="steam_api", execution_timeout=timedelta(hours=1), outlets=[STEAM_RAW], **API_RETRY)
    def get_matches() -> list[dict]:
        from dota import ingest, steam, steam_summary, storage, warehouse

        day = _day()
        conn = warehouse.connect()
        try:
            window = ingest.window(conn, datetime.fromisoformat(day).date())
        finally:
            conn.close()
        if window is None:
            raise AirflowSkipException(f"no window for {day}: it needs that day's bookmark and an earlier one")
        n = int(Variable.get(MATCHES_VAR, default=N_MATCHES))
        ids = steam.ranked_all_pick_spread(*window, n, lambda seq, page: steam_summary.save_page(seq, page, storage.put))
        print(f"{day}: window seq {window[0]}..{window[1]}, sampled {len(ids)} of {n} matches")
        return [{"match_id": i, "day": day} for i in ids]

    # no task retries: a replay that is not ready goes to the queue, which download_pending retries
    @task(pool="opendota_api", retries=0, map_index_template=MATCH_LABEL)
    def download_replay(match_id: int, day: str) -> str:
        from dota import ingest, opendota, storage, warehouse

        get_current_context()["match_label"] = str(match_id)
        ok, message = ingest.try_download(match_id, replay_url=opendota.replay_url, exists=storage.exists,
                                          upload=storage.upload_replay)
        if ok:
            print(message)
            return message
        conn = warehouse.connect()
        try:
            ingest.enqueue(conn, match_id, datetime.fromisoformat(day).date(), message)
        finally:
            conn.close()
        raise AirflowSkipException(f"queued for download_pending: {message}")

    download_replay.expand_kwargs(get_matches())


dota_daily_download()
