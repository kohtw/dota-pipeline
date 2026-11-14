"""
Download one day's ranked All Pick replays into Garage (s3://raw/replays/{match_id}.dem).

Started by record_bookmark (the BOOKMARKED asset) at 00:00 SGT. A day's window runs from the previous
bookmark to the day's own in the warehouse (ingest.bookmarks), so it holds the matches that ended in
the last ~24h, and a rerun of a day gets the same window. Each run takes N_MATCHES matches from it,
chosen by the Airflow Variable download_mode:
  sample (default)  spread evenly across the window; a rerun picks the same matches
  window            the next ones after the day's saved cursor; each rerun continues, so enough
                    runs (or a bigger matches_per_run) download the whole window
The matches are downloaded in batches of BATCH, one mapped task per batch, so a big matches_per_run
stays a handful of tasks; the task log lists what happened to each match. A replay that cannot be
downloaded yet (OpenDota has no replay_url, a rate limit) is queued in ingest.download_queue for
download_pending instead of failing the run.

To redo or continue a past day, trigger with config {"day": "YYYY-MM-DD"}. Without one it works on today.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from dota.alerts import ALERT_CALLBACKS  # standard library only, cheap at parse time
from dota.assets import BOOKMARKED, STEAM_RAW
from dota.config import API_RETRY, WAREHOUSE_INIT_DIR

# dota.* (requests, boto3, ...) is imported inside tasks, not at parse time
N_MATCHES = 50  # default replays per run
MATCHES_VAR = "matches_per_run"  # Airflow Variable: overrides N_MATCHES (e.g. a small test run, or a bigger quota)
MODE_VAR = "download_mode"  # Airflow Variable: sample (default) or window, see above
BATCH = 25  # matches per download task
BATCH_LABEL = "{{ batch_label }}"  # set by the task itself, so the UI lists mapped tasks by their match ids


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
        n = int(Variable.get(MATCHES_VAR, default=N_MATCHES))
        mode = Variable.get(MODE_VAR, default="sample").strip().lower()

        def save(seq: int, page: dict) -> None:
            steam_summary.save_page(seq, page, storage.put)

        conn = warehouse.connect()
        try:
            warehouse.apply_init_sql(WAREHOUSE_INIT_DIR, conn)  # adds cursor_seq on an older volume
            picked = ingest.select(conn, datetime.fromisoformat(day).date(), n, mode,
                                   spread=lambda *a: steam.ranked_all_pick_spread(*a, save),
                                   between=lambda *a: steam.ranked_all_pick_between(*a, save))
        finally:
            conn.close()
        if picked is None:
            raise AirflowSkipException(f"no window for {day}: it needs that day's bookmark and an earlier one")
        ids, note = picked
        print(f"download_mode={mode}: {note}")
        return [{"match_ids": ids[i:i + BATCH], "day": day} for i in range(0, len(ids), BATCH)]

    # no task retries: a replay that is not ready goes to the queue, which download_pending retries
    @task(pool="opendota_api", retries=0, map_index_template=BATCH_LABEL)
    def download_replay(match_ids: list[int], day: str) -> str:
        from dota import ingest, opendota, storage, warehouse
        from dota.errors import PermanentError

        get_current_context()["batch_label"] = f"{match_ids[0]}..{match_ids[-1]}"
        downloaded, queued, broken = 0, 0, []
        conn = warehouse.connect()
        try:
            for match_id in match_ids:
                try:
                    ok, message = ingest.try_download(match_id, replay_url=opendota.replay_url,
                                                      exists=storage.exists, upload=storage.upload_replay)
                except PermanentError as e:  # e.g. not a Dota 2 replay: carry on with the batch, fail at the end
                    print(f"{match_id}: FAILED {e}")
                    broken.append(match_id)
                    continue
                if ok:
                    downloaded += 1
                    print(f"{match_id}: {message}")
                else:
                    ingest.enqueue(conn, match_id, datetime.fromisoformat(day).date(), message)
                    queued += 1
                    print(f"{match_id}: queued for download_pending: {message}")
        finally:
            conn.close()
        summary = f"{downloaded} downloaded, {queued} queued, {len(broken)} failed of {len(match_ids)}"
        print(summary)
        if broken:
            raise RuntimeError(f"{summary}; failed: {broken}")
        return summary

    download_replay.expand_kwargs(get_matches())


dota_daily_download()
