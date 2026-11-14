"""
Download a sample of yesterday's ranked All Pick replays into Garage (s3://raw/replays/{match_id}.dem).

Daily at 01:00 SGT, once the last hourly steam_fetch of the day has been loaded. Picks N_MATCHES
ranked All Pick matches that ended yesterday (SGT) from stg.steam_matches, the matches steam_fetch
pooled, spread evenly through the day; a rerun of a day picks the same matches. It reads no Steam
API itself, so steam_fetch must be unpaused, or there is nothing to pick.

The matches are downloaded in batches of BATCH, one mapped task per batch, so a big matches_per_run
stays a handful of tasks; the task log lists what happened to each match. A replay that cannot be
downloaded yet (OpenDota has no replay_url, a rate limit) is queued in ingest.download_queue for
download_pending instead of failing the run.

To redo a past day, trigger with config {"day": "YYYY-MM-DD"}. Without one it works on yesterday.
"""

from datetime import datetime, timezone

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from dota.alerts import ALERT_CALLBACKS  # standard library only, cheap at parse time
from dota.config import WAREHOUSE_INIT_DIR

# dota.* (requests, boto3, ...) is imported inside tasks, not at parse time
N_MATCHES = 50  # default replays per run
MATCHES_VAR = "matches_per_run"  # Airflow Variable: overrides N_MATCHES (e.g. a small test run, or a bigger quota)
BATCH = 25  # matches per download task
BATCH_LABEL = "{{ batch_label }}"  # set by the task itself, so the UI lists mapped tasks by their match ids


@dag(
    schedule="0 17 * * *",  # 01:00 SGT
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={"day": Param(None, type=["null", "string"], format="date",
                         description="Day to download (YYYY-MM-DD); empty for yesterday")},
    default_args=ALERT_CALLBACKS,
)
def dota_daily_download():
    @task
    def get_matches() -> list[dict]:
        from dota import ingest, warehouse

        context = get_current_context()
        day = ingest.day_of(context["dag_run"].run_after, context["params"].get("day"))
        n = int(Variable.get(MATCHES_VAR, default=N_MATCHES))
        conn = warehouse.connect()
        try:
            warehouse.apply_init_sql(WAREHOUSE_INIT_DIR, conn)  # creates ingest.download_queue on an older volume
            ids = ingest.sample(conn, day, n)
        finally:
            conn.close()
        if not ids:
            raise AirflowSkipException(f"no ranked All Pick matches for {day} in stg.steam_matches; "
                                       "is steam_fetch unpaused?")
        print(f"{day}: picked {len(ids)} of {n}")
        return [{"match_ids": ids[i:i + BATCH], "day": day.isoformat()} for i in range(0, len(ids), BATCH)]

    # no task retries: a replay that is not ready goes to the queue, which download_pending retries
    @task(pool="opendota_api", retries=0, map_index_template=BATCH_LABEL)
    def download_replay(match_ids: list[int], day: str) -> str:
        from dota import ingest, opendota, storage, warehouse
        from dota.errors import PermanentError

        get_current_context()["batch_label"] = f"{match_ids[0]}..{match_ids[-1]}"
        downloaded, present, queued, broken = 0, 0, 0, []
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
                    if message.endswith("already in Garage"):  # see ingest.try_download
                        present += 1
                    else:
                        downloaded += 1
                    print(f"{match_id}: {message}")
                else:
                    ingest.enqueue(conn, match_id, datetime.fromisoformat(day).date(), message)
                    queued += 1
                    print(f"{match_id}: queued for download_pending: {message}")
        finally:
            conn.close()
        summary = (f"{downloaded} downloaded, {present} already in Garage, {queued} queued, "
                   f"{len(broken)} failed of {len(match_ids)}")
        print(summary)
        if broken:
            raise RuntimeError(f"{summary}; failed: {broken}")
        return summary

    download_replay.expand_kwargs(get_matches())


dota_daily_download()
