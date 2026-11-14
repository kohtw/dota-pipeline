"""
Pick a sample of yesterday's ranked All Pick matches and queue their replays for download_pending.

Daily at 01:00 SGT, once the last hourly steam_fetch of the day has been loaded. Picks N_MATCHES
ranked All Pick matches that ended yesterday (SGT) from stg.steam_matches, the matches steam_fetch
pooled, spread evenly through the day; a rerun of a day picks the same matches. It reads no Steam
API itself, so steam_fetch must be unpaused, or there is nothing to pick.

The picks go into ingest.download_queue, due at once, and the DOWNLOADS_QUEUED asset wakes
download_pending, which does the downloading. A match already queued is left as it is.

To redo a past day, trigger with config {"day": "YYYY-MM-DD"}. Without one it works on yesterday.
"""

from datetime import datetime, timezone

from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from dota.alerts import ALERT_CALLBACKS  # standard library only, cheap at parse time
from dota.assets import DOWNLOADS_QUEUED
from dota.config import WAREHOUSE_INIT_DIR

# dota.* (psycopg2, ...) is imported inside tasks, not at parse time
N_MATCHES = 50  # default replays per day
MATCHES_VAR = "matches_per_run"  # Airflow Variable: overrides N_MATCHES (e.g. a small test run, or a bigger quota)


@dag(
    schedule="0 17 * * *",  # 01:00 SGT
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={"day": Param(None, type=["null", "string"], format="date",
                         description="Day to pick from (YYYY-MM-DD); empty for yesterday")},
    default_args=ALERT_CALLBACKS,
)
def dota_daily_download():
    @task(outlets=[DOWNLOADS_QUEUED])
    def queue_matches() -> int:
        from dota import ingest, warehouse

        context = get_current_context()
        day = ingest.day_of(context["dag_run"].run_after, context["params"].get("day"))
        n = int(Variable.get(MATCHES_VAR, default=N_MATCHES))
        conn = warehouse.connect()
        try:
            warehouse.apply_init_sql(WAREHOUSE_INIT_DIR, conn)  # creates ingest.download_queue on an older volume
            ids = ingest.sample(conn, day, n)
            added = sum(ingest.enqueue(conn, match_id, day) for match_id in ids)
        finally:
            conn.close()
        if not ids:
            raise AirflowSkipException(f"no ranked All Pick matches for {day} in stg.steam_matches; "
                                       "is steam_fetch unpaused?")
        print(f"{day}: picked {len(ids)} of {n}; queued {added}, {len(ids) - added} were already queued")
        return added

    queue_matches()


dota_daily_download()
