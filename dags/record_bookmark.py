"""
Daily at 00:00 SGT: save Steam's newest match_seq_num as today's bookmark in the warehouse
(ingest.bookmarks), then emit BOOKMARKED, which starts dota_daily_download for the day.

A day keeps its first bookmark, so rerunning or triggering this DAG by hand never moves it. The
first run after the switch from the Airflow Variable dota_seq_bookmark copies that value in as
yesterday's bookmark, so the first window starts where the old download stopped.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable, dag, get_current_context, task

from dota.alerts import ALERT_CALLBACKS
from dota.assets import BOOKMARKED
from dota.config import API_RETRY, WAREHOUSE_INIT_DIR

LEGACY_BOOKMARK = "dota_seq_bookmark"  # Airflow Variable the download used before ingest.bookmarks


@dag(
    schedule="0 16 * * *",  # 16:00 UTC = 00:00 SGT
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=ALERT_CALLBACKS,
)
def record_bookmark():
    @task(pool="steam_api", outlets=[BOOKMARKED], **API_RETRY)
    def record() -> int:
        from dota import ingest, steam, warehouse

        day = ingest.day_of(get_current_context()["dag_run"].run_after)
        conn = warehouse.connect()
        try:
            warehouse.apply_init_sql(WAREHOUSE_INIT_DIR, conn)  # creates ingest.* on an older volume
            if legacy := Variable.get(LEGACY_BOOKMARK, default=None):
                if ingest.seed_bookmark(conn, day - timedelta(days=1), int(legacy)):
                    print(f"copied {LEGACY_BOOKMARK}={legacy} in as the bookmark for {day - timedelta(days=1)}")
            seq = ingest.record_bookmark(conn, day, steam.newest_seq())
        finally:
            conn.close()
        print(f"bookmark for {day}: match_seq_num {seq}")
        return seq

    record()


record_bookmark()
