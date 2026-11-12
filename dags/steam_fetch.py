"""
Hourly Steam fetch: save the newest Steam match pages to Garage (steam_raw/) and emit STEAM_RAW,
which wakes steam_summary_elt to load them.

The Variable steam_fetch_pages_per_run sets the pages of 100 matches per run (default 10). Each run
starts that many pages behind the newest seq num, so samples spread across hours instead of walking
a bookmark that falls further behind. The DAG starts paused.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable, dag, task

from dota.alerts import ALERT_CALLBACKS
from dota.assets import STEAM_RAW
from dota.config import API_RETRY

PAGES = 10
PAGES_VAR = "steam_fetch_pages_per_run"


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=ALERT_CALLBACKS,
    tags=["orchestration"],
)
def steam_fetch():
    # 40 pages take about 2 min; 30 min allows many retried calls and still ends before the next hourly run
    @task(pool="steam_api", execution_timeout=timedelta(minutes=30), outlets=[STEAM_RAW], **API_RETRY)
    def fetch() -> int:
        from dota import steam, steam_summary, storage

        pages = int(Variable.get(PAGES_VAR, default=PAGES))
        start = steam.newest_seq() - pages * 100
        saved = steam.fetch_pages(start, pages, lambda seq, page: steam_summary.save_page(seq, page, storage.put))
        print(f"saved {saved} of {pages} Steam pages from seq {start}")
        return saved

    fetch()


steam_fetch()
