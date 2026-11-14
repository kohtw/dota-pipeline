"""Ingestion state in the warehouse `ingest` schema (warehouse/init/05_ingest_tables.sql): one Steam
bookmark per day, and the queue of matches whose replay could not be downloaded yet. Callers pass a
live DB connection (dota.warehouse.connect); every function commits its own change."""

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

SGT = timezone(timedelta(hours=8))  # a "day" is a Singapore calendar day, starting at 00:00 SGT
RETRY_EVERY = timedelta(hours=1)  # download_pending runs hourly
# Valve deletes replays after roughly 1-2 weeks, so a match not ready in 5 days likely never will be
GIVE_UP_AFTER = timedelta(days=5)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)  # the columns are TIMESTAMP, in UTC


def _execute(conn, sql: str, params: tuple, fetch: str | None = None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        result = cur.fetchone() if fetch == "one" else cur.fetchall() if fetch == "all" else None
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def record_bookmark(conn, day: date, seq: int, now: datetime | None = None) -> int:
    """Saves seq as day's bookmark unless the day already has one, and returns the day's bookmark.
    A rerun keeps the first value, so the day's window never moves."""
    row = _execute(conn, "SELECT match_seq_num FROM ingest.bookmarks WHERE day = %s", (day,), "one")
    if row:
        return row[0]
    _execute(conn, "INSERT INTO ingest.bookmarks (day, match_seq_num, recorded_at) VALUES (%s, %s, %s)",
             (day, seq, now or utcnow()))
    return seq


def seed_bookmark(conn, day: date, seq: int, now: datetime | None = None) -> bool:
    """Saves seq as day's bookmark only while ingest.bookmarks is empty: the one-off switch from the
    old Airflow Variable, so the first window starts where the old download stopped. Returns True
    when it saved."""
    if _execute(conn, "SELECT 1 FROM ingest.bookmarks LIMIT 1", (), "one"):
        return False
    record_bookmark(conn, day, seq, now)
    return True


def window(conn, day: date) -> tuple[int, int] | None:
    """(start, end) seq nums of day's download: the latest bookmark before day, and day's own.
    None until both exist. After a missed day the window simply covers both days."""
    rows = _execute(conn, "SELECT day, match_seq_num FROM ingest.bookmarks WHERE day <= %s "
                          "ORDER BY day DESC LIMIT 2", (day,), "all")
    if len(rows) < 2 or rows[0][0] != day:
        return None
    return rows[1][1], rows[0][1]


def enqueue(conn, match_id: int, day: date, error: str, now: datetime | None = None) -> None:
    """Queues a match whose download failed, for download_pending to retry. A match that is
    already queued is left as it is."""
    if _execute(conn, "SELECT 1 FROM ingest.download_queue WHERE match_id = %s", (match_id,), "one"):
        return
    now = now or utcnow()
    _execute(conn, "INSERT INTO ingest.download_queue (match_id, day, attempts, last_error, added_at, "
                   "next_try_at) VALUES (%s, %s, %s, %s, %s, %s)",
             (match_id, day, 1, error, now, now + RETRY_EVERY))


def due(conn, now: datetime | None = None, limit: int = 50) -> list[int]:
    """Queued match ids whose next try is due, oldest first."""
    rows = _execute(conn, "SELECT match_id FROM ingest.download_queue WHERE done_at IS NULL "
                          "AND gave_up_at IS NULL AND next_try_at <= %s ORDER BY next_try_at LIMIT %s",
                    (now or utcnow(), limit), "all")
    return [r[0] for r in rows]


def mark_done(conn, match_id: int, now: datetime | None = None) -> None:
    _execute(conn, "UPDATE ingest.download_queue SET done_at = %s WHERE match_id = %s",
             (now or utcnow(), match_id))


def mark_failed(conn, match_id: int, error: str, now: datetime | None = None) -> bool:
    """Records another failed try and schedules the next one, or gives up once the match has been
    queued for GIVE_UP_AFTER. Returns True when it gave up."""
    now = now or utcnow()
    added_at, attempts = _execute(conn, "SELECT added_at, attempts FROM ingest.download_queue "
                                        "WHERE match_id = %s", (match_id,), "one")
    gave_up = now - added_at >= GIVE_UP_AFTER
    _execute(conn, "UPDATE ingest.download_queue SET attempts = %s, last_error = %s, next_try_at = %s, "
                   "gave_up_at = %s WHERE match_id = %s",
             (attempts + 1, error, now + RETRY_EVERY, now if gave_up else None, match_id))
    return gave_up


def day_of(run_after: datetime, override: str | None = None) -> date:
    """The day a run works on: `override` (YYYY-MM-DD) when given, else run_after's SGT date."""
    return date.fromisoformat(override) if override else run_after.astimezone(SGT).date()


def try_download(match_id: int, *, replay_url: Callable[[int], str], exists: Callable[[str], bool],
                 upload: Callable[[str, str], None]) -> tuple[bool, str]:
    """Downloads one replay into Garage unless it is there already. Returns (True, what happened),
    or (False, the error) when a later try may succeed: OpenDota has no replay_url yet, a rate limit,
    a 5xx. A PermanentError is not a RuntimeError, so it propagates and fails the task."""
    key = f"replays/{match_id}.dem"
    if exists(key):
        return True, f"{key} already in Garage"
    try:
        upload(replay_url(match_id), key)
    except RuntimeError as e:
        return False, str(e)
    return True, f"downloaded {key}"
