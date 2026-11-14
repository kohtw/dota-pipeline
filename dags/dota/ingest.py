"""Replay download state in the warehouse: which matches a day's download picks (from the Steam
matches steam_fetch pooled in stg.steam_matches), and the queue of matches to download
(ingest.download_queue, warehouse/init/05_ingest_tables.sql). Callers pass a live
DB connection (dota.warehouse.connect); every function commits its own change."""

from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone

SGT = timezone(timedelta(hours=8))  # a "day" is a Singapore calendar day, starting at 00:00 SGT
ALL_PICK = 22  # game_mode; Turbo is 23
RANKED = 7  # lobby_type
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


def sample(conn, day: date, n: int) -> list[int]:
    """Up to n ranked All Pick match ids that ended on day (SGT), from stg.steam_matches, evenly
    spaced through the day in match_seq_num order. steam_fetch pools a slice of matches every hour,
    so the sample covers the whole day; the same day gives the same matches on every rerun."""
    start = int(datetime.combine(day, time(), SGT).timestamp())
    rows = _execute(conn, "SELECT match_id FROM stg.steam_matches WHERE game_mode = %s AND lobby_type = %s "
                          "AND start_time + duration >= %s AND start_time + duration < %s "
                          "ORDER BY match_seq_num", (ALL_PICK, RANKED, start, start + 86400), "all")
    ids = [r[0] for r in rows]
    if len(ids) <= n:
        return ids
    return [ids[i * len(ids) // n] for i in range(n)]


def enqueue(conn, match_id: int, day: date, now: datetime | None = None) -> bool:
    """Queues a match for download_pending to download, due at once. A match that is already
    queued is left as it is. Returns True when it was added."""
    if _execute(conn, "SELECT 1 FROM ingest.download_queue WHERE match_id = %s", (match_id,), "one"):
        return False
    now = now or utcnow()
    _execute(conn, "INSERT INTO ingest.download_queue (match_id, day, attempts, added_at, next_try_at) "
                   "VALUES (%s, %s, %s, %s, %s)", (match_id, day, 0, now, now))
    return True


def due(conn, now: datetime | None = None, limit: int = 50) -> list[int]:
    """Queued match ids whose next try is due, longest waiting first."""
    rows = _execute(conn, "SELECT match_id FROM ingest.download_queue WHERE done_at IS NULL "
                          "AND gave_up_at IS NULL AND next_try_at <= %s ORDER BY next_try_at LIMIT %s",
                    (now or utcnow(), limit), "all")
    return [r[0] for r in rows]


def mark_done(conn, match_id: int, now: datetime | None = None) -> None:
    _execute(conn, "UPDATE ingest.download_queue SET done_at = %s, attempts = attempts + 1 WHERE match_id = %s",
             (now or utcnow(), match_id))


def mark_failed(conn, match_id: int, error: str, now: datetime | None = None, permanent: bool = False) -> bool:
    """Records a failed try and schedules the next one an hour later, or gives up: on a permanent
    error, or once the match has been queued for GIVE_UP_AFTER. Returns True when it gave up."""
    now = now or utcnow()
    added_at, attempts = _execute(conn, "SELECT added_at, attempts FROM ingest.download_queue "
                                        "WHERE match_id = %s", (match_id,), "one")
    gave_up = permanent or now - added_at >= GIVE_UP_AFTER
    _execute(conn, "UPDATE ingest.download_queue SET attempts = %s, last_error = %s, next_try_at = %s, "
                   "gave_up_at = %s WHERE match_id = %s",
             (attempts + 1, error, now + RETRY_EVERY, now if gave_up else None, match_id))
    return gave_up


def day_of(run_after: datetime, override: str | None = None) -> date:
    """The day a run downloads: `override` (YYYY-MM-DD) when given, else the SGT day before
    run_after, the last complete day."""
    return date.fromisoformat(override) if override else run_after.astimezone(SGT).date() - timedelta(days=1)


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
