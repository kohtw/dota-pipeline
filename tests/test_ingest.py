from datetime import date, datetime, timedelta

import pytest

from dota import ingest

DAY = date(2026, 11, 14)
NOW = datetime(2026, 11, 14, 16, 0)


class FakeConn:
    """Answers each SELECT with the next scripted result and records every statement."""

    def __init__(self, *results):
        self.results = list(results)
        self.executed: list[tuple[str, tuple]] = []
        self.commits = 0

    def cursor(self):
        return self

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.results.pop(0)

    def fetchall(self):
        return self.results.pop(0)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass

    def writes(self):
        return [(sql.split()[0], params) for sql, params in self.executed if not sql.startswith("SELECT")]


def test_record_bookmark_saves_a_new_day():
    conn = FakeConn(None)
    assert ingest.record_bookmark(conn, DAY, 100, NOW) == 100
    assert conn.writes() == [("INSERT", (DAY, 100, NOW))]


def test_record_bookmark_keeps_the_first_value_on_a_rerun():
    conn = FakeConn((100,))
    assert ingest.record_bookmark(conn, DAY, 999, NOW) == 100
    assert conn.writes() == []


def test_seed_bookmark_saves_only_into_an_empty_table():
    conn = FakeConn(None, None)  # table empty, then no bookmark for the day
    assert ingest.seed_bookmark(conn, DAY, 100, NOW) is True
    assert conn.writes() == [("INSERT", (DAY, 100, NOW))]


def test_seed_bookmark_does_nothing_once_any_bookmark_exists():
    conn = FakeConn((1,))
    assert ingest.seed_bookmark(conn, DAY, 100, NOW) is False
    assert conn.writes() == []


def test_window_runs_from_the_previous_bookmark_to_the_days_own():
    conn = FakeConn([(DAY, 300), (DAY - timedelta(days=2), 100)])  # a missed day widens the window
    assert ingest.window(conn, DAY) == (100, 300)


def test_window_is_none_until_both_bookmarks_exist():
    assert ingest.window(FakeConn([(DAY, 300)]), DAY) is None
    assert ingest.window(FakeConn([(DAY - timedelta(days=1), 200), (DAY - timedelta(days=2), 100)]), DAY) is None


def test_enqueue_adds_a_match_due_in_an_hour():
    conn = FakeConn(None)
    ingest.enqueue(conn, 7, DAY, "no replay_url", NOW)
    assert conn.writes() == [("INSERT", (7, DAY, 1, "no replay_url", NOW, NOW + ingest.RETRY_EVERY))]


def test_enqueue_leaves_an_already_queued_match_alone():
    conn = FakeConn((1,))
    ingest.enqueue(conn, 7, DAY, "no replay_url", NOW)
    assert conn.writes() == []


def test_due_returns_match_ids():
    assert ingest.due(FakeConn([(7,), (8,)]), NOW) == [7, 8]


def test_mark_failed_schedules_the_next_try():
    conn = FakeConn((NOW - timedelta(days=1), 3))
    assert ingest.mark_failed(conn, 7, "HTTP 429", NOW) is False
    assert conn.writes() == [("UPDATE", (4, "HTTP 429", NOW + ingest.RETRY_EVERY, None, 7))]


def test_mark_failed_gives_up_after_five_days():
    conn = FakeConn((NOW - ingest.GIVE_UP_AFTER, 40))
    assert ingest.mark_failed(conn, 7, "no replay_url", NOW) is True
    assert conn.writes()[0][1][3] == NOW  # gave_up_at


def test_mark_done_sets_done_at():
    conn = FakeConn()
    ingest.mark_done(conn, 7, NOW)
    assert conn.writes() == [("UPDATE", (NOW, 7))]


def test_day_of_uses_the_sgt_date_or_the_override():
    late_utc = datetime(2026, 11, 14, 16, 5, tzinfo=ingest.timezone.utc)  # 00:05 SGT on the 15th
    assert ingest.day_of(late_utc) == date(2026, 11, 15)
    assert ingest.day_of(late_utc, "2026-11-10") == date(2026, 11, 10)


def download(present=(), url_error=None):
    uploaded = []

    def replay_url(match_id):
        if url_error:
            raise url_error
        return f"http://replay/{match_id}"

    result = ingest.try_download(7, replay_url=replay_url, exists=lambda k: k in present,
                                 upload=lambda url, key: uploaded.append((url, key)))
    return result, uploaded


def test_try_download_uploads_a_new_replay():
    assert download() == ((True, "downloaded replays/7.dem"), [("http://replay/7", "replays/7.dem")])


def test_try_download_skips_a_replay_already_in_garage():
    assert download(present={"replays/7.dem"}) == ((True, "replays/7.dem already in Garage"), [])


def test_try_download_returns_a_retryable_error_instead_of_raising():
    assert download(url_error=RuntimeError("no replay_url for 7 yet")) == ((False, "no replay_url for 7 yet"), [])


def test_try_download_lets_a_permanent_error_fail_the_task():
    from dota.errors import PermanentError

    with pytest.raises(PermanentError):
        download(url_error=PermanentError("HTTP 400"))
