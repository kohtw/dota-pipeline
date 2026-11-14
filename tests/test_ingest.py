from datetime import date, datetime, timedelta

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
