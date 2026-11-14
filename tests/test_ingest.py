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


def test_day_of_is_the_sgt_day_before_or_the_override():
    late_utc = datetime(2026, 11, 14, 17, 0, tzinfo=ingest.timezone.utc)  # 01:00 SGT on the 15th
    assert ingest.day_of(late_utc) == date(2026, 11, 14)
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


def test_sample_spreads_n_evenly_through_the_days_matches():
    conn = FakeConn([(i,) for i in range(10)])
    assert ingest.sample(conn, DAY, 3) == [0, 3, 6]
    sql, params = conn.executed[0]
    start = int(datetime(2026, 11, 13, 16, 0, tzinfo=ingest.timezone.utc).timestamp())  # 00:00 SGT on the 14th
    assert params == (ingest.ALL_PICK, ingest.RANKED, start, start + 86400)
    assert "start_time + duration" in sql and "ORDER BY match_seq_num" in sql


def test_sample_returns_all_when_the_day_has_n_or_fewer():
    assert ingest.sample(FakeConn([(1,), (2,)]), DAY, 5) == [1, 2]
    assert ingest.sample(FakeConn([]), DAY, 5) == []
