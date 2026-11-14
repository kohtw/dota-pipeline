import pytest
import requests

from dota import steam
from dota.errors import PermanentError


def pages(*page_list):
    """A fake steam.call returning the given result dicts in order."""
    it = iter(page_list)
    calls = []

    def fake(method, **params):
        calls.append(params)
        return next(it)

    return fake, calls


def match(seq, mode=22, lobby=7):
    return {"match_id": seq * 10, "match_seq_num": seq, "game_mode": mode, "lobby_type": lobby}












def test_fetch_pages_saves_contiguous_pages_and_sleeps_between_calls(monkeypatch):
    p1 = {"matches": [match(100), match(105)]}
    p2 = {"matches": [match(106), match(110)]}
    p3 = {"matches": [match(111)]}
    fake, calls = pages(p1, p2, p3)
    monkeypatch.setattr(steam, "call", fake)
    saved, slept = [], []
    n = steam.fetch_pages(100, 3, lambda s, r: saved.append((s, r)), sleep=slept.append, pace_s=0.5)
    assert n == 3
    assert saved == [(100, p1), (106, p2), (111, p3)]
    assert [c["start_at_match_seq_num"] for c in calls] == [100, 106, 111]
    assert all(c["matches_requested"] == 100 for c in calls)
    assert slept == [0.5, 0.5]


def test_fetch_pages_stops_on_an_empty_page_without_saving_it(monkeypatch):
    fake, calls = pages({"matches": [match(1)]}, {"matches": []}, {"matches": [match(5)]})
    monkeypatch.setattr(steam, "call", fake)
    saved, slept = [], []
    assert steam.fetch_pages(1, 5, lambda s, r: saved.append(s), sleep=slept.append) == 1
    assert saved == [1]
    assert len(calls) == 2
    assert slept == [2.0]


def test_http_error_message_does_not_contain_the_api_key(monkeypatch):
    class Resp:
        ok, status_code, reason = False, 403, "Forbidden"

    monkeypatch.setenv("STEAM_API_KEY", "SECRET123")
    monkeypatch.setattr(steam.requests, "get", lambda *a, **k: Resp())
    with pytest.raises(PermanentError) as err:
        steam.call("GetMatchHistory", matches_requested=1)
    assert "SECRET123" not in str(err.value)
    assert "403" in str(err.value)


class Resp:
    def __init__(self, status, result=None, headers=None):
        self.status_code, self.ok, self.reason = status, status < 400, "Reason"
        self.headers = headers or {}
        self._result = result

    def json(self):
        return {"result": self._result}


def responses(monkeypatch, *outcomes):
    """Fake requests.get returning (or raising) the outcomes in order; returns the call log."""
    it = iter(outcomes)
    calls = []

    def get(*a, **k):
        calls.append(k.get("params"))
        outcome = next(it)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setenv("STEAM_API_KEY", "SECRET123")
    monkeypatch.setattr(steam.requests, "get", get)
    return calls


def test_connection_error_message_does_not_contain_the_api_key(monkeypatch):
    url = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1/?key=SECRET123&x=1"
    responses(monkeypatch, *[requests.ConnectionError(f"Max retries exceeded with url: {url}")] * steam.STEAM_ATTEMPTS)
    with pytest.raises(RuntimeError) as err:
        steam.call("GetMatchHistory", sleep=lambda s: None, matches_requested=1)
    assert "SECRET123" not in str(err.value)
    assert err.value.__cause__ is None
    assert err.value.__suppress_context__


def test_429_then_ok_retries_and_returns_result(monkeypatch):
    calls = responses(monkeypatch, Resp(429), Resp(200, {"matches": []}))
    slept = []
    assert steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1) == {"matches": []}
    assert len(calls) == 2
    assert slept == [15]


def test_5xx_exhausts_attempts_then_raises_runtime_error(monkeypatch):
    calls = responses(monkeypatch, *[Resp(503)] * steam.STEAM_ATTEMPTS)
    slept = []
    with pytest.raises(RuntimeError) as err:
        steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert not isinstance(err.value, PermanentError)
    assert len(calls) == 5
    assert slept == [15, 30, 60, 120]


def test_403_raises_permanent_error_without_retry(monkeypatch):
    calls = responses(monkeypatch, Resp(403), Resp(200, {}))
    slept = []
    with pytest.raises(PermanentError):
        steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert len(calls) == 1
    assert slept == []


def test_retry_after_header_is_honoured_up_to_60s(monkeypatch):
    responses(monkeypatch, Resp(429, headers={"Retry-After": "45"}), Resp(429, headers={"Retry-After": "120"}),
              Resp(200, {}))
    slept = []
    steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert slept == [45, 30]


def test_retry_after_never_shortens_a_429_wait_below_the_backoff(monkeypatch):
    responses(monkeypatch, Resp(429, headers={"Retry-After": "5"}), Resp(200, {}))
    slept = []
    steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert slept == [15]


def test_spread_reads_evenly_spaced_pages_and_takes_one_match_from_each(monkeypatch):
    fake, calls = pages(*[{"matches": [match(s, mode=23), match(s + 1), match(s + 2)]} for s in (0, 250, 500, 750)])
    monkeypatch.setattr(steam, "call", fake)
    saved = []
    ids = steam.ranked_all_pick_spread(0, 1000, 4, lambda s, r: saved.append(s), sleep=lambda _: None)
    assert [c["start_at_match_seq_num"] for c in calls] == [0, 250, 500, 750]
    assert saved == [0, 250, 500, 750]
    assert ids == [10, 2510, 5010, 7510]


def test_spread_skips_matches_past_the_window_and_ones_already_taken(monkeypatch):
    fake, _ = pages({"matches": [match(1), match(2)]}, {"matches": [match(1), match(2)]},
                    {"matches": [match(9), match(10)]})
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_spread(0, 10, 3, sleep=lambda _: None) == [10, 20, 90]


def test_spread_sleeps_2s_between_pages_only(monkeypatch):
    fake, _ = pages(*[{"matches": []}] * 3)
    monkeypatch.setattr(steam, "call", fake)
    slept = []
    assert steam.ranked_all_pick_spread(0, 300, 3, sleep=slept.append) == []
    assert slept == [2.0, 2.0]


def test_between_reads_forward_and_stops_after_the_nth_match(monkeypatch):
    fake, calls = pages({"matches": [match(1), match(2, mode=23)]}, {"matches": [match(3), match(4), match(5)]})
    monkeypatch.setattr(steam, "call", fake)
    saved = []
    ids, next_seq = steam.ranked_all_pick_between(1, 100, 2, lambda s, r: saved.append(s), sleep=lambda _: None)
    assert (ids, next_seq) == ([10, 30], 4)  # the next call starts at match 4, which this one did not take
    assert [c["start_at_match_seq_num"] for c in calls] == saved == [1, 3]


def test_between_stops_at_the_window_end(monkeypatch):
    fake, calls = pages({"matches": [match(8), match(9), match(10)]})
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_between(8, 10, 50, sleep=lambda _: None) == ([80, 90], 10)
    assert len(calls) == 1


def test_between_reads_nothing_once_the_window_is_done(monkeypatch):
    fake, calls = pages()
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_between(10, 10, 50) == ([], 10)
    assert calls == []


def test_between_sleeps_2s_between_pages_only(monkeypatch):
    fake, _ = pages(*[{"matches": [match(i, mode=23)]} for i in range(1, 4)])
    monkeypatch.setattr(steam, "call", fake)
    slept = []
    steam.ranked_all_pick_between(1, 4, 50, sleep=slept.append)
    assert slept == [2.0, 2.0]
