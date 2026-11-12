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


def test_save_page_called_once_per_page_with_start_seq_and_raw_result(monkeypatch):
    p1 = {"status": 1, "matches": [match(100), match(101, mode=23)]}
    p2 = {"status": 1, "matches": [match(102)]}
    p3 = {"status": 1, "matches": []}
    fake, _ = pages(p1, p2, p3)
    monkeypatch.setattr(steam, "call", fake)
    saved = []
    ids = steam.ranked_all_pick_from(100, 50, save_page=lambda s, r: saved.append((s, r)))
    assert ids == [1000, 1020]
    assert saved == [(100, p1), (102, p2), (103, p3)]


def test_without_save_page_result_is_unchanged(monkeypatch):
    fake, _ = pages({"matches": [match(1), match(2), match(3)]})
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_from(1, 2) == [10, 20]


def test_max_pages_is_overridable(monkeypatch):
    fake, calls = pages(*[{"matches": [match(i, mode=23)]} for i in range(1, 10)])
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_from(1, 50, max_pages=3) == []
    assert len(calls) == 3


def test_default_max_pages_is_the_module_constant(monkeypatch):
    fake, calls = pages(*[{"matches": [match(i, mode=23)]} for i in range(1, 40)])
    monkeypatch.setattr(steam, "call", fake)
    steam.ranked_all_pick_from(1, 50)
    assert len(calls) == steam.MAX_PAGES


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
    assert slept == [1.0]


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
    assert slept == [1]


def test_5xx_exhausts_attempts_then_raises_runtime_error(monkeypatch):
    calls = responses(monkeypatch, *[Resp(503)] * steam.STEAM_ATTEMPTS)
    slept = []
    with pytest.raises(RuntimeError) as err:
        steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert not isinstance(err.value, PermanentError)
    assert len(calls) == 4
    assert slept == [1, 2, 4]


def test_403_raises_permanent_error_without_retry(monkeypatch):
    calls = responses(monkeypatch, Resp(403), Resp(200, {}))
    slept = []
    with pytest.raises(PermanentError):
        steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert len(calls) == 1
    assert slept == []


def test_retry_after_header_is_honoured_up_to_60s(monkeypatch):
    responses(monkeypatch, Resp(429, headers={"Retry-After": "7"}), Resp(429, headers={"Retry-After": "120"}),
              Resp(200, {}))
    slept = []
    steam.call("GetMatchHistory", sleep=slept.append, matches_requested=1)
    assert slept == [7, 2]
