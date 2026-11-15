import pytest
import requests

from dota import datafeed
from dota.errors import PermanentError


class Resp:
    def __init__(self, status, body=None):
        self.status_code, self.ok = status, status < 400
        self._body = body

    def json(self):
        return self._body


def fake_get(monkeypatch, outcomes):
    """Each call pops the next response or exception; records (resource, params)."""
    calls = []

    def get(url, params, timeout):
        calls.append((url.rsplit("/", 1)[-1], params))
        result = outcomes.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(datafeed.requests, "get", get)
    return calls


def test_get_asks_for_english_and_retries_transient_failures(monkeypatch):
    calls = fake_get(monkeypatch, [requests.ConnectionError("reset"), Resp(503), Resp(200, {"ok": 1})])
    waits = []
    assert datafeed.get("itemlist", sleep=waits.append) == {"ok": 1}
    assert [c[1] for c in calls] == [{"language": "english"}] * 3
    assert waits == [10, 20]


def test_get_gives_up_after_the_last_attempt(monkeypatch):
    fake_get(monkeypatch, [Resp(503)] * datafeed.ATTEMPTS)
    with pytest.raises(RuntimeError, match="itemlist failed after 3 attempts: HTTP 503"):
        datafeed.get("itemlist", sleep=lambda s: None)


def test_get_4xx_is_permanent(monkeypatch):
    fake_get(monkeypatch, [Resp(404)])
    with pytest.raises(PermanentError, match="HTTP 404"):
        datafeed.get("herodata", hero_id=999, sleep=lambda s: None)


def test_fetch_gets_herodata_per_hero_keyed_by_id_with_a_pause(monkeypatch):
    herolist = {"result": {"data": {"heroes": [{"id": 1}, {"id": 76}]}}}
    calls = fake_get(monkeypatch, [Resp(200, herolist), Resp(200, {"h": 1}), Resp(200, {"h": 76}),
                                   Resp(200, {"items": 1}), Resp(200, {"patches": 1})])
    waits = []
    raw = datafeed.fetch(sleep=waits.append)
    assert raw == {"herolist": herolist, "herodata": {"1": {"h": 1}, "76": {"h": 76}},
                   "itemlist": {"items": 1}, "patchnoteslist": {"patches": 1}}
    assert [c[0] for c in calls] == ["herolist", "herodata", "herodata", "itemlist", "patchnoteslist"]
    assert calls[2][1] == {"language": "english", "hero_id": 76}
    assert waits == [datafeed.PAUSE_S] * 2
