import pytest
import requests

from dota import opendota
from dota.errors import PermanentError


class Resp:
    def __init__(self, status, body=None):
        self.status_code, self.ok = status, status < 400
        self._body = body or {}

    def json(self):
        return self._body


@pytest.fixture(autouse=True)
def slept(monkeypatch):
    """Records the pacing waits instead of sleeping."""
    waits = []
    monkeypatch.setattr(opendota.time, "sleep", waits.append)
    return waits


@pytest.fixture
def fake_http(monkeypatch):
    posted = []

    def use(outcome):
        def get(*a, **k):
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(opendota.requests, "get", get)
        monkeypatch.setattr(opendota.requests, "post", lambda url, **k: posted.append(url))
        return posted

    return use


def test_404_raises_not_ready_and_requests_a_fetch(fake_http):
    posted = fake_http(Resp(404))
    with pytest.raises(RuntimeError, match="no replay_url for 1 yet"):
        opendota.replay_url(1)
    assert posted == [f"{opendota.API}/request/1"]


def test_missing_replay_url_raises_not_ready(fake_http):
    fake_http(Resp(200, {"match_id": 1}))
    with pytest.raises(RuntimeError, match="no replay_url for 1 yet"):
        opendota.replay_url(1)


def test_429_raises_retryable_runtime_error(fake_http):
    fake_http(Resp(429))
    with pytest.raises(RuntimeError) as err:
        opendota.replay_url(1)
    assert not isinstance(err.value, PermanentError)
    assert "429" in str(err.value)


def test_400_raises_permanent_error(fake_http):
    fake_http(Resp(400))
    with pytest.raises(PermanentError):
        opendota.replay_url(1)


def test_connection_error_is_not_chained(fake_http):
    fake_http(requests.ConnectionError("Max retries exceeded with url: /api/matches/1?api_key=SECRET123"))
    with pytest.raises(RuntimeError) as err:
        opendota.replay_url(1)
    assert "SECRET123" not in str(err.value)
    assert err.value.__cause__ is None
    assert err.value.__suppress_context__


def test_waits_after_every_call_to_stay_at_50_a_minute(fake_http, slept):
    fake_http(Resp(404))
    with pytest.raises(RuntimeError):
        opendota.replay_url(1)
    assert slept == [60 / 50] * 2  # the match lookup and the fetch request
