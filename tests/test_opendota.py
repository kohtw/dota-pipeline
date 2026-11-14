import pytest
import requests

from dota import opendota
from dota.errors import PermanentError


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self.ok = status, status < 400
        self._body = body or {}
        self.headers = headers or {}

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

    def use(outcome, fetch=None):
        """outcome: the GET response or exception, or a list of them in order. fetch: the POST response."""
        outcomes = outcome if isinstance(outcome, list) else None

        def get(*a, **k):
            result = outcomes.pop(0) if outcomes is not None else outcome
            if isinstance(result, Exception):
                raise result
            return result

        def post(url, **k):
            posted.append(url)
            return fetch or Resp(200)

        monkeypatch.setattr(opendota.requests, "get", get)
        monkeypatch.setattr(opendota.requests, "post", post)
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


def test_429_waits_5_minutes_then_tries_again(fake_http, slept):
    fake_http([Resp(429), Resp(200, {"replay_url": "http://replay1.valve.net/570/1_2.dem.bz2"})])
    assert opendota.replay_url(1) == "http://replay1.valve.net/570/1_2.dem.bz2"
    assert slept == [opendota.PACE_S, 300, opendota.PACE_S]


def test_429_honours_retry_after(fake_http, slept):
    fake_http([Resp(429, headers={"Retry-After": "42"}), Resp(200, {"replay_url": "u"})])
    opendota.replay_url(1)
    assert 42 in slept


def test_429_gives_up_after_three_tries(fake_http, slept):
    fake_http([Resp(429)] * 3)
    with pytest.raises(RuntimeError, match="HTTP 429"):
        opendota.replay_url(1)
    assert slept.count(300) == 2


def test_rate_limited_fetch_request_is_reported_as_429_not_as_requested(fake_http):
    fake_http(Resp(404), fetch=Resp(429))
    with pytest.raises(RuntimeError, match="fetch request for 1 failed: HTTP 429") as err:
        opendota.replay_url(1)
    assert not isinstance(err.value, PermanentError)
