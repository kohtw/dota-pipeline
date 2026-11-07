import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import replay_downloader as rd  # noqa: E402

ZSTD = b"\x28\xb5\x2f\xfd" + b"\x00" * 60


class FakeResponse:
    def __init__(self, status: int = 200, json_data=None, body: bytes = b"", headers: dict | None = None):
        self.status_code, self._json, self.content = status, json_data, body
        self.headers = headers or {}
        self.ok = status < 400

    def json(self):
        return self._json

    def raise_for_status(self) -> None:
        if not self.ok:
            raise requests.HTTPError(f"{self.status_code} Error for url: https://x/?key=SECRETKEY")

    def iter_content(self, chunk_size: int):
        for i in range(0, len(self.content), 16):
            yield self.content[i:i + 16]

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass


class FakeSession:
    """Returns queued responses per (method, url-prefix) and records every call."""

    def __init__(self, routes: dict[tuple[str, str], list[FakeResponse]]):
        self.routes, self.calls = routes, []

    def request(self, method: str, url: str, **kwargs) -> FakeResponse:
        self.calls.append((method, url))
        for (m, prefix), queue in self.routes.items():
            if m == method and url.startswith(prefix):
                return queue.pop(0) if len(queue) > 1 else queue[0]
        raise AssertionError(f"unexpected {method} {url}")

    def get(self, url: str, **kwargs) -> FakeResponse:
        return self.request("GET", url, **kwargs)


def no_pace() -> rd.Paced:
    return rd.Paced(0, sleep=lambda s: None)


def match(**overrides) -> dict:
    m = {"match_id": 1, "match_seq_num": 10, "start_time": 0, "lobby_type": 7, "game_mode": 22,
         "duration": 1800, "radiant_win": True, "players": [{"leaver_status": 0}] * 10}
    return {**m, **overrides}


def test_completed_ranked_all_pick_filter():
    wanted = rd.is_completed_ranked_all_pick
    assert wanted(match())
    assert not wanted(match(lobby_type=0))
    assert not wanted(match(game_mode=23))
    assert not wanted(match(duration=0))
    assert not wanted(match(duration=rd.MIN_DURATION_S - 1))
    assert not wanted(match(players=[{"leaver_status": 0}] * 9))
    m = match()
    del m["radiant_win"]
    assert not wanted(m)


def test_leavers_rule_out_a_match_but_reconnects_do_not():
    wanted = rd.is_completed_ranked_all_pick
    assert wanted(match(players=[{"leaver_status": 1}] + [{"leaver_status": 0}] * 9))
    assert not wanted(match(players=[{"leaver_status": 3}] + [{"leaver_status": 0}] * 9))


def test_replay_suffix_from_magic_bytes():
    assert rd.replay_suffix(ZSTD) == ".dem.zst"
    assert rd.replay_suffix(b"BZh91AY") == ".dem.bz2"
    assert rd.replay_suffix(b"PBDEMS2\x00") == ".dem"
    with pytest.raises(ValueError):
        rd.replay_suffix(b"<html>")


def test_opendota_stops_at_daily_reserve():
    session = FakeSession({("GET", rd.OPENDOTA_API): [
        FakeResponse(json_data={"replay_url": "u"}, headers={"x-rate-limit-remaining-day": "20"})]})
    od = rd.OpenDota(session, None, reserve=20, pace=no_pace())
    assert od.replay_url(1) == "u"
    with pytest.raises(rd.BudgetExhausted):
        od.replay_url(2)
    assert len(session.calls) == 1


def test_opendota_429_with_no_calls_left_stops():
    session = FakeSession({("GET", rd.OPENDOTA_API): [
        FakeResponse(status=429, headers={"x-rate-limit-remaining-day": "0"})]})
    od = rd.OpenDota(session, None, reserve=0, pace=no_pace(), sleep=lambda s: None)
    with pytest.raises(rd.BudgetExhausted):
        od.replay_url(1)


def test_steam_errors_hide_the_key():
    session = FakeSession({("GET", rd.STEAM_API): [FakeResponse(status=403)]})
    steam = rd.Steam(session, "SECRETKEY", pace=no_pace())
    with pytest.raises(RuntimeError) as err:
        steam.newest_seq()
    assert "SECRETKEY" not in str(err.value)


def test_seq_for_age_uses_observed_rate():
    newest = 1_000_000_000
    session = FakeSession({
        ("GET", f"{rd.STEAM_API}/GetMatchHistory/"): [
            FakeResponse(json_data={"result": {"matches": [{"match_seq_num": newest}]}})],
        ("GET", f"{rd.STEAM_API}/GetMatchHistoryBySequenceNum/"): [
            FakeResponse(json_data={"result": {"matches": [
                {"match_seq_num": newest - 1_000_000, "start_time": 10_000 - 3600}]}})],
    })
    steam = rd.Steam(session, "k", pace=no_pace())
    # 1,000,000 seq numbers per hour, so 24 hours back is 24,000,000
    assert steam.seq_for_age(24, now=10_000) == newest - 24_000_000


@pytest.fixture
def state(tmp_path: Path) -> rd.State:
    s = rd.State(tmp_path / "state.db")
    s.add_discovered([match(match_id=1), match(match_id=2)])
    return s


def test_downloads_when_replay_url_known(state: rd.State, tmp_path: Path):
    session = FakeSession({
        ("GET", rd.OPENDOTA_API): [FakeResponse(json_data={"replay_url": "http://replay1.valve.net/570/1_9.dem.bz2"})],
        ("GET", "http://replay1.valve.net"): [FakeResponse(body=ZSTD)],
    })
    od = rd.OpenDota(session, None, pace=no_pace())
    size = rd.resolve_and_download(state, od, session, tmp_path / "staging", (1, "discovered", None, 0))
    assert size == len(ZSTD)
    assert (tmp_path / "staging" / "replays" / "1.dem.zst").read_bytes() == ZSTD
    assert state.counts()["downloaded"] == 1


def test_missing_salt_requests_fetch_then_gives_up(state: rd.State, tmp_path: Path):
    session = FakeSession({
        ("GET", rd.OPENDOTA_API): [FakeResponse(json_data={"replay_url": None})],
        ("POST", rd.OPENDOTA_API): [FakeResponse()],
    })
    od = rd.OpenDota(session, None, pace=no_pace())
    rd.resolve_and_download(state, od, session, tmp_path, (1, "discovered", None, 0))
    assert ("POST", f"{rd.OPENDOTA_API}/request/1") in session.calls
    assert state.counts()["requested"] == 1
    rd.resolve_and_download(state, od, session, tmp_path, (1, "requested", None, rd.MAX_ATTEMPTS - 1))
    assert state.counts()["no_replay"] == 1


def test_expired_replay_is_marked_gone(state: rd.State, tmp_path: Path):
    session = FakeSession({("GET", "http://replay1.valve.net"): [FakeResponse(status=404)]})
    od = rd.OpenDota(session, None, pace=no_pace())
    rd.resolve_and_download(state, od, session, tmp_path, (1, "ready", "http://replay1.valve.net/570/1_9.dem.bz2", 0))
    assert state.counts()["gone"] == 1


def test_opendota_error_schedules_a_retry(state: rd.State, tmp_path: Path):
    session = FakeSession({("GET", rd.OPENDOTA_API): [FakeResponse(status=500)]})
    od = rd.OpenDota(session, None, pace=no_pace())
    rd.resolve_and_download(state, od, session, tmp_path, (1, "discovered", None, 0))
    assert state.counts()["requested"] == 1
    assert state.due(now=0, limit=10) == [(2, "discovered", None, 0)]


def test_due_order_and_mark_uploaded(state: rd.State, tmp_path: Path):
    state.update(2, status="ready", replay_url="u")
    assert [row[0] for row in state.due(now=0, limit=10)] == [2, 1]
    gone_file = tmp_path / "uploaded.dem.zst"
    state.update(1, status="downloaded", file=str(gone_file))
    assert state.mark_uploaded() == 1
    assert state.counts()["uploaded"] == 1


def test_dotenv_does_not_override_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nSTEAM_API_KEY='from-file'\nDRIVE_DEST=x:y\n")
    monkeypatch.setenv("STEAM_API_KEY", "from-env")
    monkeypatch.delenv("DRIVE_DEST", raising=False)
    rd.load_dotenv(env)
    import os
    assert os.environ["STEAM_API_KEY"] == "from-env"
    assert os.environ["DRIVE_DEST"] == "x:y"
