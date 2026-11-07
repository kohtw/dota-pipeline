import gzip
import os
import sys
import time
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import downloader as rd  # noqa: E402

ZSTD = b"\x28\xb5\x2f\xfd" + b"\x00" * 60
OLD = 0  # start_time far in the past, so matches are old enough for the replay route


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
        self.calls.append((method, url, kwargs.get("params")))
        for (m, prefix), queue in self.routes.items():
            if m == method and url.startswith(prefix):
                return queue.pop(0) if len(queue) > 1 else queue[0]
        raise AssertionError(f"unexpected {method} {url}")

    def get(self, url: str, **kwargs) -> FakeResponse:
        return self.request("GET", url, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        pass


def no_pace() -> rd.Paced:
    return rd.Paced(0, sleep=lambda s: None)


def match(**overrides) -> dict:
    m = {"match_id": 1, "match_seq_num": 10, "start_time": OLD, "lobby_type": 7, "game_mode": 22,
         "duration": 1800, "radiant_win": True, "players": [{"leaver_status": 0}] * 10}
    return {**m, **overrides}


def steam_page(*matches: dict) -> FakeResponse:
    return FakeResponse(json_data={"result": {"matches": list(matches)}}, body=b'{"result": "raw page"}')


@pytest.fixture
def state(tmp_path: Path) -> rd.State:
    s = rd.State(tmp_path / "state.db")
    s.add_discovered([match(match_id=1), match(match_id=2)])
    return s


# Filters

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


# Steam route

def test_steam_errors_hide_the_key():
    session = FakeSession({("GET", rd.STEAM_API): [FakeResponse(status=403)]})
    steam = rd.Steam(session, "SECRETKEY", pace=no_pace())
    with pytest.raises(RuntimeError) as err:
        steam.newest_seq()
    assert "SECRETKEY" not in str(err.value)


def test_steam_stops_at_daily_cap(state: rd.State):
    session = FakeSession({("GET", rd.STEAM_API): [steam_page(match())]})
    steam = rd.Steam(session, "k", state, pace=no_pace(), daily_cap=2)
    steam.page(1)
    steam.page(2)
    with pytest.raises(rd.BudgetExhausted):
        steam.page(3)
    assert len(session.calls) == 2
    assert state.calls_today("steam") == 2


def test_steam_429_backs_off_slows_down_and_retries():
    session = FakeSession({("GET", rd.STEAM_API): [FakeResponse(status=429), FakeResponse(status=503),
                                                     steam_page(match())]})
    slept: list[float] = []
    steam = rd.Steam(session, "k", pace=rd.Paced(1.0, sleep=lambda s: None), sleep=slept.append)
    _, matches = steam.page(1)
    assert len(matches) == 1
    assert slept == [rd.STEAM_BACKOFF_S, rd.STEAM_BACKOFF_S * 2]
    assert steam.pace.min_interval == 4.0  # doubled twice
    assert steam.throttled == 2


def test_steam_gives_up_the_run_when_throttling_persists():
    session = FakeSession({("GET", rd.STEAM_API): [FakeResponse(status=429)]})
    steam = rd.Steam(session, "k", pace=no_pace(), sleep=lambda s: None)
    with pytest.raises(rd.BudgetExhausted):
        steam.page(1)
    assert len(session.calls) == rd.STEAM_MAX_RETRIES + 1
    assert steam.pace.min_interval <= rd.STEAM_MAX_INTERVAL_S


def test_steam_speeds_up_after_a_run_of_successes():
    session = FakeSession({("GET", rd.STEAM_API): [steam_page(match())]})
    steam = rd.Steam(session, "k", pace=rd.Paced(4.0, sleep=lambda s: None), sleep=lambda s: None)
    for _ in range(rd.STEAM_SPEEDUP_AFTER):
        steam.page(1)
    assert steam.pace.min_interval == pytest.approx(3.0)


def test_seq_for_age_uses_observed_rate_and_returns_probe_page():
    newest = 1_000_000_000
    session = FakeSession({
        ("GET", f"{rd.STEAM_API}/GetMatchHistory/"): [
            FakeResponse(json_data={"result": {"matches": [{"match_seq_num": newest}]}})],
        ("GET", f"{rd.STEAM_API}/GetMatchHistoryBySequenceNum/"): [
            steam_page({"match_seq_num": newest - 1_000_000, "start_time": 10_000 - 3600})],
    })
    steam = rd.Steam(session, "k", pace=no_pace())
    # 1,000,000 seq numbers per hour, so 24 hours back is 24,000,000
    start, probe, raw = steam.seq_for_age(24, now=10_000)
    assert start == newest - 24_000_000
    assert steam.seq_for_age(24 * 365, now=10_000)[0] == 0  # never below zero
    assert probe == newest - 1_000_000
    assert raw == b'{"result": "raw page"}'


def test_take_page_queues_completed_and_holds_in_progress(tmp_path: Path):
    state = rd.State(tmp_path / "state.db")
    page = [match(match_id=1), match(match_id=2, duration=0), match(match_id=3, duration=300),
            match(match_id=4, lobby_type=0)]
    queued, waiting = rd.take_page(state, page, now=1000)
    assert (queued, waiting) == (1, 1)
    assert state.counts("matches") == {"discovered": 1}
    assert state.due_callbacks(now=1000, limit=10) == []  # not due for an hour
    assert state.due_callbacks(now=1000 + rd.CALLBACK_AFTER_S, limit=10) == [(2, 10, 0)]


def test_callback_queues_a_match_once_it_finishes(tmp_path: Path):
    state = rd.State(tmp_path / "state.db")
    state.add_callbacks([match(match_id=7, match_seq_num=70, duration=0)], now=0)
    session = FakeSession({("GET", rd.STEAM_API): [steam_page(match(match_id=7, match_seq_num=70))]})
    steam = rd.Steam(session, "k", state, pace=no_pace())
    staging = tmp_path / "steam"
    assert rd.run_callbacks(state, steam, staging, now=rd.CALLBACK_AFTER_S) == 1
    assert session.calls[0][2]["start_at_match_seq_num"] == 70
    assert session.calls[0][2]["matches_requested"] == 1
    assert state.counts("callbacks") == {"done": 1}
    assert state.counts("matches") == {"discovered": 1}
    assert gzip.decompress((staging / "callback_7.json.gz").read_bytes()) == b'{"result": "raw page"}'


def test_callback_gives_up_after_max_attempts(tmp_path: Path):
    state = rd.State(tmp_path / "state.db")
    state.add_callbacks([match(match_id=7, duration=0)], now=0)
    session = FakeSession({("GET", rd.STEAM_API): [steam_page(match(match_id=7, duration=0))]})
    steam = rd.Steam(session, "k", state, pace=no_pace())
    now = 0.0
    for _ in range(rd.CALLBACK_MAX):
        now += rd.CALLBACK_AFTER_S
        rd.run_callbacks(state, steam, tmp_path, now)
    assert state.counts("callbacks") == {"gave_up": 1}


def test_steam_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    now = time.time()
    newest = 5_000_000
    # 1,000,000 seq numbers in 14 days, so 7 days back starts at newest - 500,000
    probe = match(match_id=50, match_seq_num=newest - 1_000_000, start_time=int(now - 14 * 86400))
    old = [match(match_id=60, match_seq_num=100, start_time=int(now - 7 * 86400)),
           match(match_id=61, match_seq_num=101, start_time=int(now - 7 * 86400), duration=0)]
    fresh = match(match_id=70, match_seq_num=102, start_time=int(now - 60))
    session = FakeSession({
        ("GET", f"{rd.STEAM_API}/GetMatchHistory/"): [
            FakeResponse(json_data={"result": {"matches": [{"match_seq_num": newest}]}})],
        ("GET", f"{rd.STEAM_API}/GetMatchHistoryBySequenceNum/"): [
            steam_page(probe), steam_page(*old), steam_page(fresh)],
    })
    monkeypatch.setattr(rd.requests, "Session", lambda: session)
    monkeypatch.setattr(rd.Paced, "wait", lambda self: None)
    uploads: list[str] = []
    monkeypatch.setattr(rd, "upload", lambda local, dest: uploads.append(dest) or True)
    cfg = rd.Config("k", None, "remote:raw", tmp_path)

    rd.run_steam(cfg, max_minutes=0.005, days_back=7, sleep=lambda s: None)

    state = rd.State(cfg.db)
    assert state.get_meta("steam_cursor") == "102"  # stopped before the page of live games
    saved = sorted(p.name for p in (tmp_path / "staging" / "steam").iterdir())
    assert saved == [f"seq_{newest - 1_000_000}.json.gz", f"seq_{newest - 500_000}.json.gz"]  # probe, then start
    assert state.counts("matches") == {"discovered": 1}
    assert state.counts("callbacks") == {"pending": 1}
    assert uploads and all(dest == "remote:raw/steam" for dest in uploads)


def test_save_gz_round_trips(tmp_path: Path):
    size = rd.save_gz(tmp_path, "seq_5.json.gz", b"x" * 1000)
    assert gzip.decompress((tmp_path / "seq_5.json.gz").read_bytes()) == b"x" * 1000
    assert size < 1000
    assert not list(tmp_path.glob("*.part"))


# Replay route

def test_opendota_stops_at_daily_reserve():
    session = FakeSession({("GET", rd.OPENDOTA_API): [
        FakeResponse(json_data={"replay_url": "u"}, headers={"x-rate-limit-remaining-day": "20"})]})
    od = rd.OpenDota(session, None, reserve=20, pace=no_pace())
    assert od.replay_url(1) == "u"
    with pytest.raises(rd.BudgetExhausted):
        od.replay_url(2)
    assert len(session.calls) == 1


def test_opendota_hard_cap_applies_with_a_key(state: rd.State):
    # a keyed (billed) response carries no free-tier header, so only the hard cap can stop it
    session = FakeSession({("GET", rd.OPENDOTA_API): [FakeResponse(json_data={"replay_url": "u"})]})
    od = rd.OpenDota(session, "paid-key", pace=no_pace(), state=state, daily_cap=2)
    od.replay_url(1)
    od.replay_url(2)
    with pytest.raises(rd.BudgetExhausted):
        od.replay_url(3)
    assert len(session.calls) == 2
    assert session.calls[0][2] == {"api_key": "paid-key"}


def test_cap_from_env_file_is_applied(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rd, "ROOT", tmp_path)
    (tmp_path / ".env").write_text("OPENDOTA_DAILY_CAP=500\n")
    monkeypatch.delenv("OPENDOTA_DAILY_CAP", raising=False)
    assert rd.load_config().opendota_daily_cap == 500
    monkeypatch.delenv("OPENDOTA_DAILY_CAP", raising=False)


def test_opendota_429_with_no_calls_left_stops():
    session = FakeSession({("GET", rd.OPENDOTA_API): [
        FakeResponse(status=429, headers={"x-rate-limit-remaining-day": "0"})]})
    od = rd.OpenDota(session, None, reserve=0, pace=no_pace(), sleep=lambda s: None)
    with pytest.raises(rd.BudgetExhausted):
        od.replay_url(1)


def test_replay_route_waits_until_a_match_is_a_day_old(tmp_path: Path):
    state = rd.State(tmp_path / "state.db")
    now = time.time()
    state.add_discovered([match(match_id=1, start_time=int(now - 3600)),
                          match(match_id=2, start_time=int(now - rd.MIN_REPLAY_AGE_S - 1))])
    assert [row[0] for row in state.due(now, 10)] == [2]


def test_downloads_when_replay_url_known(state: rd.State, tmp_path: Path):
    session = FakeSession({
        ("GET", rd.OPENDOTA_API): [FakeResponse(json_data={"replay_url": "http://replay1.valve.net/570/1_9.dem.bz2"})],
        ("GET", "http://replay1.valve.net"): [FakeResponse(body=ZSTD)],
    })
    od = rd.OpenDota(session, None, pace=no_pace())
    size = rd.resolve_and_download(state, od, session, tmp_path / "replays", (1, "discovered", None, 0))
    assert size == len(ZSTD)
    assert (tmp_path / "replays" / "1.dem.zst").read_bytes() == ZSTD
    assert state.counts("matches")["downloaded"] == 1


def test_missing_salt_requests_fetch_then_gives_up(state: rd.State, tmp_path: Path):
    session = FakeSession({
        ("GET", rd.OPENDOTA_API): [FakeResponse(json_data={"replay_url": None})],
        ("POST", rd.OPENDOTA_API): [FakeResponse()],
    })
    od = rd.OpenDota(session, None, pace=no_pace())
    rd.resolve_and_download(state, od, session, tmp_path, (1, "discovered", None, 0))
    assert ("POST", f"{rd.OPENDOTA_API}/request/1", {}) in session.calls
    assert state.counts("matches")["requested"] == 1
    rd.resolve_and_download(state, od, session, tmp_path, (1, "requested", None, rd.MAX_ATTEMPTS - 1))
    assert state.counts("matches")["no_replay"] == 1


def test_expired_replay_is_marked_gone(state: rd.State, tmp_path: Path):
    session = FakeSession({("GET", "http://replay1.valve.net"): [FakeResponse(status=404)]})
    od = rd.OpenDota(session, None, pace=no_pace())
    rd.resolve_and_download(state, od, session, tmp_path, (1, "ready", "http://replay1.valve.net/570/1_9.dem.bz2", 0))
    assert state.counts("matches")["gone"] == 1


def test_slow_download_is_dropped_and_retried(state: rd.State, tmp_path: Path):
    session = FakeSession({("GET", "http://replay1.valve.net"): [FakeResponse(body=ZSTD)]})
    ticks = iter([0, rd.STALL_GRACE_S + 1])  # 64 bytes in 61 seconds
    with pytest.raises(rd.StalledDownload):
        rd.download_replay(session, "http://replay1.valve.net/x", tmp_path, 1, clock=lambda: next(ticks))
    rd.record_download(state, 1, 0, rd.StalledDownload("slow"))
    assert state.counts("matches")["ready"] == 1  # retried on a later pass


def test_replay_run_downloads_in_parallel_and_uploads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg = rd.Config("k", None, "remote:raw", tmp_path)
    state = rd.State(cfg.db)
    state.add_discovered([match(match_id=i) for i in (1, 2, 3)])
    state.update(3, status="downloading", replay_url="http://replay1.valve.net/570/3_9.dem.bz2")  # left by a crash
    session = FakeSession({
        ("GET", rd.OPENDOTA_API): [FakeResponse(json_data={"replay_url": "http://replay1.valve.net/570/x.dem.bz2"})],
        ("GET", "http://replay1.valve.net"): [FakeResponse(body=ZSTD)],
    })
    monkeypatch.setattr(rd.requests, "Session", lambda: session)
    monkeypatch.setattr(rd.Paced, "wait", lambda self: None)
    uploads: list[str] = []
    monkeypatch.setattr(rd, "upload", lambda local, dest: uploads.append(dest) or True)

    rd.run_replays(cfg, max_minutes=0.01, sleep=lambda s: None, workers=2)

    assert state.counts("matches") == {"downloaded": 3}
    assert sorted(p.name for p in (tmp_path / "staging" / "replays").iterdir()) == \
        ["1.dem.zst", "2.dem.zst", "3.dem.zst"]
    assert uploads and all(dest == "remote:raw/replays" for dest in uploads)
    assert state.calls_today("opendota") == 2  # match 3 already had its URL


def test_opendota_error_schedules_a_retry(state: rd.State, tmp_path: Path):
    session = FakeSession({("GET", rd.OPENDOTA_API): [FakeResponse(status=500)]})
    od = rd.OpenDota(session, None, pace=no_pace())
    rd.resolve_and_download(state, od, session, tmp_path, (1, "discovered", None, 0))
    assert state.counts("matches")["requested"] == 1
    assert state.due(now=rd.MIN_REPLAY_AGE_S, limit=10) == [(2, "discovered", None, 0)]


def test_due_order_and_mark_uploaded(state: rd.State, tmp_path: Path):
    state.update(2, status="ready", replay_url="u")
    assert [row[0] for row in state.due(now=rd.MIN_REPLAY_AGE_S, limit=10)] == [2, 1]
    state.update(1, status="downloaded", file=str(tmp_path / "uploaded.dem.zst"))
    assert state.mark_uploaded() == 1
    assert state.counts("matches")["uploaded"] == 1


# Config

def test_dotenv_does_not_override_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nSTEAM_API_KEY='from-file'\nDRIVE_DEST=x:y\n")
    monkeypatch.setenv("STEAM_API_KEY", "from-env")
    monkeypatch.delenv("DRIVE_DEST", raising=False)
    rd.load_dotenv(env)
    assert os.environ["STEAM_API_KEY"] == "from-env"
    assert os.environ["DRIVE_DEST"] == "x:y"
