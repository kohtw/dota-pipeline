"""
Stopgap downloader: collect as much demo data as the API limits allow while the Airflow
pipeline is being built. One machine runs it; files go to Google Drive with rclone.

Two routes, run as separate processes:

  steam    Samples Valve's match feed (GetMatchHistoryBySequenceNum, 100 matches per call)
           forward from a saved cursor: reads one page, skips the next 3, so it covers every
           hour of the day and moves faster than real time until it is 3 hours behind live
           games. Saves every page it reads, gzipped. Queues completed ranked All Pick
           matches for the replay route. Ranked All Pick matches with no duration yet go on a
           callback list and are fetched again later.
  replays  Draws queued matches at random times from the newest day of the queue (matches at
           least a day old), gets each replay URL from OpenDota (60 calls a minute, 3,000 a
           day), and downloads replays from Valve as delivered, 8 at a time. Downloads slower
           than 100 KB/s after a minute, or that cannot connect, are retried 30 minutes later.

State lives in SQLite, so both routes resume where they stopped and never repeat work.

Drive layout (DRIVE_DEST):
  steam/seq_{start_seq}.json.gz        one Steam page: 100 consecutive matches, every mode, unfiltered
  steam/callback_{match_id}.json.gz    a match fetched again after it finished
  replays/{match_id}.dem.zst           one replay, compressed as Valve serves it

Config (environment, or a .env file at the repo root):
  STEAM_API_KEY      required
  OPENDOTA_API_KEY   optional; every call is then billed (about US$0.0001 each)
  OPENDOTA_DAILY_CAP hard cap on OpenDota calls per UTC day, keyed or not (default 3000)
  DRIVE_DEST         rclone destination  (default: "Google Drive:dota-pipeline/raw")
  DOWNLOADER_HOME    state, staging, logs (default: ~/.local/share/dota-pipeline)

Run:
  python3 scripts/downloader.py check      # config only, no API calls
  python3 scripts/downloader.py steam      # one pass of the Steam route
  python3 scripts/downloader.py replays    # one pass of the replay route
  python3 scripts/downloader.py status     # progress, no API calls
"""

import argparse
import datetime as dt
import fcntl
import gzip
import logging
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
STEAM_API = "https://api.steampowered.com/IDOTA2Match_570"
OPENDOTA_API = "https://api.opendota.com/api"

RANKED_LOBBY = 7  # lobby_type 7 = ranked matchmaking
ALL_PICK = 22  # game_mode 22 = All Pick; excludes Turbo (23)
MIN_DURATION_S = 15 * 60  # drops remakes and early surrenders
MAX_LEAVER_STATUS = 1  # 0 = stayed, 1 = disconnected but came back; 2+ = abandoned, AFK, never connected

STEAM_DAILY_CAP = 95_000  # Valve allows 100,000 a day; the rest is headroom
STEAM_START_INTERVAL_S = 2.0
STEAM_MIN_INTERVAL_S = 1.0
STEAM_MAX_INTERVAL_S = 60.0
STEAM_BACKOFF_S = 15
STEAM_MAX_RETRIES = 6
STEAM_SPEEDUP_AFTER = 10  # successful calls before shortening the gap by a quarter
DAYS_BACK = 7  # where the Steam route starts on its first run
HEAD_LAG_S = 3 * 3600  # near the newest matches, wait so most of them have finished
CALLBACK_AFTER_S = 3600
CALLBACK_MAX = 3
STEAM_UPLOAD_EVERY = 100  # pages per rclone batch
# Read 1 page in 4. One page in sequence covers about 0.5 s of real match time per second, so a
# stride of 4 moves at about 2x real time, catching up by an hour every hour until HEAD_LAG_S.
STEAM_STRIDE = 4

MIN_REPLAY_AGE_S = 24 * 3600  # matches under an hour old had no replay salt on OpenDota
RETRY_AFTER_S = 2 * 3600  # wait after asking OpenDota to fetch a salt
ERROR_RETRY_S = 600
MAX_ATTEMPTS = 3
OPENDOTA_RESERVE = 20  # stop with this many daily calls left
# When the free tier says the day is spent, check again this often. Its reset time isn't documented:
# it came at 00:00 UTC on 2026-11-08 and 2026-11-09, but not by 03:23 UTC on 2026-11-10.
OPENDOTA_RECHECK_S = 3600
OPENDOTA_OUTAGE_AFTER = 5  # server errors in a row before a run gives up on OpenDota
# Hard cap on OpenDota calls per UTC day, keyed or not. With a key each call costs about US$0.0001,
# so 3,000 calls is about US$0.30 a day. Raise it with OPENDOTA_DAILY_CAP in .env.
OPENDOTA_DAILY_CAP = 3000
OPENDOTA_USD_PER_CALL = 0.0001
REPLAY_UPLOAD_EVERY = 20
DOWNLOAD_WORKERS = 8  # China-cluster downloads run at about 0.5 MB/s, so a few can be slow at once
MIN_DOWNLOAD_BPS = 100_000  # give up (and retry later) below this average speed...
STALL_GRACE_S = 60  # ...once a download has run this long
DOWNLOAD_RETRY_S = 30 * 60  # after a stall or a failed connection
SAMPLE_WINDOW_S = 24 * 3600  # replays are drawn from the newest day of the queue

MIN_FREE_GB = 20
CHINA_REPLAY_HOSTS = (".dota2.com.cn",)  # slower from here, and timed out once; logged separately

log = logging.getLogger("downloader")


class BudgetExhausted(Exception):
    pass


class OpenDotaDown(RuntimeError):
    """A 5xx or a failed connection: OpenDota's fault, not the match's, so it costs no attempt."""


class ReplayGone(Exception):
    pass


@dataclass(frozen=True)
class Config:
    steam_key: str
    opendota_key: str | None
    drive_dest: str
    home: Path
    opendota_daily_cap: int = OPENDOTA_DAILY_CAP

    @property
    def db(self) -> Path:
        return self.home / "state.db"

    @property
    def staging(self) -> Path:
        return self.home / "staging"


def load_dotenv(path: Path) -> None:
    """Sets variables from a KEY=VALUE file without overriding the environment."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def load_config() -> Config:
    load_dotenv(ROOT / ".env")
    return Config(
        steam_key=os.environ.get("STEAM_API_KEY", ""),
        opendota_key=os.environ.get("OPENDOTA_API_KEY") or None,
        drive_dest=os.environ.get("DRIVE_DEST", "Google Drive:dota-pipeline/raw"),
        home=Path(os.environ.get("DOWNLOADER_HOME", Path.home() / ".local/share/dota-pipeline")),
        opendota_daily_cap=int(os.environ.get("OPENDOTA_DAILY_CAP") or OPENDOTA_DAILY_CAP),
    )


def is_ranked_all_pick(match: dict) -> bool:
    return match.get("lobby_type") == RANKED_LOBBY and match.get("game_mode") == ALL_PICK


def is_in_progress(match: dict) -> bool:
    """Duration 0. In the seq feed these are mostly bot and new-player games that never ran, so only
    ranked All Pick ones go on the callback list."""
    return is_ranked_all_pick(match) and match.get("duration", 0) == 0


def is_completed_ranked_all_pick(match: dict) -> bool:
    """Ranked All Pick, played to a normal finish: long enough, and nobody abandoned."""
    players = match.get("players", [])
    return (
        is_ranked_all_pick(match)
        and match.get("duration", 0) >= MIN_DURATION_S
        and "radiant_win" in match
        and len(players) == 10
        and all(p.get("leaver_status", 0) <= MAX_LEAVER_STATUS for p in players)
    )


def replay_suffix(head: bytes) -> str:
    if head.startswith(b"\x28\xb5\x2f\xfd"):
        return ".dem.zst"
    if head.startswith(b"BZh"):
        return ".dem.bz2"
    if head.startswith(b"PBDEMS2"):
        return ".dem"
    raise ValueError(f"unknown replay format, first bytes {head[:8]!r}")


class Paced:
    """Keeps at least min_interval seconds between calls."""

    def __init__(self, min_interval: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.min_interval, self.clock, self.sleep = min_interval, clock, sleep
        self.last = float("-inf")

    def wait(self) -> None:
        gap = self.clock() - self.last
        if gap < self.min_interval:
            self.sleep(self.min_interval - gap)
        self.last = self.clock()


def utc_day() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


class State:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, timeout=30)  # both routes share this file
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id      INTEGER PRIMARY KEY,
                match_seq_num INTEGER,
                start_time    INTEGER,
                status        TEXT NOT NULL,  -- discovered|requested|ready|downloading|downloaded|uploaded|no_replay|gone|failed
                attempts      INTEGER NOT NULL DEFAULT 0,
                next_try      REAL NOT NULL DEFAULT 0,
                replay_url    TEXT,
                file          TEXT,
                bytes         INTEGER,
                error         TEXT,
                updated       REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS callbacks (
                match_id      INTEGER PRIMARY KEY,
                match_seq_num INTEGER NOT NULL,
                status        TEXT NOT NULL,  -- pending|done|gave_up
                attempts      INTEGER NOT NULL DEFAULT 0,
                next_try      REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS api_calls (day TEXT, api TEXT, n INTEGER NOT NULL, PRIMARY KEY (day, api));
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS matches_status_start ON matches (status, start_time);
        """)
        self.db.commit()

    # API call counts, per UTC day
    def count_call(self, api: str) -> int:
        day = utc_day()
        self.db.execute("INSERT INTO api_calls VALUES (?, ?, 1) ON CONFLICT (day, api) DO UPDATE SET n = n + 1",
                        (day, api))
        self.db.commit()
        return self.calls_today(api)

    def calls_today(self, api: str) -> int:
        day = utc_day()
        row = self.db.execute("SELECT n FROM api_calls WHERE day = ? AND api = ?", (day, api)).fetchone()
        return row[0] if row else 0

    def get_meta(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, value))
        self.db.commit()

    # Replay queue
    def add_discovered(self, matches: list[dict]) -> int:
        before = self.db.total_changes
        self.db.executemany(
            "INSERT OR IGNORE INTO matches (match_id, match_seq_num, start_time, status, updated) "
            "VALUES (?, ?, ?, 'discovered', ?)",
            [(m["match_id"], m["match_seq_num"], m["start_time"], time.time()) for m in matches],
        )
        self.db.commit()
        return self.db.total_changes - before

    def due(self, now: float, limit: int,
            rng: random.Random | None = None) -> list[tuple[int, str, str | None, int]]:
        """Replay work: ready and retry rows that are due, then a random sample of new matches."""
        rows = self.db.execute(
            "SELECT match_id, status, replay_url, attempts FROM matches "
            "WHERE status IN ('ready', 'requested') AND next_try <= ? "
            "ORDER BY CASE status WHEN 'ready' THEN 0 ELSE 1 END, match_id DESC LIMIT ?", (now, limit)).fetchall()
        if len(rows) < limit:
            rows += self._sample_discovered(now - MIN_REPLAY_AGE_S, limit - len(rows), rng or random.Random())
        return rows

    def _sample_discovered(self, cutoff: float, n: int, rng: random.Random) -> list[tuple[int, str, str | None, int]]:
        # Uniform over start time, not over matches, so quiet hours are as well covered as busy ones.
        newest = self.db.execute("SELECT MAX(start_time) FROM matches WHERE status = 'discovered' "
                                 "AND start_time <= ?", (cutoff,)).fetchone()[0]
        if newest is None:
            return []
        picks: dict[int, tuple[int, str, str | None, int]] = {}
        for _ in range(n * 3):
            if len(picks) == n:
                break
            row = self.db.execute(
                "SELECT match_id, status, replay_url, attempts FROM matches WHERE status = 'discovered' "
                "AND start_time >= ? AND start_time <= ? ORDER BY start_time LIMIT 1",
                (rng.uniform(newest - SAMPLE_WINDOW_S, newest), cutoff)).fetchone()
            picks[row[0]] = row
        return list(picks.values())

    def waiting(self, now: float) -> int:
        return self.db.execute(
            "SELECT COUNT(*) FROM matches WHERE status IN ('ready', 'requested') "
            "OR (status = 'discovered' AND start_time <= ?)", (now - MIN_REPLAY_AGE_S,)).fetchone()[0]

    def update(self, match_id: int, **fields) -> None:
        fields["updated"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in fields)
        self.db.execute(f"UPDATE matches SET {cols} WHERE match_id = ?", (*fields.values(), match_id))
        self.db.commit()

    def mark_uploaded(self) -> int:
        rows = self.db.execute("SELECT match_id, file FROM matches WHERE status = 'downloaded'").fetchall()
        done = [mid for mid, file in rows if file and not Path(file).exists()]
        for mid in done:
            self.update(mid, status="uploaded")
        return len(done)

    # Callbacks for matches fetched while still in progress
    def add_callbacks(self, matches: list[dict], now: float) -> int:
        before = self.db.total_changes
        self.db.executemany(
            "INSERT OR IGNORE INTO callbacks VALUES (?, ?, 'pending', 0, ?)",
            [(m["match_id"], m["match_seq_num"], now + CALLBACK_AFTER_S) for m in matches],
        )
        self.db.commit()
        return self.db.total_changes - before

    def due_callbacks(self, now: float, limit: int) -> list[tuple[int, int, int]]:
        return self.db.execute(
            "SELECT match_id, match_seq_num, attempts FROM callbacks WHERE status = 'pending' AND next_try <= ? "
            "ORDER BY next_try LIMIT ?", (now, limit)).fetchall()

    def update_callback(self, match_id: int, status: str, attempts: int, next_try: float = 0) -> None:
        self.db.execute("UPDATE callbacks SET status = ?, attempts = ?, next_try = ? WHERE match_id = ?",
                        (status, attempts, next_try, match_id))
        self.db.commit()

    def counts(self, table: str) -> dict[str, int]:
        return dict(self.db.execute(f"SELECT status, COUNT(*) FROM {table} GROUP BY status").fetchall())


class Steam:
    """Valve also rate-limits in the short term (429 after about 10 calls a second apart), so the
    pace adapts: each 429 or 5xx doubles the gap between calls and backs off, and every run of
    successful calls shrinks the gap again, down to STEAM_MIN_INTERVAL_S. Throttling looks like a
    short window, so recovery is fast."""

    def __init__(self, session: requests.Session, key: str, state: State | None = None,
                 pace: Paced | None = None, daily_cap: int = STEAM_DAILY_CAP,
                 sleep: Callable[[float], None] = time.sleep, deadline: float = float("inf"),
                 clock: Callable[[], float] = time.monotonic):
        self.session, self.key, self.state, self.daily_cap, self.sleep = session, key, state, daily_cap, sleep
        self.deadline, self.clock = deadline, clock
        self.pace = pace or Paced(STEAM_START_INTERVAL_S)
        self.calls = self.throttled = self.streak = 0

    def _get(self, endpoint: str, **params) -> requests.Response:
        for attempt in range(STEAM_MAX_RETRIES + 1):
            if self.state and self.state.calls_today("steam") >= self.daily_cap:
                raise BudgetExhausted(f"Steam daily cap of {self.daily_cap} calls reached")
            self.pace.wait()
            self.calls += 1
            if self.state:
                self.state.count_call("steam")
            try:
                r = self.session.get(f"{STEAM_API}/{endpoint}/v1/", params={"key": self.key, **params}, timeout=60)
            except requests.RequestException as e:
                raise RuntimeError(f"Steam {endpoint}: {str(e).replace(self.key, '<key>')}") from None
            if r.status_code == 429 or r.status_code >= 500:
                self.throttled += 1
                self.streak = 0
                self.pace.min_interval = min(max(self.pace.min_interval, STEAM_MIN_INTERVAL_S) * 2, STEAM_MAX_INTERVAL_S)
                backoff = min(STEAM_BACKOFF_S * 2 ** attempt, 600)
                if self.clock() + backoff > self.deadline:
                    raise BudgetExhausted(f"Steam returned {r.status_code}; backing off past this run's time limit")
                log.warning("Steam %s returned %d; waiting %ds, then 1 call every %.1fs",
                            endpoint, r.status_code, backoff, self.pace.min_interval)
                self.sleep(backoff)
                continue
            if not r.ok:
                raise RuntimeError(f"Steam {endpoint}: HTTP {r.status_code}")
            self.streak += 1
            if self.streak >= STEAM_SPEEDUP_AFTER:
                self.pace.min_interval = max(self.pace.min_interval * 0.75, STEAM_MIN_INTERVAL_S)
                self.streak = 0
            return r
        raise BudgetExhausted(f"Steam kept refusing calls after {STEAM_MAX_RETRIES} retries; ending this run")

    def newest_seq(self) -> int:
        return self._get("GetMatchHistory", matches_requested=1).json()["result"]["matches"][0]["match_seq_num"]

    def page(self, start_seq: int, count: int = 100) -> tuple[bytes, list[dict]]:
        r = self._get("GetMatchHistoryBySequenceNum", start_at_match_seq_num=start_seq, matches_requested=count)
        return r.content, r.json()["result"].get("matches", [])

    def seq_for_age(self, hours: float, now: float, probe_back: int = 1_000_000) -> tuple[int, int, bytes]:
        """Estimates the match_seq_num of matches that started `hours` ago, from the seq rate.

        Returns (estimate, probe_seq, probe_page) so the caller can keep the probe page.
        """
        newest = self.newest_seq()
        probe = newest - probe_back
        raw, matches = self.page(probe)
        if not matches:
            return probe, probe, raw
        first = matches[0]
        rate = (newest - first["match_seq_num"]) / max(now - first["start_time"], 1)
        return max(int(newest - rate * hours * 3600), 0), probe, raw


class OpenDota:
    """Free tier without a key. With a key, every call is billed, so a hard daily cap always applies."""

    def __init__(self, session: requests.Session, key: str | None, reserve: int = OPENDOTA_RESERVE,
                 pace: Paced | None = None, sleep: Callable[[float], None] = time.sleep,
                 state: State | None = None, daily_cap: int = OPENDOTA_DAILY_CAP,
                 clock: Callable[[], float] = time.time):
        self.session, self.key, self.reserve, self.sleep, self.clock = session, key, reserve, sleep, clock
        self.state, self.daily_cap = state, daily_cap
        self.pace = pace or Paced(1.05)  # free tier: 60 calls a minute
        self.calls = 0
        self.remaining_day: int | None = None
        self.server_errors = 0  # in a row

    def _call(self, method: str, path: str) -> requests.Response:
        for attempt in range(2):
            if self.remaining_day is not None and self.remaining_day <= self.reserve:
                raise BudgetExhausted(f"OpenDota daily calls left: {self.remaining_day}")
            recheck = float(self.state.get_meta("opendota_recheck_at") or 0) if self.state else 0
            if recheck > self.clock():
                raise BudgetExhausted("OpenDota free-tier calls used up; checking again at "
                                      + time.strftime("%H:%M", time.localtime(recheck)))
            if self.state and self.state.calls_today("opendota") >= self.daily_cap:
                raise BudgetExhausted(f"OpenDota daily cap of {self.daily_cap} calls reached")
            self.pace.wait()
            self.calls += 1
            if self.state:
                self.state.count_call("opendota")
            params = {"api_key": self.key} if self.key else {}
            try:
                r = self.session.request(method, f"{OPENDOTA_API}/{path}", params=params, timeout=30)
            except requests.RequestException as e:
                self._server_error()
                raise OpenDotaDown(f"OpenDota {path}: {str(e).replace(self.key or '<none>', '<key>')}") from None
            if r.status_code >= 500:
                self._server_error()
                raise OpenDotaDown(f"OpenDota {path}: HTTP {r.status_code}")
            self.server_errors = 0
            day = r.headers.get("x-rate-limit-remaining-day")
            if day is not None:
                self.remaining_day = int(day)
                if self.state and self.remaining_day <= self.reserve:
                    # later runs skip OpenDota until then, without spending a call to find out
                    self.state.set_meta("opendota_recheck_at", str(self.clock() + OPENDOTA_RECHECK_S))
            if r.status_code != 429:
                return r
            if self.remaining_day == 0 or attempt == 1:
                raise BudgetExhausted("OpenDota returned 429")
            self.sleep(60)
        raise AssertionError("unreachable")

    def _server_error(self) -> None:
        self.server_errors += 1
        if self.server_errors >= OPENDOTA_OUTAGE_AFTER:
            raise BudgetExhausted(f"OpenDota looks down: {self.server_errors} server errors in a row")

    def replay_url(self, match_id: int) -> str | None:
        r = self._call("GET", f"matches/{match_id}")
        if r.status_code == 404:
            return None
        if not r.ok:
            raise RuntimeError(f"OpenDota matches/{match_id}: HTTP {r.status_code}")
        return r.json().get("replay_url")

    def request_fetch(self, match_id: int) -> None:
        r = self._call("POST", f"request/{match_id}")
        if not r.ok:
            raise RuntimeError(f"OpenDota request/{match_id}: HTTP {r.status_code}")


def save_gz(dest: Path, name: str, raw: bytes) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    data = gzip.compress(raw)
    part = dest / f"{name}.part"
    part.write_bytes(data)
    part.rename(dest / name)
    return len(data)


def upload(local: Path, dest: str) -> bool:
    """Moves finished files to Drive. rclone deletes each local file after it uploads."""
    if not local.exists():
        return True
    result = subprocess.run(
        ["rclone", "move", str(local), dest, "--exclude", "*.part", "--transfers", "4"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.error("rclone move failed: %s", result.stderr.strip()[-500:])
    return result.returncode == 0


def disk_low(path: Path) -> bool:
    if shutil.disk_usage(path).free < MIN_FREE_GB * 1e9:
        log.warning("free disk below %d GB; stopping", MIN_FREE_GB)
        return True
    return False


# Steam route

def take_page(state: State, matches: list[dict], now: float) -> tuple[int, int]:
    """Queues completed ranked All Pick matches and puts in-progress ones on the callback list."""
    queued = state.add_discovered([m for m in matches if is_completed_ranked_all_pick(m)])
    waiting = state.add_callbacks([m for m in matches if is_in_progress(m)], now)
    return queued, waiting


def run_callbacks(state: State, steam: Steam, staging: Path, now: float, limit: int = 20) -> int:
    """Fetches in-progress matches again, one call each. Returns how many have now finished."""
    finished = 0
    for match_id, seq, attempts in state.due_callbacks(now, limit):
        raw, matches = steam.page(seq, count=1)
        match = next((m for m in matches if m["match_id"] == match_id), None)
        if match and not is_in_progress(match):
            save_gz(staging, f"callback_{match_id}.json.gz", raw)
            if is_completed_ranked_all_pick(match):
                state.add_discovered([match])
            state.update_callback(match_id, "done", attempts + 1)
            finished += 1
        elif attempts + 1 >= CALLBACK_MAX:
            state.update_callback(match_id, "gave_up", attempts + 1)
        else:
            state.update_callback(match_id, "pending", attempts + 1, now + CALLBACK_AFTER_S)
    return finished


def run_steam(cfg: Config, max_minutes: float, days_back: float,
              sleep: Callable[[float], None] = time.sleep) -> None:
    deadline = time.monotonic() + max_minutes * 60
    state = State(cfg.db)
    # start at half the pace learned last run, so each run probes for a faster rate
    interval = max(float(state.get_meta("steam_interval") or STEAM_START_INTERVAL_S) / 2, STEAM_MIN_INTERVAL_S)
    steam = Steam(requests.Session(), cfg.steam_key, state, pace=Paced(interval), sleep=sleep, deadline=deadline)
    staging = cfg.staging / "steam"
    dest = f"{cfg.drive_dest}/steam"
    pages = queued = waiting = finished = stored = since_upload = 0
    try:
        cursor_text = state.get_meta("steam_cursor")
        if cursor_text is None:
            start, probe, probe_raw = steam.seq_for_age(days_back * 24, time.time())
            stored += save_gz(staging, f"seq_{probe}.json.gz", probe_raw)
            cursor = start
            log.info("first run: starting %.1f days back at seq %d", days_back, cursor)
        else:
            cursor = int(cursor_text)
        while time.monotonic() < deadline and not disk_low(cfg.home):
            finished += run_callbacks(state, steam, staging, time.time())
            raw, matches = steam.page(cursor)
            if not matches or time.time() - matches[0]["start_time"] < HEAD_LAG_S:
                # caught up with live games: wait rather than store pages that are mostly unfinished
                sleep(min(300, max(deadline - time.monotonic(), 0)))
                continue
            stored += save_gz(staging, f"seq_{cursor}.json.gz", raw)
            q, w = take_page(state, matches, time.time())
            queued, waiting, pages, since_upload = queued + q, waiting + w, pages + 1, since_upload + 1
            first, last = matches[0]["match_seq_num"], matches[-1]["match_seq_num"]
            cursor = last + 1 + (STEAM_STRIDE - 1) * (last - first + 1)  # skip the next pages
            state.set_meta("steam_cursor", str(cursor))
            if pages % 50 == 1:
                age_h = (time.time() - matches[0]["start_time"]) / 3600
                log.info("seq %d (%.1f h old): %d matches, %d ranked All Pick, %d completed queued, %d in progress",
                         cursor, age_h, len(matches), sum(map(is_ranked_all_pick, matches)), q, w)
            if since_upload >= STEAM_UPLOAD_EVERY:
                upload(staging, dest)
                since_upload = 0
    except BudgetExhausted as e:
        log.info("stopping: %s", e)
    except RuntimeError as e:  # the next run resumes from the saved cursor
        log.error("stopping on error: %s", e)
    finally:
        state.set_meta("steam_interval", f"{steam.pace.min_interval:.2f}")
        upload(staging, dest)
        log.info("steam done: %d pages (%.1f MB gzipped), %d queued for replays, %d in progress, "
                 "%d finished on callback, %d Steam calls today, %d throttled, pace 1 call every %.1fs",
                 pages, stored / 1e6, queued, waiting, finished, state.calls_today("steam"),
                 steam.throttled, steam.pace.min_interval)


# Replay route

class StalledDownload(Exception):
    pass


DOWNLOAD_ERRORS = (ReplayGone, StalledDownload, requests.RequestException, ValueError, OSError)


def download_replay(session: requests.Session, url: str, dest: Path, match_id: int,
                    clock: Callable[[], float] = time.monotonic) -> tuple[Path, int]:
    """Streams one replay to dest. Gives up on a download that stays slower than MIN_DOWNLOAD_BPS."""
    dest.mkdir(parents=True, exist_ok=True)
    part = dest / f"{match_id}.part"
    size, head = 0, b""
    started = clock()
    with session.get(url, stream=True, timeout=60) as r:
        if r.status_code == 404:
            raise ReplayGone(f"Valve has no replay for {match_id}")
        r.raise_for_status()
        with open(part, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if not head:
                    head = chunk[:16]
                f.write(chunk)
                size += len(chunk)
                elapsed = clock() - started
                if elapsed > STALL_GRACE_S and size / elapsed < MIN_DOWNLOAD_BPS:
                    raise StalledDownload(f"{match_id}: {size / elapsed / 1e3:.0f} KB/s after {elapsed:.0f}s")
    final = dest / f"{match_id}{replay_suffix(head)}"
    part.rename(final)
    return final, size


def resolve(state: State, od: OpenDota, row: tuple[int, str, str | None, int]) -> str | None:
    """Gets a match's replay URL, or schedules a retry. Returns the URL when it is ready to download."""
    match_id, status, url, attempts = row
    if status == "ready":
        return url
    try:
        url = od.replay_url(match_id)
        if not url:
            if attempts + 1 >= MAX_ATTEMPTS:
                state.update(match_id, status="no_replay", attempts=attempts + 1)
            else:
                od.request_fetch(match_id)
                state.update(match_id, status="requested", attempts=attempts + 1,
                             next_try=time.time() + RETRY_AFTER_S)
            return None
    except OpenDotaDown as e:
        state.update(match_id, status="requested", next_try=time.time() + ERROR_RETRY_S, error=str(e)[:300])
        log.warning("resolve %s failed: %s", match_id, e)
        return None
    except RuntimeError as e:
        failed = attempts + 1 >= MAX_ATTEMPTS
        state.update(match_id, status="failed" if failed else "requested", attempts=attempts + 1,
                     next_try=time.time() + ERROR_RETRY_S, error=str(e)[:300])
        log.warning("resolve %s failed: %s", match_id, e)
        return None
    state.update(match_id, status="ready", replay_url=url)
    return url


def is_china_host(url: str) -> bool:
    return (urlparse(url).hostname or "").endswith(CHINA_REPLAY_HOSTS)


def record_download(state: State, match_id: int, attempts: int, outcome: tuple[Path, int] | Exception,
                    tag: str = "") -> int:
    """Stores a download's result. Returns bytes downloaded (0 if none)."""
    if isinstance(outcome, ReplayGone):
        state.update(match_id, status="gone", error=str(outcome))
        return 0
    if isinstance(outcome, Exception):
        failed = attempts + 1 >= MAX_ATTEMPTS
        state.update(match_id, status="failed" if failed else "ready", attempts=attempts + 1,
                     next_try=time.time() + DOWNLOAD_RETRY_S, error=str(outcome)[:300])
        log.warning("download %s%s failed: %s", match_id, tag, outcome)
        return 0
    path, size = outcome
    state.update(match_id, status="downloaded", file=str(path), bytes=size, error=None)
    return size


def resolve_and_download(state: State, od: OpenDota, session: requests.Session, staging: Path,
                         row: tuple[int, str, str | None, int]) -> int:
    """Works one match forward in one thread. Returns bytes downloaded (0 if none)."""
    url = resolve(state, od, row)
    if not url:
        return 0
    try:
        outcome: tuple[Path, int] | Exception = download_replay(session, url, staging, row[0])
    except DOWNLOAD_ERRORS as e:
        outcome = e
    return record_download(state, row[0], row[3], outcome)


def download_job(url: str, dest: Path, match_id: int) -> tuple[Path, int] | Exception:
    """Runs in a worker thread: its own session, and errors returned rather than raised."""
    with requests.Session() as session:
        try:
            return download_replay(session, url, dest, match_id)
        except DOWNLOAD_ERRORS as e:
            return e


def run_replays(cfg: Config, max_minutes: float, sleep: Callable[[float], None] = time.sleep,
                workers: int = DOWNLOAD_WORKERS) -> None:
    """Resolves URLs one at a time in this thread (OpenDota is rate-limited) and downloads in parallel.
    Only this thread touches SQLite."""
    deadline = time.monotonic() + max_minutes * 60
    state = State(cfg.db)
    state.db.execute("UPDATE matches SET status = 'ready' WHERE status = 'downloading'")  # left by a crash
    state.db.commit()
    od = OpenDota(requests.Session(), cfg.opendota_key, state=state, daily_cap=cfg.opendota_daily_cap)
    log.info("replay run started; %d matches waiting, %d download workers", state.waiting(time.time()), workers)
    if cfg.opendota_key:
        log.info("OpenDota key set: calls are billed; cap %d a day (about US$%.2f)",
                 cfg.opendota_daily_cap, cfg.opendota_daily_cap * OPENDOTA_USD_PER_CALL)
    staging = cfg.staging / "replays"
    dest = f"{cfg.drive_dest}/replays"
    replays = total_bytes = since_upload = 0
    inflight: dict[Future, tuple[int, int, str]] = {}

    def collect(block: bool) -> None:
        nonlocal replays, total_bytes, since_upload
        if not inflight:
            return
        done, _ = wait(inflight, timeout=None if block else 0, return_when=FIRST_COMPLETED)
        for future in done:
            match_id, attempts, url = inflight.pop(future)
            china = " [china]" if is_china_host(url) else ""
            size = record_download(state, match_id, attempts, future.result(), tag=china)
            if size:
                replays, total_bytes, since_upload = replays + 1, total_bytes + size, since_upload + 1
                log.info("replay %d%s: %.1f MB (%d this run, %.2f GB, %d OpenDota calls today)",
                         match_id, china, size / 1e6, replays, total_bytes / 1e9, state.calls_today("opendota"))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        try:
            while time.monotonic() < deadline and not disk_low(cfg.home):
                collect(block=False)
                if since_upload >= REPLAY_UPLOAD_EVERY:
                    if upload(staging, dest):
                        state.mark_uploaded()
                    since_upload = 0
                if len(inflight) >= workers * 2:  # enough queued; wait for one to finish
                    collect(block=True)
                    continue
                batch = state.due(time.time(), workers * 2 - len(inflight))
                if not batch:
                    if inflight:
                        collect(block=True)
                    else:  # the Steam route has not queued anything old enough yet
                        sleep(min(300, max(deadline - time.monotonic(), 0)))
                    continue
                for row in batch:
                    url = resolve(state, od, row)
                    if url:
                        state.update(row[0], status="downloading")
                        inflight[pool.submit(download_job, url, staging, row[0])] = (row[0], row[3], url)
        except BudgetExhausted as e:
            log.info("stopping: %s", e)
        finally:
            while inflight:  # let started downloads finish; slow ones give up on their own
                collect(block=True)
            if upload(staging, dest):
                state.mark_uploaded()
            log.info("replays done: %d replays (%.1f GB), %d OpenDota calls (%d today, cap %d), "
                     "free-tier calls left: %s", replays, total_bytes / 1e9, od.calls,
                     state.calls_today("opendota"), cfg.opendota_daily_cap, od.remaining_day)


def check(cfg: Config) -> bool:
    """Validates config without calling any API."""
    ok = True

    def report(passed: bool, message: str) -> None:
        nonlocal ok
        ok &= passed
        print(("ok    " if passed else "FAIL  ") + message)

    report(bool(cfg.steam_key), "STEAM_API_KEY is set")
    if cfg.opendota_key:
        print(f"WARN  OPENDOTA_API_KEY is set: calls are billed, capped at {cfg.opendota_daily_cap} a day "
              f"(about US${cfg.opendota_daily_cap * OPENDOTA_USD_PER_CALL:.2f})")
    else:
        print("info  OPENDOTA_API_KEY not set; free tier, 3,000 calls a day, no charges")
    rclone = shutil.which("rclone")
    report(rclone is not None, "rclone is installed")
    if rclone:
        remotes = subprocess.run([rclone, "listremotes"], capture_output=True, text=True).stdout.splitlines()
        remote = cfg.drive_dest.split(":", 1)[0] + ":"
        report(remote in remotes, f"rclone remote {remote!r} exists")
    free_gb = shutil.disk_usage(cfg.home if cfg.home.exists() else Path.home()).free / 1e9
    report(free_gb >= MIN_FREE_GB, f"free disk {free_gb:.0f} GB (need {MIN_FREE_GB})")
    print(f"info  state and staging: {cfg.home}")
    print(f"info  Drive destination: {cfg.drive_dest}")
    return ok


def status(cfg: Config) -> None:
    state = State(cfg.db)
    print(f"Steam calls today: {state.calls_today('steam')} of {STEAM_DAILY_CAP}")
    od_today = state.calls_today("opendota")
    cost = f", about US${od_today * OPENDOTA_USD_PER_CALL:.2f} if billed" if cfg.opendota_key else ", free tier"
    print(f"OpenDota calls today: {od_today} of {cfg.opendota_daily_cap}{cost}")
    od_total = state.db.execute("SELECT COALESCE(SUM(n), 0) FROM api_calls WHERE api = 'opendota'").fetchone()[0]
    print(f"OpenDota calls in total: {od_total}")
    print(f"Steam cursor: {state.get_meta('steam_cursor') or 'not started'}")
    print("Callbacks:", state.counts("callbacks") or "none")
    print("Replay queue:")
    for name, n in sorted(state.counts("matches").items()):
        print(f"  {name:<11} {n}")
    china = dict(state.db.execute("SELECT status, COUNT(*) FROM matches WHERE replay_url LIKE '%.dota2.com.cn/%' "
                                  "GROUP BY status").fetchall())
    print("China-host replays:", china or "none")
    total = state.db.execute("SELECT COALESCE(SUM(bytes), 0) FROM matches").fetchone()[0]
    print(f"Replays downloaded: {total / 1e9:.1f} GB")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("steam")
    s.add_argument("--max-minutes", type=float, default=55)
    s.add_argument("--days-back", type=float, default=DAYS_BACK, help="first run only")
    r = sub.add_parser("replays")
    r.add_argument("--max-minutes", type=float, default=55)
    sub.add_parser("check")
    sub.add_parser("status")
    args = ap.parse_args()

    cfg = load_config()
    if args.cmd == "check":
        sys.exit(0 if check(cfg) else 1)
    cfg.home.mkdir(parents=True, exist_ok=True)
    if args.cmd == "status":
        status(cfg)
        return
    if not cfg.steam_key and args.cmd == "steam":
        sys.exit("STEAM_API_KEY is not set; run `check` for details")

    (cfg.home / "logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format=f"%(asctime)s {args.cmd} %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(cfg.home / "logs" / f"{args.cmd}.log")],
    )
    with open(cfg.home / f"{args.cmd}.lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log.info("another %s run is in progress; exiting", args.cmd)
            return
        if args.cmd == "steam":
            run_steam(cfg, args.max_minutes, args.days_back)
        else:
            run_replays(cfg, args.max_minutes)


if __name__ == "__main__":
    main()
