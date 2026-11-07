"""
Replay downloader: Steam match feed -> OpenDota replay URL -> Valve replay -> Google Drive.

Runs locally, outside Airflow, until the orchestration stack is ready.

  1. discover  Steam GetMatchHistoryBySequenceNum (100 matches per call); keep ranked All Pick.
               Each Steam page is also saved as raw JSON.
  2. resolve   OpenDota /matches/{id} gives replay_url. On a miss, POST /request/{id} so
               OpenDota fetches the replay salt, and check again later.
  3. download  Stream the replay from Valve into a staging folder, stored as delivered (compressed).
  4. upload    rclone move staging -> Google Drive. rclone deletes each local file once uploaded.

State lives in SQLite, so runs resume and never fetch a match twice. A run stops at its time
limit, when the OpenDota daily budget is nearly spent, or when free disk runs low.

Config (environment, or a .env file at the repo root):
  STEAM_API_KEY      required
  OPENDOTA_API_KEY   optional; lifts the free daily limit (paid per call)
  DRIVE_DEST         rclone destination  (default: "Google Drive:dota-pipeline/raw")
  DOWNLOADER_HOME    state, staging, logs (default: ~/.local/share/dota-pipeline)

Run:
  python3 scripts/replay_downloader.py check    # config only, no API calls
  python3 scripts/replay_downloader.py run      # one pass
  python3 scripts/replay_downloader.py status   # counts by status, no API calls
"""

import argparse
import fcntl
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import requests

ROOT = Path(__file__).resolve().parent.parent
STEAM_API = "https://api.steampowered.com/IDOTA2Match_570"
OPENDOTA_API = "https://api.opendota.com/api"

RANKED_LOBBY = 7  # lobby_type 7 = ranked matchmaking
ALL_PICK = 22  # game_mode 22 = All Pick; excludes Turbo (23)
AGE_HOURS = 24  # start this far back: matches under an hour old had no replay salt on OpenDota
RETRY_AFTER_S = 2 * 3600  # wait after asking OpenDota to fetch a salt
ERROR_RETRY_S = 600
MAX_ATTEMPTS = 3
OPENDOTA_RESERVE = 20  # stop with this many daily calls left
UPLOAD_EVERY = 20  # replays per rclone batch
MIN_FREE_GB = 20

log = logging.getLogger("replay_downloader")


class BudgetExhausted(Exception):
    pass


class ReplayGone(Exception):
    pass


@dataclass(frozen=True)
class Config:
    steam_key: str
    opendota_key: str | None
    drive_dest: str
    home: Path

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
    )


def is_ranked_all_pick(match: dict) -> bool:
    return (
        match.get("lobby_type") == RANKED_LOBBY
        and match.get("game_mode") == ALL_PICK
        and match.get("duration", 0) > 0
        and len(match.get("players", [])) == 10
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


class Steam:
    def __init__(self, session: requests.Session, key: str, pace: Paced | None = None):
        self.session, self.key = session, key
        self.pace = pace or Paced(1.0)
        self.calls = 0

    def _get(self, endpoint: str, **params) -> requests.Response:
        self.pace.wait()
        self.calls += 1
        try:
            r = self.session.get(f"{STEAM_API}/{endpoint}/v1/", params={"key": self.key, **params}, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Steam {endpoint}: {str(e).replace(self.key, '<key>')}") from None
        return r

    def newest_seq(self) -> int:
        return self._get("GetMatchHistory", matches_requested=1).json()["result"]["matches"][0]["match_seq_num"]

    def page(self, start_seq: int) -> tuple[bytes, list[dict]]:
        r = self._get("GetMatchHistoryBySequenceNum", start_at_match_seq_num=start_seq, matches_requested=100)
        return r.content, r.json()["result"].get("matches", [])

    def seq_for_age(self, hours: float, now: float, probe_back: int = 1_000_000) -> int:
        """Estimates the match_seq_num of matches that started `hours` ago, from the seq rate."""
        newest = self.newest_seq()
        _, matches = self.page(newest - probe_back)
        if not matches:
            return newest - probe_back
        first = matches[0]
        rate = (newest - first["match_seq_num"]) / max(now - first["start_time"], 1)
        return int(newest - rate * hours * 3600)


class OpenDota:
    def __init__(self, session: requests.Session, key: str | None, reserve: int = OPENDOTA_RESERVE,
                 pace: Paced | None = None, sleep: Callable[[float], None] = time.sleep):
        self.session, self.key, self.reserve, self.sleep = session, key, reserve, sleep
        self.pace = pace or Paced(1.05)  # free tier: 60 calls a minute
        self.calls = 0
        self.remaining_day: int | None = None

    def _call(self, method: str, path: str) -> requests.Response:
        for attempt in range(2):
            if self.remaining_day is not None and self.remaining_day <= self.reserve:
                raise BudgetExhausted(f"OpenDota daily calls left: {self.remaining_day}")
            self.pace.wait()
            self.calls += 1
            params = {"api_key": self.key} if self.key else {}
            try:
                r = self.session.request(method, f"{OPENDOTA_API}/{path}", params=params, timeout=30)
            except requests.RequestException as e:
                raise RuntimeError(f"OpenDota {path}: {str(e).replace(self.key or '<none>', '<key>')}") from None
            day = r.headers.get("x-rate-limit-remaining-day")
            if day is not None:
                self.remaining_day = int(day)
            if r.status_code != 429:
                return r
            if self.remaining_day == 0 or attempt == 1:
                raise BudgetExhausted("OpenDota returned 429")
            self.sleep(60)
        raise AssertionError("unreachable")

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


class State:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id      INTEGER PRIMARY KEY,
                match_seq_num INTEGER,
                start_time    INTEGER,
                status        TEXT NOT NULL,  -- discovered|requested|ready|downloaded|uploaded|no_replay|gone|failed
                attempts      INTEGER NOT NULL DEFAULT 0,
                next_try      REAL NOT NULL DEFAULT 0,
                replay_url    TEXT,
                file          TEXT,
                bytes         INTEGER,
                error         TEXT,
                updated       REAL NOT NULL
            )""")
        self.db.commit()

    def add_discovered(self, matches: list[dict]) -> int:
        before = self.db.total_changes
        self.db.executemany(
            "INSERT OR IGNORE INTO matches (match_id, match_seq_num, start_time, status, updated) "
            "VALUES (?, ?, ?, 'discovered', ?)",
            [(m["match_id"], m["match_seq_num"], m["start_time"], time.time()) for m in matches],
        )
        self.db.commit()
        return self.db.total_changes - before

    def due(self, now: float, limit: int) -> list[tuple[int, str, str | None, int]]:
        """Matches to work on: ready ones first, then retries that are due, then new ones."""
        return self.db.execute(
            "SELECT match_id, status, replay_url, attempts FROM matches "
            "WHERE status IN ('ready', 'discovered') OR (status = 'requested' AND next_try <= ?) "
            "ORDER BY CASE status WHEN 'ready' THEN 0 WHEN 'requested' THEN 1 ELSE 2 END, match_id DESC "
            "LIMIT ?", (now, limit)).fetchall()

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

    def counts(self) -> dict[str, int]:
        return dict(self.db.execute("SELECT status, COUNT(*) FROM matches GROUP BY status").fetchall())


def download_replay(session: requests.Session, url: str, dest: Path, match_id: int) -> tuple[Path, int]:
    dest.mkdir(parents=True, exist_ok=True)
    part = dest / f"{match_id}.part"
    size, head = 0, b""
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
    final = dest / f"{match_id}{replay_suffix(head)}"
    part.rename(final)
    return final, size


def upload(staging: Path, dest: str) -> bool:
    """Moves finished files to Drive. rclone deletes each local file after it uploads."""
    if not staging.exists():
        return True
    result = subprocess.run(
        ["rclone", "move", str(staging), dest, "--exclude", "*.part", "--transfers", "4"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.error("rclone move failed: %s", result.stderr.strip()[-500:])
    return result.returncode == 0


def resolve_and_download(state: State, od: OpenDota, session: requests.Session, staging: Path,
                         row: tuple[int, str, str | None, int]) -> int:
    """Works one match forward. Returns bytes downloaded (0 if none)."""
    match_id, status, url, attempts = row
    if status != "ready":
        try:
            url = od.replay_url(match_id)
            if not url:
                if attempts + 1 >= MAX_ATTEMPTS:
                    state.update(match_id, status="no_replay", attempts=attempts + 1)
                else:
                    od.request_fetch(match_id)
                    state.update(match_id, status="requested", attempts=attempts + 1,
                                 next_try=time.time() + RETRY_AFTER_S)
                return 0
        except RuntimeError as e:
            failed = attempts + 1 >= MAX_ATTEMPTS
            state.update(match_id, status="failed" if failed else "requested", attempts=attempts + 1,
                         next_try=time.time() + ERROR_RETRY_S, error=str(e)[:300])
            log.warning("resolve %s failed: %s", match_id, e)
            return 0
        state.update(match_id, status="ready", replay_url=url)
    try:
        path, size = download_replay(session, url, staging / "replays", match_id)
    except ReplayGone as e:
        state.update(match_id, status="gone", error=str(e))
        return 0
    except (requests.RequestException, ValueError, OSError) as e:
        failed = attempts + 1 >= MAX_ATTEMPTS
        state.update(match_id, status="failed" if failed else "ready", attempts=attempts + 1, error=str(e)[:300])
        log.warning("download %s failed: %s", match_id, e)
        return 0
    state.update(match_id, status="downloaded", file=str(path), bytes=size, error=None)
    return size


def run(cfg: Config, max_minutes: float, max_steam_pages: int, age_hours: float) -> None:
    deadline = time.monotonic() + max_minutes * 60
    state = State(cfg.db)
    session = requests.Session()
    steam, od = Steam(session, cfg.steam_key), OpenDota(session, cfg.opendota_key)
    cursor: int | None = None
    pages = replays = total_bytes = since_upload = 0
    try:
        while time.monotonic() < deadline:
            if shutil.disk_usage(cfg.home).free < MIN_FREE_GB * 1e9:
                log.warning("free disk below %d GB; stopping", MIN_FREE_GB)
                break
            batch = state.due(time.time(), 25)
            if not batch:
                if pages >= max_steam_pages:
                    log.info("Steam page limit reached and no work left")
                    break
                if cursor is None:
                    cursor = steam.seq_for_age(age_hours, time.time())
                raw, matches = steam.page(cursor)
                if not matches:
                    break
                (cfg.staging / "steam").mkdir(parents=True, exist_ok=True)
                (cfg.staging / "steam" / f"seq_{cursor}.json").write_bytes(raw)
                added = state.add_discovered([m for m in matches if is_ranked_all_pick(m)])
                age_h = (time.time() - matches[0]["start_time"]) / 3600
                log.info("Steam page seq %d (%.1f h old): %d matches, %d new ranked All Pick",
                         cursor, age_h, len(matches), added)
                cursor = matches[-1]["match_seq_num"] + 1
                pages += 1
                continue
            for row in batch:
                if time.monotonic() >= deadline:
                    break
                size = resolve_and_download(state, od, session, cfg.staging, row)
                if size:
                    replays, total_bytes, since_upload = replays + 1, total_bytes + size, since_upload + 1
                if since_upload >= UPLOAD_EVERY:
                    if upload(cfg.staging, cfg.drive_dest):
                        state.mark_uploaded()
                    since_upload = 0
    except BudgetExhausted as e:
        log.info("stopping: %s", e)
    finally:
        if upload(cfg.staging, cfg.drive_dest):
            state.mark_uploaded()
        log.info("run done: %d replays (%.1f GB), %d Steam calls, %d OpenDota calls, OpenDota day left %s",
                 replays, total_bytes / 1e9, steam.calls, od.calls, od.remaining_day)


def check(cfg: Config) -> bool:
    """Validates config without calling any API."""
    ok = True

    def report(passed: bool, message: str) -> None:
        nonlocal ok
        ok &= passed
        print(("ok    " if passed else "FAIL  ") + message)

    report(bool(cfg.steam_key), "STEAM_API_KEY is set")
    print(f"info  OPENDOTA_API_KEY {'is set' if cfg.opendota_key else 'not set; free tier, 3,000 calls a day'}")
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--max-minutes", type=float, default=55)
    r.add_argument("--max-steam-pages", type=int, default=200)
    r.add_argument("--age-hours", type=float, default=AGE_HOURS)
    sub.add_parser("check")
    sub.add_parser("status")
    args = ap.parse_args()

    cfg = load_config()
    if args.cmd == "check":
        sys.exit(0 if check(cfg) else 1)

    cfg.home.mkdir(parents=True, exist_ok=True)
    if args.cmd == "status":
        state = State(cfg.db)
        for status, n in sorted(state.counts().items()):
            print(f"{status:<11} {n}")
        total = state.db.execute("SELECT COALESCE(SUM(bytes), 0) FROM matches").fetchone()[0]
        print(f"downloaded  {total / 1e9:.1f} GB in total")
        return

    if not cfg.steam_key:
        sys.exit("STEAM_API_KEY is not set; run `check` for details")
    (cfg.home / "logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(cfg.home / "logs" / "downloader.log")],
    )
    with open(cfg.home / "lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log.info("another run is in progress; exiting")
            return
        run(cfg, args.max_minutes, args.max_steam_pages, args.age_hours)


if __name__ == "__main__":
    main()
