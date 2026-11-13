"""Valve Steam Web API (needs STEAM_API_KEY)."""

import os
import time
from typing import Callable

import requests

from dota.errors import PermanentError, is_transient_status, redact

API = "https://api.steampowered.com/IDOTA2Match_570"
ALL_PICK = 22  # game_mode; Turbo is 23
RANKED = 7  # lobby_type
MAX_PAGES = 20  # ceiling, not a target: the loop stops once it has n ids (usually ~2 pages of 100 matches)
STEAM_ATTEMPTS = 5


def _retry_after(r: requests.Response) -> int | None:
    value = r.headers.get("Retry-After", "")
    return int(value) if value.isdecimal() and int(value) <= 60 else None


def call(method: str, *, sleep: Callable[[float], None] = time.sleep, **params) -> dict:
    """Call a Steam method, retrying 429, 5xx and network errors with backoff. Other 4xx
    raise PermanentError."""
    # requests errors carry the full URL with the key, so messages are redacted and raised
    # from None to keep the original out of the traceback
    for attempt in range(STEAM_ATTEMPTS):
        # Valve throttles for minutes at a time, so waits grow from 15 s to 120 s
        wait = min(15 * 2 ** attempt, 120)
        try:
            r = requests.get(f"{API}/{method}/v1/", params={"key": os.environ["STEAM_API_KEY"], **params}, timeout=60)
        except requests.RequestException as e:
            problem = redact(str(e))
        else:
            if r.ok:
                return r.json()["result"]
            problem = redact(f"HTTP {r.status_code} {r.reason}")
            if not is_transient_status(r.status_code):
                raise PermanentError(f"Steam {method} failed: {problem}") from None
            if (seconds := _retry_after(r)) is not None:
                wait = max(seconds, wait) if r.status_code == 429 else seconds
        if attempt < STEAM_ATTEMPTS - 1:
            sleep(wait)
    raise RuntimeError(f"Steam {method} failed after {STEAM_ATTEMPTS} attempts: {problem}") from None


def newest_seq() -> int:
    """match_seq_num of the newest match. Seq nums are assigned in the order matches END."""
    return call("GetMatchHistory", matches_requested=1)["matches"][0]["match_seq_num"]


def ranked_all_pick_from(seq: int, n: int, save_page: Callable[[int, dict], None] | None = None,
                         max_pages: int = MAX_PAGES, *, sleep: Callable[[float], None] = time.sleep,
                         pace_s: float = 2.0) -> list[int]:
    """First n ranked All Pick match ids that ended at or after seq, waiting pace_s between pages.
    If save_page is given it is called with (start_seq, raw result dict) for every page fetched."""
    ids = []
    for i in range(max_pages):
        if i:
            sleep(pace_s)
        page = call("GetMatchHistoryBySequenceNum", start_at_match_seq_num=seq, matches_requested=100)
        if save_page:
            save_page(seq, page)
        matches = page["matches"]
        ids += [m["match_id"] for m in matches if m["game_mode"] == ALL_PICK and m["lobby_type"] == RANKED]
        if not matches or len(ids) >= n:
            break
        seq = matches[-1]["match_seq_num"] + 1
    return ids[:n]


def fetch_pages(start_seq: int, pages: int, save_page: Callable[[int, dict], None], *,
                sleep: Callable[[float], None] = time.sleep, pace_s: float = 2.0) -> int:
    """Fetch up to `pages` contiguous pages of 100 matches from start_seq and save each. Stops at
    the first empty page. Returns the number of pages saved."""
    seq = start_seq
    for n in range(pages):
        if n:
            sleep(pace_s)
        page = call("GetMatchHistoryBySequenceNum", start_at_match_seq_num=seq, matches_requested=100)
        matches = page["matches"]
        if not matches:
            return n
        save_page(seq, page)
        seq = matches[-1]["match_seq_num"] + 1
    return pages
