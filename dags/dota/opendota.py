"""OpenDota: source of replay_url (needs the replay salt, which Valve's API doesn't give) and of
the game mode list."""

import time

import requests

from dota.errors import PermanentError, is_transient_status, redact

API = "https://api.opendota.com/api"
# The free tier allows 60 calls a minute; 50 leaves headroom. Each task runs in its own process, so
# this spaces calls only together with the opendota_api pool's single slot.
PACE_S = 60 / 50
# OpenDota counts calls per IP, so another client on the same network can use up the minute. Holding
# the pool's only slot while waiting pauses every OpenDota task, instead of each retrying into it.
RATE_LIMIT_WAIT_S = 300
RATE_LIMIT_TRIES = 3


def _wait_s(r: requests.Response) -> int:
    value = r.headers.get("Retry-After", "")
    return int(value) if value.isdecimal() else RATE_LIMIT_WAIT_S


def _call(method, url: str) -> requests.Response:
    """One paced call. On HTTP 429 it waits (Retry-After, else 5 min) and tries again, up to
    RATE_LIMIT_TRIES times; the last response is returned whatever its status."""
    for attempt in range(RATE_LIMIT_TRIES):
        try:
            r = method(url, timeout=30)
        finally:
            # after the call, so the gap also holds before the next task's first call
            time.sleep(PACE_S)
        if r.status_code != 429 or attempt == RATE_LIMIT_TRIES - 1:
            return r
        time.sleep(_wait_s(r))


def game_modes() -> dict:
    """OpenDota's /constants/game_mode, the list of game mode ids (Valve's datafeed has none)."""
    try:
        r = _call(requests.get, f"{API}/constants/game_mode")
    except requests.RequestException as e:
        raise RuntimeError(redact(f"OpenDota game modes failed: {e}")) from None
    if not r.ok:
        problem = f"OpenDota game modes failed: HTTP {r.status_code}"
        raise RuntimeError(problem) if is_transient_status(r.status_code) else PermanentError(problem)
    return r.json()


def replay_url(match_id: int) -> str:
    """Valve download URL for the replay. If OpenDota doesn't have the salt yet, ask it
    to fetch and raise, so the caller can retry later. Other 4xx raise PermanentError."""
    try:
        r = _call(requests.get, f"{API}/matches/{match_id}")
        if is_transient_status(r.status_code):
            raise RuntimeError(f"OpenDota match {match_id} failed: HTTP {r.status_code}")
        # OpenDota answers 404 for a match it has not fetched yet, which a fetch request fixes
        if r.status_code != 404:
            if not r.ok:
                raise PermanentError(f"OpenDota match {match_id} failed: HTTP {r.status_code}")
            if url := r.json().get("replay_url"):
                return url
        fetch = _call(requests.post, f"{API}/request/{match_id}")
        if not fetch.ok:
            problem = f"OpenDota fetch request for {match_id} failed: HTTP {fetch.status_code}"
            raise RuntimeError(problem) if is_transient_status(fetch.status_code) else PermanentError(problem)
    except requests.RequestException as e:
        raise RuntimeError(redact(f"OpenDota match {match_id} failed: {e}")) from None
    raise RuntimeError(f"no replay_url for {match_id} yet; requested OpenDota fetch")
