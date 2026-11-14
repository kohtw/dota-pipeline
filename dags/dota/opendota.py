"""OpenDota: source of replay_url (needs the replay salt, which Valve's API doesn't give)."""

import time

import requests

from dota.errors import PermanentError, is_transient_status, redact

API = "https://api.opendota.com/api"
# The free tier allows 60 calls a minute; 50 leaves headroom. Each task runs in its own process, so
# this spaces calls only together with the opendota_api pool's single slot.
PACE_S = 60 / 50


def _call(method, url: str):
    try:
        return method(url, timeout=30)
    finally:
        # after the call, so the gap also holds before the next task's first call
        time.sleep(PACE_S)


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
        _call(requests.post, f"{API}/request/{match_id}")
    except requests.RequestException as e:
        raise RuntimeError(redact(f"OpenDota match {match_id} failed: {e}")) from None
    raise RuntimeError(f"no replay_url for {match_id} yet; requested OpenDota fetch")
