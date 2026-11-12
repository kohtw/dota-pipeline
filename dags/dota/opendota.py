"""OpenDota: source of replay_url (needs the replay salt, which Valve's API doesn't give)."""

import requests

from dota.errors import PermanentError, is_transient_status, redact

API = "https://api.opendota.com/api"


def replay_url(match_id: int) -> str:
    """Valve download URL for the replay. If OpenDota doesn't have the salt yet, ask it
    to fetch and raise, so the caller can retry later. Other 4xx raise PermanentError."""
    try:
        r = requests.get(f"{API}/matches/{match_id}", timeout=30)
        if is_transient_status(r.status_code):
            raise RuntimeError(f"OpenDota match {match_id} failed: HTTP {r.status_code}")
        # OpenDota answers 404 for a match it has not fetched yet, which a fetch request fixes
        if r.status_code != 404:
            if not r.ok:
                raise PermanentError(f"OpenDota match {match_id} failed: HTTP {r.status_code}")
            if url := r.json().get("replay_url"):
                return url
        requests.post(f"{API}/request/{match_id}", timeout=30)
    except requests.RequestException as e:
        raise RuntimeError(redact(f"OpenDota match {match_id} failed: {e}")) from None
    raise RuntimeError(f"no replay_url for {match_id} yet; requested OpenDota fetch")
