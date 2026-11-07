"""OpenDota: source of replay_url (needs the replay salt, which Valve's API doesn't give)."""

import requests

API = "https://api.opendota.com/api"


def replay_url(match_id: int) -> str:
    """Valve download URL for the replay. If OpenDota doesn't have the salt yet, ask it
    to fetch and raise, so the caller can retry later."""
    r = requests.get(f"{API}/matches/{match_id}", timeout=30)
    r.raise_for_status()
    if url := r.json().get("replay_url"):
        return url
    requests.post(f"{API}/request/{match_id}", timeout=30)
    raise RuntimeError(f"no replay_url for {match_id} yet; requested OpenDota fetch")
