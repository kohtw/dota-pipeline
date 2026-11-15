"""Valve's dota2.com datafeed: the public backend of the Dota 2 website, no key needed. Source of the
hero, item and patch reference data that dota.constants parses."""

import time
from typing import Callable

import requests

from dota.errors import PermanentError, is_transient_status

API = "https://www.dota2.com/datafeed"
# herodata is one request per hero, and this is a public website backend, so go slowly
PAUSE_S = 0.5
ATTEMPTS = 3


def get(resource: str, *, sleep: Callable[[float], None] = time.sleep, **params) -> dict:
    """One datafeed resource in English. Retries 429, 5xx and network errors with a short backoff;
    other 4xx raise PermanentError."""
    for attempt in range(ATTEMPTS):
        try:
            r = requests.get(f"{API}/{resource}", params={"language": "english", **params}, timeout=30)
        except requests.RequestException as e:
            problem = str(e)
        else:
            if r.ok:
                return r.json()
            problem = f"HTTP {r.status_code}"
            if not is_transient_status(r.status_code):
                raise PermanentError(f"datafeed {resource} failed: {problem}")
        if attempt < ATTEMPTS - 1:
            sleep(10 * 2 ** attempt)
    raise RuntimeError(f"datafeed {resource} failed after {ATTEMPTS} attempts: {problem}")


def fetch(*, sleep: Callable[[float], None] = time.sleep) -> dict:
    """The raw responses: herolist, herodata for every hero (keyed by hero id as a string, as JSON
    keys are), itemlist and patchnoteslist."""
    herolist = get("herolist", sleep=sleep)
    herodata = {}
    for hero in herolist["result"]["data"]["heroes"]:
        sleep(PAUSE_S)
        herodata[str(hero["id"])] = get("herodata", hero_id=hero["id"], sleep=sleep)
    return {
        "herolist": herolist,
        "herodata": herodata,
        "itemlist": get("itemlist", sleep=sleep),
        "patchnoteslist": get("patchnoteslist", sleep=sleep),
    }
