"""Valve Steam Web API (needs STEAM_API_KEY)."""

import os

import requests

API = "https://api.steampowered.com/IDOTA2Match_570"
ALL_PICK = 22  # game_mode; Turbo is 23
RANKED = 7  # lobby_type
MAX_PAGES = 20  # safety cap, 100 matches per page


def call(method: str, **params) -> dict:
    r = requests.get(f"{API}/{method}/v1/", params={"key": os.environ["STEAM_API_KEY"], **params}, timeout=60)
    r.raise_for_status()
    return r.json()["result"]


def newest_seq() -> int:
    """match_seq_num of the newest match. Seq nums are assigned in the order matches END."""
    return call("GetMatchHistory", matches_requested=1)["matches"][0]["match_seq_num"]


def ranked_all_pick_from(seq: int, n: int) -> list[int]:
    """First n ranked All Pick match ids that ended at or after seq."""
    ids = []
    for _ in range(MAX_PAGES):
        matches = call("GetMatchHistoryBySequenceNum", start_at_match_seq_num=seq, matches_requested=100)["matches"]
        ids += [m["match_id"] for m in matches if m["game_mode"] == ALL_PICK and m["lobby_type"] == RANKED]
        if not matches or len(ids) >= n:
            break
        seq = matches[-1]["match_seq_num"] + 1
    return ids[:n]
