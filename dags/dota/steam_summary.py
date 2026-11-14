"""Steam match-summary lane: flattens raw GetMatchHistoryBySequenceNum pages (landed in Garage
under steam_raw/) into rows for stg.steam_matches, stg.steam_players and stg.steam_picks_bans.
Flattening only: no filtering and no business rules, those live in dbt. Loading goes through a
caller-supplied DB connection. Columns and exclusions: docs/steam-lane.md."""

import gzip
import json
import re
from datetime import datetime, timezone
from typing import Callable

from dota.warehouse import _execute_values

RAW_PREFIX = "steam_raw/"
LOADED_PREFIX = "steam_raw/_loaded/"  # one empty marker per page, written after that page's load commits
_PAGE = re.compile(r"^steam_raw/seq_(\d+)\.json\.gz$")

MATCH_COLUMNS = ("match_id", "match_seq_num", "start_time", "duration", "game_mode",
                 "lobby_type", "radiant_win", "human_players", "radiant_score", "dire_score",
                 "first_blood_time", "pre_game_duration", "cluster", "leagueid")
# account_id is left out on purpose: personal data the marts do not need
PLAYER_COLUMNS = ("player_slot", "hero_id", "kills", "deaths", "assists", "leaver_status",
                  "team_number", "last_hits", "denies", "gold_per_min", "xp_per_min", "level",
                  "net_worth", "hero_damage", "tower_damage", "hero_healing", "gold", "gold_spent",
                  "item_0", "item_1", "item_2", "item_3", "item_4", "item_5",
                  "backpack_0", "backpack_1", "backpack_2", "item_neutral")
PICK_BAN_COLUMNS = ("match_id", "ord", "is_pick", "hero_id", "team")


def save_page(start_seq: int, page: dict, put: Callable[[str, bytes], None]) -> str:
    """Write one raw page gzipped to steam_raw/seq_{start_seq}.json.gz and return the key."""
    key = f"{RAW_PREFIX}seq_{start_seq}.json.gz"
    put(key, gzip.compress(json.dumps(page).encode()))
    return key


def parse_page(page: dict) -> tuple[list[dict], list[dict]]:
    """One match row per match and one player row per listed player. Missing fields become None."""
    matches, players = [], []
    for m in page.get("matches", []):
        matches.append({c: m.get(c) for c in MATCH_COLUMNS})
        for p in m.get("players", []):
            players.append({"match_id": m.get("match_id"), **{c: p.get(c) for c in PLAYER_COLUMNS}})
    return matches, players


def parse_picks_bans(page: dict) -> list[dict]:
    """One row per picks_bans entry, with the entry's `order` as `ord`. Matches without picks_bans
    give no rows. Missing fields become None."""
    return [{"match_id": m.get("match_id"), "ord": pb.get("order"), "is_pick": pb.get("is_pick"),
             "hero_id": pb.get("hero_id"), "team": pb.get("team")}
            for m in page.get("matches", []) for pb in m.get("picks_bans") or []]


MATCH_ROW = MATCH_COLUMNS + ("source_page", "loaded_at")
MATCH_SQL = "INSERT INTO stg.steam_matches (" + ", ".join(MATCH_ROW) + ") VALUES %s"
PLAYER_KEY = ("match_id", "player_slot")
PLAYER_ROW = PLAYER_KEY + PLAYER_COLUMNS[1:]
PLAYER_SQL = "INSERT INTO stg.steam_players (" + ", ".join(PLAYER_ROW) + ") VALUES %s"
PICK_BAN_SQL = "INSERT INTO stg.steam_picks_bans (" + ", ".join(PICK_BAN_COLUMNS) + ") VALUES %s"


def load_steam(matches: list[dict], players: list[dict], conn, *, ddl: str, picks_bans=(),
               source_page: str | None = None, execute_values=_execute_values) -> int:
    """Loads parsed rows into stg.steam_matches / steam_players / steam_picks_bans in one
    transaction: run `ddl` (creates or upgrades the tables), dedupe by key within the batch (last
    wins), delete those match ids, insert. Re-running a batch replaces its rows. Returns matches loaded."""
    # naive UTC: the column is TIMESTAMP, and an aware value would be shifted by the session time zone
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    by_match = {m["match_id"]: m for m in matches}
    by_player = {(p["match_id"], p["player_slot"]): p for p in players}
    by_pick_ban = {(pb["match_id"], pb["ord"]): pb for pb in picks_bans}
    match_rows = [tuple(m[c] for c in MATCH_COLUMNS) + (source_page, loaded_at) for m in by_match.values()]
    player_rows = [tuple(p[c] for c in PLAYER_ROW) for p in by_player.values()]
    pick_ban_rows = [tuple(pb[c] for c in PICK_BAN_COLUMNS) for pb in by_pick_ban.values()]
    ids = list(by_match)
    cur = conn.cursor()
    try:
        cur.execute(ddl)
        cur.execute("DELETE FROM stg.steam_picks_bans WHERE match_id = ANY(%s)", (ids,))
        cur.execute("DELETE FROM stg.steam_players WHERE match_id = ANY(%s)", (ids,))
        cur.execute("DELETE FROM stg.steam_matches WHERE match_id = ANY(%s)", (ids,))
        if match_rows:
            execute_values(cur, MATCH_SQL, match_rows)
        if player_rows:
            execute_values(cur, PLAYER_SQL, player_rows)
        if pick_ban_rows:
            execute_values(cur, PICK_BAN_SQL, pick_ban_rows)
        conn.commit()
        return len(match_rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def loaded_marker_key(page_key: str) -> str:
    """Marker key for a page: steam_raw/_loaded/{page_basename}."""
    return LOADED_PREFIX + page_key.rsplit("/", 1)[-1]


def unloaded_pages(page_keys: list[str], loaded_keys: list[str]) -> list[str]:
    """Page keys (steam_raw/seq_N.json.gz) with no loaded marker, ordered by seq number so that
    on overlapping pages the later one wins. Other keys under steam_raw/ (markers) are ignored."""
    done = set(loaded_keys)
    pages = [(int(m.group(1)), k) for k in page_keys
             if (m := _PAGE.match(k)) and loaded_marker_key(k) not in done]
    return [k for _, k in sorted(pages)]


def load_pages_isolated(page_keys: list[str], read: Callable[[str], bytes],
                        load: Callable[..., object],
                        mark: Callable[[str], None]) -> tuple[list[str], dict[str, str]]:
    """Load each page in order (read, gunzip, parse, load), and mark it only after `load` returns.
    A failed page is left unmarked for the next run and does not stop the others.
    Returns (loaded keys, {failed key: error text})."""
    loaded: list[str] = []
    failed: dict[str, str] = {}
    for key in page_keys:
        try:
            page = json.loads(gzip.decompress(read(key)))
            matches, players = parse_page(page)
            load(matches, players, picks_bans=parse_picks_bans(page), source_page=key)
            mark(key)
        except Exception as err:  # system boundary: one bad page (corrupt file, DB error) must not block the lane
            print(f"page {key} failed: {type(err).__name__}: {err}")
            failed[key] = f"{type(err).__name__}: {err}"
        else:
            loaded.append(key)
    return loaded, failed


def failure_message(result: dict | None) -> str | None:
    """The error to raise for a load_pages result, or None when there is nothing to report. A None
    result means load_pages itself failed: its own failure already fails the run and alerts."""
    if not result or not result["failed"]:
        return None
    return f"{len(result['failed'])} Steam pages failed: {result['failed']}"
