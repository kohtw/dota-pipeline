"""Steam match-summary lane: flattens raw GetMatchHistoryBySequenceNum pages (landed in Garage
under steam_raw/) into rows for stg.steam_matches and stg.steam_players. Flattening only: no
filtering and no business rules, those live in dbt. Loading goes through a
caller-supplied DB connection."""

import gzip
import json
import re
from typing import Callable

from dota.warehouse import _execute_values

RAW_PREFIX = "steam_raw/"
LOADED_PREFIX = "steam_raw/_loaded/"  # one empty marker per page, written after that page's load commits
_PAGE = re.compile(r"^steam_raw/seq_(\d+)\.json\.gz$")

MATCH_COLUMNS = ("match_id", "match_seq_num", "start_time", "duration", "game_mode",
                 "lobby_type", "radiant_win", "human_players")
PLAYER_COLUMNS = ("player_slot", "hero_id", "kills", "deaths", "assists", "leaver_status")


def parse_page(page: dict) -> tuple[list[dict], list[dict]]:
    """One match row per match and one player row per listed player. Missing fields become None."""
    matches, players = [], []
    for m in page.get("matches", []):
        matches.append({c: m.get(c) for c in MATCH_COLUMNS})
        for p in m.get("players", []):
            players.append({"match_id": m.get("match_id"), **{c: p.get(c) for c in PLAYER_COLUMNS}})
    return matches, players


MATCH_SQL = "INSERT INTO stg.steam_matches (" + ", ".join(MATCH_COLUMNS) + ") VALUES %s"
PLAYER_KEY = ("match_id", "player_slot")
PLAYER_ROW = PLAYER_KEY + PLAYER_COLUMNS[1:]
PLAYER_SQL = "INSERT INTO stg.steam_players (" + ", ".join(PLAYER_ROW) + ") VALUES %s"


def load_steam(matches: list[dict], players: list[dict], conn, *, ddl: str,
               execute_values=_execute_values) -> int:
    """Loads parsed rows into stg.steam_matches / stg.steam_players in one transaction: run `ddl`
    (creates the tables if missing), dedupe by key within the batch (last wins), delete those
    match ids, insert. Re-running a batch replaces its rows. Returns matches loaded."""
    by_match = {m["match_id"]: m for m in matches}
    by_player = {(p["match_id"], p["player_slot"]): p for p in players}
    match_rows = [tuple(m[c] for c in MATCH_COLUMNS) for m in by_match.values()]
    player_rows = [tuple(p[c] for c in PLAYER_ROW) for p in by_player.values()]
    ids = list(by_match)
    cur = conn.cursor()
    try:
        cur.execute(ddl)
        cur.execute("DELETE FROM stg.steam_players WHERE match_id = ANY(%s)", (ids,))
        cur.execute("DELETE FROM stg.steam_matches WHERE match_id = ANY(%s)", (ids,))
        if match_rows:
            execute_values(cur, MATCH_SQL, match_rows)
        if player_rows:
            execute_values(cur, PLAYER_SQL, player_rows)
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
                        load: Callable[[list[dict], list[dict]], object],
                        mark: Callable[[str], None]) -> tuple[list[str], dict[str, str]]:
    """Loads each page in order: read -> gunzip -> parse_page -> load, then mark it loaded (only after
    `load` returned). One page failing does not stop the others. Returns (loaded keys, {failed key:
    error text}); a failed page is left unmarked so the next run retries it."""
    loaded: list[str] = []
    failed: dict[str, str] = {}
    for key in page_keys:
        try:
            matches, players = parse_page(json.loads(gzip.decompress(read(key))))
            load(matches, players)
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
