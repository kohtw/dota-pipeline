"""Steam match-summary lane: flattens raw GetMatchHistoryBySequenceNum pages (landed in Garage
under steam_raw/) into rows for stg.steam_matches and stg.steam_players. Flattening only: no
filtering and no business rules, those live in dbt. Loading goes through a
caller-supplied DB connection."""

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


def _execute_values(cur, sql: str, rows: list[tuple]) -> None:
    from psycopg2.extras import execute_values

    execute_values(cur, sql, rows)


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
