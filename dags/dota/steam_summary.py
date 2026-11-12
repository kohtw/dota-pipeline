"""Steam match-summary lane: flattens raw GetMatchHistoryBySequenceNum pages (landed in Garage
under steam_raw/) into rows for stg.steam_matches and stg.steam_players. Flattening only: no
filtering and no business rules, those live in dbt. psycopg2 is only touched through `conn`."""

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
