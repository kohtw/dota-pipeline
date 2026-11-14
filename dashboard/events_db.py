"""Load event-level replay marts: deaths, fights, positions, purchases, lanes and damage by minute.

Match loaders return plain records for one match; the replay-wide loaders aggregate the same marts
across every parsed match for the Replay insights section. Loaders check identifiers before
querying and reject records that break the marts' basic rules, so the views never draw a
silently wrong map or timeline.
"""

from decimal import Decimal
from math import isfinite

from config import WarehouseConfig
from postgres_connection import DashboardDatabaseError, open_postgres_database


TEAMS = (2, 3)


def _plain(value):
    """Postgres returns sums of bigints and averages as Decimal; the views need int or float."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _query(config: WarehouseConfig, sql: str, params: tuple = ()) -> list[dict]:
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
    return [{column: _plain(value) for column, value in zip(columns, row, strict=True)} for row in rows]


def _check_match(match_id: int) -> None:
    if type(match_id) is not int or not 0 < match_id < 2**63:
        raise DashboardDatabaseError("Choose a valid match to load its events.")


def _is_slot(value) -> bool:
    return type(value) is int and 0 <= value <= 9


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def load_match_deaths(config: WarehouseConfig, match_id: int) -> list[dict]:
    """Every scoreboard death of one match in time order, with its corpse spot and fight."""
    _check_match(match_id)
    rows = _query(config, """
        SELECT tick, game_time_s, victim_slot, victim_team, credited_killer_slot, killer_team,
               killer_slot, killer_name, killer_kind, link_status, assist_count, assist_slots,
               x, y, respawn_s, dead_s, fight_number, is_first_blood
        FROM marts.match_deaths WHERE match_id = %s ORDER BY tick, victim_slot
        """, (match_id,))
    for row in rows:
        killer = row["credited_killer_slot"]
        if (not _is_slot(row["victim_slot"]) or row["victim_team"] not in TEAMS
                or not _is_number(row["game_time_s"])
                or (killer is not None and not _is_slot(killer))
                or type(row["fight_number"]) is not int or row["fight_number"] < 1
                or (row["x"] is None) != (row["y"] is None)):
            raise DashboardDatabaseError("The deaths mart contains invalid death records.")
        row["assists"] = [int(slot) for slot in row.pop("assist_slots").split(",")] if row["assist_slots"] else []
    return rows


def load_match_fights(config: WarehouseConfig, match_id: int) -> list[dict]:
    _check_match(match_id)
    rows = _query(config, """
        SELECT fight_number, start_s, end_s, duration_s, deaths, radiant_deaths, dire_deaths, kind,
               winning_team, center_x, center_y, radiant_hero_damage, dire_hero_damage, participants,
               advantage_before, advantage_after, advantage_swing
        FROM marts.match_fights WHERE match_id = %s ORDER BY fight_number
        """, (match_id,))
    for row in rows:
        if (row["deaths"] != row["radiant_deaths"] + row["dire_deaths"] or row["deaths"] < 1
                or row["winning_team"] not in (None, *TEAMS)
                or not row["start_s"] <= row["end_s"]):
            raise DashboardDatabaseError("The fights mart contains invalid fight records.")
    return rows


def load_fight_players(config: WarehouseConfig, match_id: int) -> list[dict]:
    _check_match(match_id)
    rows = _query(config, """
        SELECT fight_number, player_slot, team, kills, deaths, assists, damage_dealt, damage_taken
        FROM marts.match_fight_players WHERE match_id = %s ORDER BY fight_number, player_slot
        """, (match_id,))
    if any(not _is_slot(row["player_slot"]) or row["damage_dealt"] < 0 or row["damage_taken"] < 0 for row in rows):
        raise DashboardDatabaseError("The fight participation mart contains invalid records.")
    return rows


def load_damage_timeline(config: WarehouseConfig, match_id: int) -> list[dict]:
    _check_match(match_id)
    rows = _query(config, """
        SELECT player_slot, team, minute, damage_to_heroes, damage_to_buildings, damage_to_roshan,
               damage_to_units, damage_from_heroes, damage_from_units
        FROM marts.player_damage_timeline WHERE match_id = %s ORDER BY minute, player_slot
        """, (match_id,))
    for row in rows:
        values = [row[key] for key in row if key.startswith("damage_")]
        if not _is_slot(row["player_slot"]) or any(type(value) is not int or value < 0 for value in values):
            raise DashboardDatabaseError("The damage timeline mart contains invalid records.")
    return rows


def load_purchases(config: WarehouseConfig, match_id: int) -> list[dict]:
    _check_match(match_id)
    rows = _query(config, """
        SELECT player_slot, team, game_time_s, item_name, item_display_name, quantity, is_recipe,
               completed_item_name, completed_item_display_name, is_consumable, purchase_order
        FROM marts.player_purchases WHERE match_id = %s ORDER BY player_slot, purchase_order
        """, (match_id,))
    for row in rows:
        if (not _is_slot(row["player_slot"]) or not _is_number(row["game_time_s"])
                or not isinstance(row["item_display_name"], str) or type(row["quantity"]) is not int):
            raise DashboardDatabaseError("The purchases mart contains invalid records.")
    return rows


def load_player_lanes(config: WarehouseConfig, match_id: int) -> list[dict]:
    _check_match(match_id)
    rows = _query(config, """
        SELECT player_slot, team, lane, lane_share, lane_role, net_worth_10, xp_10, level_10,
               last_hits_10, denies_10, kills_10, deaths_10, assists_10
        FROM marts.player_lanes WHERE match_id = %s ORDER BY player_slot
        """, (match_id,))
    if any(not _is_slot(row["player_slot"]) or row["team"] not in TEAMS
           or row["lane"] not in (None, "top", "mid", "bot", "jungle", "roaming") for row in rows):
        raise DashboardDatabaseError("The lanes mart contains invalid records.")
    return rows


def load_match_positions(config: WarehouseConfig, match_id: int) -> list[dict]:
    """Each hero's position every five seconds, for the map replay and fight paths."""
    _check_match(match_id)
    rows = _query(config, """
        SELECT player_slot, time_s, x, y, is_alive
        FROM marts.match_positions WHERE match_id = %s ORDER BY time_s, player_slot
        """, (match_id,))
    if any(not _is_slot(row["player_slot"]) or type(row["time_s"]) is not int
           or not _is_number(row["x"]) or not _is_number(row["y"]) for row in rows):
        raise DashboardDatabaseError("The positions mart contains invalid records.")
    return rows


# Replay-wide aggregates for the Replay insights section.

def load_replay_overview(config: WarehouseConfig) -> dict:
    rows = _query(config, """
        SELECT (SELECT count(DISTINCT match_id) FROM marts.match_scoreboard) AS matches,
               (SELECT avg(duration_s) FROM (SELECT DISTINCT match_id, duration_s
                                             FROM marts.match_scoreboard) m) AS avg_duration_s,
               (SELECT count(*) FROM marts.match_deaths) AS deaths,
               (SELECT count(*) FROM marts.match_fights WHERE kind = 'teamfight') AS teamfights,
               (SELECT avg(game_time_s) FROM marts.match_deaths WHERE is_first_blood) AS avg_first_blood_s,
               (SELECT avg(dead_s) FROM marts.match_deaths) AS avg_dead_s
        """)
    return rows[0]


def load_deaths_by_minute(config: WarehouseConfig) -> list[dict]:
    """Deaths per game minute, with how many matches lasted into that minute."""
    return _query(config, """
        WITH matches AS (
            SELECT DISTINCT match_id, duration_s FROM marts.match_scoreboard
        ), deaths AS (
            SELECT CAST(floor(game_time_s / 60) AS integer) AS minute, count(*) AS deaths,
                   avg(dead_s) AS avg_dead_s
            FROM marts.match_deaths GROUP BY 1
        )
        SELECT d.minute, d.deaths, d.avg_dead_s,
               (SELECT count(*) FROM matches m WHERE m.duration_s >= d.minute * 60) AS matches_reaching
        FROM deaths d ORDER BY d.minute
        """)


def load_killer_kinds(config: WarehouseConfig) -> list[dict]:
    return _query(config, """
        SELECT killer_kind, count(*) AS deaths FROM marts.match_deaths
        GROUP BY killer_kind ORDER BY deaths DESC
        """)


def load_lead_outcomes(config: WarehouseConfig) -> list[dict]:
    """Radiant's net-worth lead at 10, 20 and 30 minutes and whether Radiant won, one row per match and time."""
    return _query(config, """
        WITH results AS (
            SELECT match_id, bool_and(won) FILTER (WHERE team = 2) AS radiant_won
            FROM marts.match_scoreboard GROUP BY match_id
        )
        SELECT t.match_id, t.observation_time_s, t.net_worth_advantage, r.radiant_won
        FROM marts.team_advantage_timeline t
        JOIN results r ON r.match_id = t.match_id
        WHERE t.observation_time_s IN (600, 1200, 1800) AND t.net_worth_advantage IS NOT NULL
          AND r.radiant_won IS NOT NULL
        """)


def load_lane_outcomes(config: WarehouseConfig) -> list[dict]:
    """Per match and lane, each side's net worth at 10:00 and whether that side won the match."""
    return _query(config, """
        WITH results AS (
            SELECT match_id, team, bool_and(won) AS won FROM marts.match_scoreboard GROUP BY 1, 2
        )
        SELECT l.match_id, l.lane, l.team, sum(l.net_worth_10) AS net_worth_10, count(*) AS players,
               count(l.net_worth_10) AS measured, r.won
        FROM marts.player_lanes l
        JOIN results r ON r.match_id = l.match_id AND r.team = l.team
        WHERE l.lane IN ('top', 'mid', 'bot')
        GROUP BY l.match_id, l.lane, l.team, r.won
        """)


def load_item_timing_heroes(config: WarehouseConfig) -> list[dict]:
    return _query(config, """
        SELECT hero_name, max(hero_localized_name) AS hero_localized_name, max(hero_matches) AS hero_matches
        FROM marts.hero_item_timings GROUP BY hero_name ORDER BY hero_matches DESC, hero_name
        """)


def load_hero_item_timings_by_id(config: WarehouseConfig, hero_id: int) -> list[dict]:
    """Replay item timings for one Steam hero ID (the Hero statistics page keys heroes by ID)."""
    if type(hero_id) is not int or not 0 < hero_id < 2**31:
        raise DashboardDatabaseError("Choose a valid hero to load its item timings.")
    return _query(config, """
        SELECT item_name, item_display_name, hero_matches, matches_bought, purchase_rate,
               median_first_purchase_s, fastest_first_purchase_s
        FROM marts.hero_item_timings WHERE hero_id = %s
        ORDER BY matches_bought DESC, median_first_purchase_s
        """, (hero_id,))


def load_hero_item_timings(config: WarehouseConfig, hero_name: str) -> list[dict]:
    if not isinstance(hero_name, str) or not hero_name.startswith("npc_dota_hero_"):
        raise DashboardDatabaseError("Choose a valid hero to load its item timings.")
    rows = _query(config, """
        SELECT item_name, item_display_name, hero_matches, matches_bought, purchase_rate,
               median_first_purchase_s, fastest_first_purchase_s
        FROM marts.hero_item_timings WHERE hero_name = %s
        ORDER BY matches_bought DESC, median_first_purchase_s
        """, (hero_name,))
    if any(not 0 < row["matches_bought"] <= row["hero_matches"] for row in rows):
        raise DashboardDatabaseError("The item timings mart contains invalid counts.")
    return rows
