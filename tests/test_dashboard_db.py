"""Check Postgres loader parameters and the records returned to dashboard views."""

import math

import pytest


MATCH_ID = 9000000001


def test_scoreboard_maps_mart_rows(dashboard_database, warehouse_config, scoreboard_cursor):
    match, players = dashboard_database.load_scoreboard(warehouse_config, MATCH_ID)

    assert match == {"match_id": MATCH_ID, "duration_s": 600, "winner_team": 2}
    assert [(p["slot"], p["team"]) for p in players] == [(0, 2), (1, 2), (5, 3), (6, 3)]
    antimage = players[0]
    assert antimage["hero"] == "npc_dota_hero_antimage"
    assert antimage["hero_label"] == "Antimage"
    assert antimage["kills"] == 2
    assert antimage["net_worth"] == 6000
    assert antimage["gold"] == 400
    assert antimage["main_inventory"] == ["Blink", "Desolator"]
    assert antimage["kill_participation"] == pytest.approx(2 / 3)
    assert players[1]["main_inventory"] is None
    query, params = scoreboard_cursor.execute.call_args.args
    assert "WHERE s.match_id = %s" in query
    assert params == (MATCH_ID,)
    dashboard_database.open_postgres_database.assert_called_once_with(warehouse_config)


def test_scoreboard_distinguishes_zero_and_unavailable_kp(
    dashboard_database, warehouse_config, scoreboard_cursor, scoreboard_rows,
):
    from components import format_percentage
    from player import _kill_participation_detail

    # Zero participation, a zero-kill team, and a missing join are different states.
    scoreboard_rows[0].update(kill_participation=0, kill_involvements=0)
    scoreboard_rows[1].update(kill_participation=None, team_kills=None, kill_involvements=None)
    scoreboard_rows[2].update(kill_participation=None, team_kills=0, kill_involvements=0)
    scoreboard_cursor.fetchall.return_value = [tuple(row.values()) for row in scoreboard_rows]

    _, players = dashboard_database.load_scoreboard(warehouse_config, MATCH_ID)
    by_slot = {p["slot"]: p for p in players}

    assert format_percentage(by_slot[0]["kill_participation"]) == "0.0%"
    assert _kill_participation_detail(by_slot[0]) == "0.0% · 0 of 3 team kills"
    assert _kill_participation_detail(by_slot[5]) == "— · Team had zero kills"
    assert _kill_participation_detail(by_slot[1]) == "— · Unavailable"


def test_unavailable_match_returns_no_players(dashboard_database, warehouse_config, scoreboard_cursor):
    scoreboard_cursor.fetchall.return_value = []
    match, players = dashboard_database.load_scoreboard(warehouse_config, MATCH_ID)
    assert players == []
    assert match == {"match_id": MATCH_ID, "duration_s": None, "winner_team": None}


@pytest.mark.parametrize("match_id", [None, MATCH_ID])
def test_default_match_handles_an_empty_mart(
    dashboard_database, warehouse_config, warehouse_cursor, match_id,
):
    warehouse_cursor.fetchone.return_value = (match_id,)
    assert dashboard_database.load_default_match_id(warehouse_config) == match_id


@pytest.mark.parametrize("rows", [[], [(9000000001,), (9000000002,)]])
def test_available_examples_use_a_bounded_distinct_scoreboard_query(
    dashboard_database, warehouse_config, warehouse_cursor, rows,
):
    warehouse_cursor.fetchall.return_value = rows
    assert dashboard_database.load_available_matches(warehouse_config) == [row[0] for row in rows]
    query = warehouse_cursor.execute.call_args.args[0]
    assert "SELECT DISTINCT match_id" in query
    assert "marts.match_scoreboard" in query
    assert "ORDER BY match_id" in query
    assert "LIMIT 5" in query


@pytest.mark.parametrize("comparison_slot", [None, 5])
def test_progression_binds_selection_and_preserves_missing_values(
    dashboard_database, warehouse_config, warehouse_cursor, comparison_slot,
):
    warehouse_cursor.fetchall.return_value = [(0, -2.0, None, 0), (0, 0.0, 600, 0)]
    result = dashboard_database.load_player_snapshots(warehouse_config, MATCH_ID, 0, comparison_slot)
    assert result[0] == [
        {"game_time_s": -2.0, "net_worth": None, "total_xp": 0},
        {"game_time_s": 0.0, "net_worth": 600, "total_xp": 0},
    ]
    query, params = warehouse_cursor.execute.call_args.args
    assert params == (MATCH_ID, 0, comparison_slot)
    assert "ORDER BY player_slot, game_time_s, tick" in query


def test_damage_keeps_sources_and_sanitizes_undefined_shares(
    dashboard_database, warehouse_config, warehouse_cursor,
):
    warehouse_cursor.fetchall.return_value = [
        (0, 5, 3, "dota_unknown", 300, 3, 0.5),
        (0, 6, 3, "item_test", 0, 1, math.nan),
        (5, 0, 2, "spell_test", 100, 1, None),
    ]
    result = dashboard_database.load_player_damage(warehouse_config, MATCH_ID, 0, 5)
    assert set(result) == {0, 5}
    assert result[0][0]["source"] == "dota_unknown"
    assert result[0][0]["total_damage"] == 300
    assert result[0][0]["share_of_team_damage"] == 0.5
    assert result[0][1]["share_of_team_damage"] is None
    assert result[5][0]["share_of_team_damage"] is None
    assert warehouse_cursor.execute.call_args.args[1] == (MATCH_ID, 0, 5)
