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
    assert antimage["hero_label"] == "Anti-Mage"
    assert antimage["kills"] == 2
    assert antimage["net_worth"] == 6000
    assert antimage["gold"] == 400
    assert antimage["main_inventory"] == ["Blink Dagger", "Desolator"]
    assert antimage["kill_participation"] == pytest.approx(2 / 3)
    assert players[1]["main_inventory"] is None
    query, params = scoreboard_cursor.execute.call_args.args
    assert "WHERE s.match_id = %s" in query
    assert params == (MATCH_ID,)
    dashboard_database.open_postgres_database.assert_called_once_with(warehouse_config)


@pytest.mark.parametrize("raw, label, expected", [
    ("npc_dota_hero_obsidian_destroyer", "Outworld Destroyer", "Outworld Destroyer"),
    ("npc_dota_hero_ringmaster", "Ringmaster", "Ringmaster"),
    ("npc_dota_hero_unknown_hero", None, "Unknown Hero"),
    ("npc_dota_hero_unknown_hero", "  ", "Unknown Hero"),
])
def test_scoreboard_uses_valve_hero_names_with_unknown_fallback(
    dashboard_database, warehouse_config, scoreboard_cursor, scoreboard_rows, raw, label, expected,
):
    scoreboard_rows[0].update(hero=raw, hero_label=label)
    scoreboard_cursor.fetchall.return_value = [tuple(row.values()) for row in scoreboard_rows]
    _, players = dashboard_database.load_scoreboard(warehouse_config, MATCH_ID)
    assert players[0]["hero"] == raw
    assert players[0]["hero_label"] == expected


@pytest.mark.parametrize("labels, expected", [
    (["Boots of Speed", "Boots of Speed", "Eye of Skadi"], ["Boots of Speed", "Boots of Speed", "Eye of Skadi"]),
    (["item_unknown_relic", "item_recipe_unlabeled"], ["Unknown Relic", "Recipe Unlabeled"]),
    (["Label, with comma", "Io's Item"], ["Label, with comma", "Io's Item"]),
    ([], []), (None, None),
])
def test_inventory_keeps_valve_labels_order_duplicates_and_unknown_fallback(
    dashboard_database, warehouse_config, scoreboard_cursor, scoreboard_rows, labels, expected,
):
    scoreboard_rows[0]["final_items"] = labels
    scoreboard_cursor.fetchall.return_value = [tuple(row.values()) for row in scoreboard_rows]
    _, players = dashboard_database.load_scoreboard(warehouse_config, MATCH_ID)
    assert players[0]["main_inventory"] == expected


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


@pytest.mark.parametrize("comparison_slot", [None, 5])
def test_inventory_loads_selected_slots_without_padding_or_losing_duplicates(
    dashboard_database, warehouse_config, warehouse_cursor, comparison_slot,
):
    rows = [
        (0, 0, "main", "item_blink", "Blink Dagger"),
        (0, 6, "backpack", "item_branches", "Iron Branch"),
        (0, 7, "backpack", "item_branches", "Iron Branch"),
        (0, 9, "stash", "item_unknown", "unknown"),
        (0, 15, "teleport", "item_tpscroll", "Town Portal Scroll"),
        (0, 16, "neutral", "item_faded_broach", "Faded Broach"),
    ]
    if comparison_slot is not None:
        rows.append((5, 2, "main", "item_boots", "Boots of Speed"))
    warehouse_cursor.fetchall.return_value = rows
    result = dashboard_database.load_player_inventory(warehouse_config, MATCH_ID, 0, comparison_slot)
    assert set(result) == ({0} if comparison_slot is None else {0, 5})
    assert [item["inventory_slot"] for item in result[0]] == [0, 6, 7, 9, 15, 16]
    assert result[0][0] == dict(inventory_slot=0, location="main", item_name="item_blink",
                              item_display_name="Blink Dagger")
    assert [item["item_display_name"] for item in result[0]][1:4] == ["Iron Branch", "Iron Branch", "unknown"]
    if comparison_slot is not None:
        assert result[5][0]["inventory_slot"] == 2
    query, params = warehouse_cursor.execute.call_args.args
    assert "FROM marts.match_inventory" in query
    assert "WHERE match_id = %s AND player_slot IN (%s, %s)" in query
    assert "ORDER BY player_slot, inventory_slot" in query
    assert params == (MATCH_ID, 0, comparison_slot)
    dashboard_database.open_postgres_database.assert_called_once_with(warehouse_config)


def test_inventory_does_not_treat_missing_rows_as_confirmed_empty(
    dashboard_database, warehouse_config, warehouse_cursor,
):
    warehouse_cursor.fetchall.return_value = []
    assert dashboard_database.load_player_inventory(warehouse_config, MATCH_ID, 0, 5) == {0: None, 5: None}
    warehouse_cursor.fetchall.return_value = [(5, 0, "main", "item_blink", "Blink Dagger")]
    result = dashboard_database.load_player_inventory(warehouse_config, MATCH_ID, 0, 5)
    assert result[0] is None
    assert len(result[5]) == 1


@pytest.mark.parametrize("row", [
    (0, 17, "neutral", "item_test", "Test"),
    (0, 6, "main", "item_test", "Test"),
    (1, 0, "main", "item_test", "Test"),
    (0, 0, "main", None, "Test"),
    (0, 0, "main", "item_test", ""),
])
def test_inventory_rejects_invalid_records(dashboard_database, warehouse_config, warehouse_cursor, row):
    warehouse_cursor.fetchall.return_value = [row]
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="invalid inventory"):
        dashboard_database.load_player_inventory(warehouse_config, MATCH_ID, 0, None)


def test_inventory_rejects_duplicate_slots(dashboard_database, warehouse_config, warehouse_cursor):
    warehouse_cursor.fetchall.return_value = [(0, 0, "main", "item_blink", "Blink Dagger")] * 2
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="duplicate inventory"):
        dashboard_database.load_player_inventory(warehouse_config, MATCH_ID, 0, None)


def test_inventory_propagates_database_errors(dashboard_database, warehouse_config, warehouse_cursor):
    warehouse_cursor.execute.side_effect = dashboard_database.DashboardDatabaseError("missing mart")
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="missing mart"):
        dashboard_database.load_player_inventory(warehouse_config, MATCH_ID, 0, None)
