"""Opt-in, read-only integration checks against an already populated warehouse."""

import os
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(
    not os.environ.get("DASHBOARD_TEST_MATCH_ID"),
    reason="Set DASHBOARD_TEST_MATCH_ID and WAREHOUSE_* to test a populated Postgres warehouse.",
)


def test_live_loaders_agree_with_warehouse_marts(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import open_postgres_database

    config = load_warehouse_config()
    match_id = int(os.environ["DASHBOARD_TEST_MATCH_ID"])
    match, players = dashboard_database.load_scoreboard(config, match_id)
    assert players, "The selected integration match must already exist in the scoreboard mart"
    assert match["match_id"] == match_id
    assert all(p["team_kills"] is None or type(p["team_kills"]) is int for p in players)
    primary, comparison = players[0]["slot"], players[-1]["slot"]

    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT min(match_id) FROM marts.match_scoreboard")
        expected_default = cursor.fetchone()[0]
        cursor.execute("SELECT DISTINCT match_id FROM marts.match_scoreboard ORDER BY match_id")
        expected_examples = [row[0] for row in cursor.fetchall()][:5]
        cursor.execute(
            "SELECT player_slot,kills,deaths,assists,net_worth,unspent_gold "
            "FROM marts.match_scoreboard WHERE match_id=%s ORDER BY player_slot", (match_id,),
        )
        expected_scoreboard = cursor.fetchall()
        cursor.execute(
            "SELECT player_slot,game_time_s,net_worth,total_earned_xp "
            "FROM marts.player_timeseries WHERE match_id=%s AND player_slot IN (%s,%s) "
            "ORDER BY player_slot,game_time_s,tick", (match_id, primary, comparison),
        )
        expected_snapshots = cursor.fetchall()
        cursor.execute(
            "SELECT attacker_slot,count(*),sum(total_damage) FROM marts.damage_by_pair "
            "WHERE match_id=%s AND attacker_slot IN (%s,%s) GROUP BY attacker_slot",
            (match_id, primary, comparison),
        )
        expected_damage = cursor.fetchall()
        cursor.execute(
            "SELECT s.player_slot,s.hero_name,s.team,s.won,s.final_items,k.kill_participation,"
            "CAST(k.team_kills AS BIGINT),k.kills_from_events+k.assists_from_events,h.localized_name "
            "FROM marts.match_scoreboard s LEFT JOIN marts.kill_participation k "
            "ON k.match_id=s.match_id AND k.player_slot=s.player_slot "
            "LEFT JOIN marts.dim_heroes h ON h.npc_name=s.hero_name "
            "WHERE s.match_id=%s ORDER BY s.player_slot", (match_id,),
        )
        expected_details = cursor.fetchall()

    assert dashboard_database.load_default_match_id(config) == expected_default
    assert dashboard_database.load_available_matches(config) == expected_examples
    assert [(p["slot"], p["kills"], p["deaths"], p["assists"], p["net_worth"], p["gold"])
            for p in players] == expected_scoreboard
    for player, (slot, hero, team, won, items, kp, team_kills, involvements, hero_name) in zip(players, expected_details):
        assert (player["slot"], player["hero"], player["team"]) == (slot, hero, team)
        assert player["hero_label"] == (hero_name.strip() if hero_name and hero_name.strip() else
                                        hero.removeprefix('npc_dota_hero_').replace('_', ' ').title())
        assert player["kill_participation"] == kp
        assert player["team_kills"] == team_kills
        assert player["kill_involvements"] == involvements
        # Check the seed mapping against source inventory in its original slot order.
        if items is None:
            assert player["main_inventory"] is None
        else:
            with open_postgres_database(config) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT i.item_name, d.localized_name FROM clean.final_inventory i "
                    "LEFT JOIN marts.dim_items d ON d.name=i.item_name "
                    "WHERE i.match_id=%s AND i.player_slot=%s AND i.inventory_slot BETWEEN 0 AND 5 "
                    "ORDER BY i.inventory_slot", (match_id, slot),
                )
                expected_labels = [name.strip() if name and name.strip() else
                                   raw.removeprefix('item_').replace('_', ' ').title()
                                   for raw, name in cursor.fetchall()]
            assert player["main_inventory"] == expected_labels
        if won is True:
            assert match["winner_team"] == team

    snapshots = dashboard_database.load_player_snapshots(config, match_id, primary, comparison)
    assert [(slot, row["game_time_s"], row["net_worth"], row["total_xp"])
            for slot in sorted(snapshots) for row in snapshots[slot]] == expected_snapshots
    primary_only = dashboard_database.load_player_snapshots(config, match_id, primary, None)
    assert set(primary_only) <= {primary}
    assert primary_only.get(primary, []) == snapshots.get(primary, [])

    damage = dashboard_database.load_player_damage(config, match_id, primary, comparison)
    assert {slot: (len(rows), sum(r["total_damage"] for r in rows)) for slot, rows in damage.items()} == {
        slot: (count, total) for slot, count, total in expected_damage
    }
    _, unavailable = dashboard_database.load_scoreboard(config, -1)
    assert unavailable == []


def test_live_inventory_loader_preserves_exact_slots_and_player_selection(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import open_postgres_database

    config = load_warehouse_config()
    match_id = int(os.environ["DASHBOARD_TEST_MATCH_ID"])
    _, players = dashboard_database.load_scoreboard(config, match_id)
    primary, comparison = players[0]["slot"], players[-1]["slot"]
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT player_slot,inventory_slot,location,item_name,item_display_name "
            "FROM marts.match_inventory WHERE match_id=%s AND player_slot IN (%s,%s) "
            "ORDER BY player_slot,inventory_slot", (match_id, primary, comparison),
        )
        expected = cursor.fetchall()
    inventory = dashboard_database.load_player_inventory(config, match_id, primary, comparison)
    assert set(inventory) == {primary, comparison}
    assert [(slot, item["inventory_slot"], item["location"], item["item_name"], item["item_display_name"])
            for slot in sorted(inventory) for item in inventory[slot] or []] == expected
    single = dashboard_database.load_player_inventory(config, match_id, primary)
    assert single == {primary: inventory[primary]}
    assert dashboard_database.load_player_inventory(config, -1, primary, comparison) == {
        primary: None, comparison: None,
    }


def test_live_connection_limits_and_error_cleanup(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import DashboardDatabaseError, open_postgres_database

    config = load_warehouse_config()
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute("SHOW transaction_read_only")
        assert cursor.fetchone()[0] == "on"
        cursor.execute("SHOW statement_timeout")
        assert cursor.fetchone()[0] == "15s"
    assert connection.closed
    assert cursor.closed

    with pytest.raises(DashboardDatabaseError, match="missing or out of date"):
        with open_postgres_database(config) as missing, missing.cursor() as cursor:
            cursor.execute("SELECT missing_dashboard_probe FROM marts.match_scoreboard LIMIT 0")
    assert missing.closed
    assert cursor.closed

    with pytest.raises(DashboardDatabaseError, match="time limit"):
        with open_postgres_database(config) as cancelled, cancelled.cursor() as cursor:
            # Limit only this transaction so the timeout check cannot affect other sessions.
            cursor.execute("SET LOCAL statement_timeout = '50ms'")
            cursor.execute("SELECT pg_sleep(0.2)")
    assert cancelled.closed
    assert cursor.closed


def test_live_hero_loader_reads_the_current_mart(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import open_postgres_database

    config = load_warehouse_config()
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT hero_id,picks,wins,ranked_matches FROM marts.hero_pick_win_rate ORDER BY hero_id")
        expected_counts = cursor.fetchall()
    heroes = dashboard_database.load_hero_statistics(config)
    assert [(hero["hero_id"], hero["picks"], hero["wins"], hero["ranked_matches"])
            for hero in sorted(heroes, key=lambda hero: hero["hero_id"])] == expected_counts
    if heroes:
        assert len({hero["ranked_matches"] for hero in heroes}) == 1


def test_live_app_renders_players_comparison_and_hero_route(dashboard_database):
    testing = pytest.importorskip("streamlit.testing.v1")
    from config import load_warehouse_config

    match_id = int(os.environ["DASHBOARD_TEST_MATCH_ID"])
    _, players = dashboard_database.load_scoreboard(load_warehouse_config(), match_id)
    assert len(players) >= 2
    app = testing.AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))
    app.query_params["match_id"] = str(match_id)
    app.query_params["player_slot"] = str(players[0]["slot"])
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.title[0].value == players[0]["hero_label"]
    inventories = dashboard_database.load_player_inventory(
        load_warehouse_config(), match_id, players[0]["slot"], players[-1]["slot"],
    )

    def expected_loadout(slot):
        records = inventories[slot]
        if not records:
            return []
        labels = {row["inventory_slot"]: row["item_display_name"] for row in records}
        order = [*range(9), 16, 15, *range(9, 15)]
        return [labels.get(index, "Not recorded") for index in order]

    assert "Final inventory" in [heading.value for heading in app.subheader]
    assert [text.value for text in app.text][1:] == expected_loadout(players[0]["slot"])
    assert all(not expander.proto.expanded for expander in app.expander if expander.label == "Stash")

    app.selectbox[0].select(players[-1]["slot"]).run(timeout=30)
    assert not app.exception
    assert not app.error
    stats = app.table[0].value.set_index("Metric")
    assert stats.loc["Kills"].tolist() == [str(players[0]["kills"]), str(players[-1]["kills"])]
    inventory_text = [text.value for text in app.text]
    first_loadout = expected_loadout(players[0]["slot"])
    assert inventory_text[1:1 + len(first_loadout)] == first_loadout
    assert inventory_text[2 + len(first_loadout):] == expected_loadout(players[-1]["slot"])
    app.button(key="player_back").click().run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.title[0].value == "Match overview"

    examples = dashboard_database.load_available_matches(load_warehouse_config())
    assert examples
    app.button(key=f"example_match_{examples[0]}").click().run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.query_params["match_id"] == str(examples[0])
    assert app.text_input(key="match_search_input").value == str(examples[0])
    assert app.title[0].value == "Match overview"

    app.text_input(key="match_search_input").set_value(str(match_id))
    next(button for button in app.button if button.label == "View match").click().run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.query_params["match_id"] == str(match_id)
    assert app.query_params["view"] == "matches"
    assert app.title[0].value == "Match overview"

    # The actual hero mart may still be empty; the live page must reflect its current availability.
    heroes = dashboard_database.load_hero_statistics(load_warehouse_config())
    app.radio(key="dashboard_view").set_value("heroes").run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.title[0].value == "Hero statistics"
    if heroes:
        assert app.selectbox(key="hero_stats_selected_id").value in {hero["hero_id"] for hero in heroes}
        comparison = next(frame.value for frame in app.dataframe if "Hero" in frame.value.columns)
        assert len(comparison) == len(heroes)
    else:
        assert app.info[0].value == "No hero statistics available yet."
        assert not app.metric
