"""Event-level dashboard: loaders, chart builders, and the match, player and insight sections."""

import json
from decimal import Decimal
from importlib import import_module
from unittest.mock import MagicMock

import pytest

from test_dashboard_app import dashboard_app  # noqa: F401  (shared app fixture)


def death(tick, time, victim, team, killer, killer_team, fight, *, assists=(), kind="hero", x=0.0, y=0.0,
          dead=20.0, first=False, killer_slot="same"):
    return dict(
        tick=tick, game_time_s=time, victim_slot=victim, victim_team=team, credited_killer_slot=killer,
        killer_team=killer_team, killer_slot=killer if killer_slot == "same" else killer_slot,
        killer_name="npc_dota_goodguys_tower1_mid" if kind == "building" else "npc_dota_hero_x",
        killer_kind=kind, link_status="exact", assist_count=len(assists), assists=list(assists),
        x=x, y=y, respawn_s=None if dead is None else time + dead, dead_s=dead, fight_number=fight,
        is_first_blood=first,
    )


# Slots match the shared scoreboard fixture: Anti-Mage 0 and Crystal Maiden 1 (Radiant), Axe 5 and Lina 6 (Dire).
DEATHS = [
    death(4800, 100.0, 5, 3, 0, 2, 1, assists=[1], kind="building", killer_slot=None, x=-5050.0, y=5000.0, first=True),
    death(10800, 300.0, 1, 2, 5, 3, 2, x=100.0, y=200.0),
    death(10900, 303.0, 6, 3, 0, 2, 2, assists=[1], x=150.0, y=250.0, dead=None),
]
FIGHTS = [
    dict(fight_number=1, start_s=100.0, end_s=100.0, duration_s=0.0, deaths=1, radiant_deaths=0, dire_deaths=1,
         kind="pick-off", winning_team=2, center_x=-5050.0, center_y=5000.0, radiant_hero_damage=0,
         dire_hero_damage=0, participants=3, advantage_before=None, advantage_after=None, advantage_swing=None),
    dict(fight_number=2, start_s=300.0, end_s=303.0, duration_s=3.0, deaths=2, radiant_deaths=1, dire_deaths=1,
         kind="skirmish", winning_team=None, center_x=125.0, center_y=225.0, radiant_hero_damage=900,
         dire_hero_damage=700, participants=4, advantage_before=-200, advantage_after=300, advantage_swing=500),
]
FIGHT_PLAYERS = [
    dict(fight_number=2, player_slot=slot, team=2 if slot < 5 else 3, kills=kills, deaths=deaths, assists=assists,
         damage_dealt=dealt, damage_taken=taken)
    for slot, kills, deaths, assists, dealt, taken in ((0, 1, 0, 0, 600, 300), (1, 0, 1, 1, 300, 400),
                                                       (5, 1, 0, 0, 500, 500), (6, 0, 1, 0, 200, 400))
]
POSITIONS = [
    dict(player_slot=slot, time_s=time, x=x + time, y=y, is_alive=True)
    for slot, x, y in ((0, -6000.0, -6000.0), (1, -5000.0, -6000.0), (5, 6000.0, 6000.0), (6, 5000.0, 6000.0))
    for time in range(0, 310, 5)
]
PURCHASES = [
    dict(player_slot=0, team=2, game_time_s=0.0, item_name="item_tango", item_display_name="Tango", quantity=2,
         is_recipe=False, completed_item_name=None, completed_item_display_name=None, is_consumable=True, purchase_order=1),
    dict(player_slot=0, team=2, game_time_s=140.0, item_name="item_blink", item_display_name="Blink Dagger", quantity=1,
         is_recipe=False, completed_item_name=None, completed_item_display_name=None, is_consumable=False, purchase_order=2),
    dict(player_slot=5, team=3, game_time_s=200.0, item_name="item_recipe_phase_boots",
         item_display_name="recipe phase boots", quantity=1, is_recipe=True, completed_item_name="item_phase_boots",
         completed_item_display_name="Phase Boots", is_consumable=False, purchase_order=1),
    dict(player_slot=1, team=2, game_time_s=10.0, item_name="item_ward_observer", item_display_name="Observer Ward",
         quantity=2, is_recipe=False, completed_item_name=None, completed_item_display_name=None, is_consumable=True,
         purchase_order=1),
]
LANES = [
    dict(player_slot=slot, team=2 if slot < 5 else 3, lane=lane, lane_share=0.9, lane_role=role, net_worth_10=nw,
         xp_10=4000, level_10=8, last_hits_10=40, denies_10=5, kills_10=0, deaths_10=0, assists_10=0)
    for slot, lane, role, nw in ((0, "bot", "safe", 4000), (1, "bot", "safe", 2000),
                                 (5, "bot", "off", 3500), (6, "mid", "mid", 3800))
]
TIMELINE = [
    dict(player_slot=slot, team=2 if slot < 5 else 3, minute=minute, damage_to_heroes=100 * (slot + 1),
         damage_to_buildings=10, damage_to_roshan=0, damage_to_units=500, damage_from_heroes=50, damage_from_units=20)
    for slot in (0, 1, 5, 6) for minute in range(0, 6)
]


@pytest.fixture
def events_database(dashboard_database, monkeypatch):
    """Replace every event loader with recorded data."""
    module = import_module("events_db")
    values = {
        "load_match_deaths": DEATHS, "load_match_fights": FIGHTS, "load_fight_players": FIGHT_PLAYERS,
        "load_match_positions": POSITIONS, "load_purchases": PURCHASES, "load_player_lanes": LANES,
        "load_damage_timeline": TIMELINE,
        "load_replay_overview": dict(matches=2, avg_duration_s=1800.0, deaths=60, teamfights=8,
                                     avg_first_blood_s=95.0, avg_dead_s=40.0),
        "load_lead_outcomes": [dict(match_id=1, observation_time_s=600, net_worth_advantage=3000, radiant_won=True),
                               dict(match_id=2, observation_time_s=600, net_worth_advantage=-1000, radiant_won=True)],
        "load_lane_outcomes": [dict(match_id=1, lane="bot", team=team, net_worth_10=nw, players=2, measured=2, won=won)
                               for team, nw, won in ((2, 6000, True), (3, 5000, False))],
        "load_deaths_by_minute": [dict(minute=m, deaths=10, avg_dead_s=20.0 + m, matches_reaching=10) for m in range(30)],
        "load_killer_kinds": [dict(killer_kind="hero", deaths=50), dict(killer_kind="building", deaths=10)],
        "load_item_timing_heroes": [dict(hero_name="npc_dota_hero_antimage", hero_localized_name="Anti-Mage",
                                         hero_matches=2)],
        "load_hero_item_timings": [dict(item_name="item_blink", item_display_name="Blink Dagger", hero_matches=2,
                                        matches_bought=2, purchase_rate=1.0, median_first_purchase_s=900.0,
                                        fastest_first_purchase_s=800.0)],
    }
    for name, value in values.items():
        monkeypatch.setattr(module, name, MagicMock(return_value=value))
    return module


@pytest.fixture
def events_cursor(dashboard_database, monkeypatch):
    """A fake cursor behind events_db's connection, for the loaders' own checks."""
    module = import_module("events_db")
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.__enter__.return_value = connection
    monkeypatch.setattr(module, "open_postgres_database", MagicMock(return_value=manager))
    return module, cursor


def _set_rows(cursor, rows: list[dict]):
    cursor.description = [(column,) for column in rows[0]]
    cursor.fetchall.return_value = [tuple(row.values()) for row in rows]


# Loaders

def test_death_loader_splits_assists_and_binds_the_match(events_cursor, warehouse_config):
    module, cursor = events_cursor
    row = {**DEATHS[0], "assist_slots": "1,5"}
    row.pop("assists")
    _set_rows(cursor, [row, {**row, "tick": 4900, "assist_slots": None}])
    deaths = module.load_match_deaths(warehouse_config, 9000000001)
    assert [d["assists"] for d in deaths] == [[1, 5], []]
    assert cursor.execute.call_args.args[1] == (9000000001,)


@pytest.mark.parametrize("change", [{"victim_slot": 10}, {"victim_team": 1}, {"fight_number": 0},
                                    {"credited_killer_slot": -1}, {"x": None}])
def test_death_loader_rejects_invalid_records(events_cursor, warehouse_config, dashboard_database, change):
    module, cursor = events_cursor
    row = {**DEATHS[0], "assist_slots": None, **change}
    row.pop("assists")
    _set_rows(cursor, [row])
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        module.load_match_deaths(warehouse_config, 9000000001)


def test_loaders_turn_postgres_decimals_into_numbers(events_cursor, warehouse_config):
    module, cursor = events_cursor
    _set_rows(cursor, [{**TIMELINE[0], "damage_to_heroes": Decimal("1200")}])
    row = module.load_damage_timeline(warehouse_config, 9000000001)[0]
    assert row["damage_to_heroes"] == 1200 and type(row["damage_to_heroes"]) is int
    assert module._plain(Decimal("2.5")) == 2.5


def test_loaders_reject_invalid_match_ids_before_querying(events_cursor, warehouse_config, dashboard_database):
    module, cursor = events_cursor
    for loader in (module.load_match_deaths, module.load_match_positions, module.load_purchases):
        with pytest.raises(dashboard_database.DashboardDatabaseError):
            loader(warehouse_config, 0)
    cursor.execute.assert_not_called()


def test_fight_loader_rejects_inconsistent_death_counts(events_cursor, warehouse_config, dashboard_database):
    module, cursor = events_cursor
    _set_rows(cursor, [{**FIGHTS[1], "deaths": 3}])
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        module.load_match_fights(warehouse_config, 9000000001)


# Charts

@pytest.fixture
def charts(dashboard_database):
    return import_module("event_charts")


LABELS = {0: "Anti-Mage", 1: "Crystal Maiden", 5: "Axe", 6: "Lina"}


def test_timeline_puts_kills_on_the_credited_killer_and_deaths_on_the_victim(charts):
    figure = charts.build_event_timeline(sorted(LABELS), LABELS, DEATHS, FIGHTS)
    traces = {trace.name: trace for trace in figure.data}
    assert list(traces["Kill"].y) == ["Anti-Mage", "Axe", "Anti-Mage"]
    assert list(traces["Death"].y) == ["Axe", "Crystal Maiden", "Lina"]
    assert list(traces["Assist"].y) == ["Crystal Maiden", "Crystal Maiden"]
    # Only fights of two or more deaths are shaded.
    assert len(figure.layout.shapes) == 2  # the skirmish band and the team divider
    no_assists = charts.build_event_timeline(sorted(LABELS), LABELS, DEATHS, FIGHTS, show_assists=False)
    assert "Assist" not in {trace.name for trace in no_assists.data}


def test_fight_map_breaks_paths_at_teleports_and_frames_the_deaths(charts):
    positions = [dict(player_slot=0, time_s=t, x=x, y=0.0, is_alive=True)
                 for t, x in ((290, 0.0), (295, 300.0), (300, 7000.0), (305, 7100.0))]
    figure = charts.build_fight_map(FIGHTS[1], positions, DEATHS[1:], LABELS, {0})
    path = next(trace for trace in figure.data if trace.name == "Anti-Mage")
    assert list(path.x) == [0.0, 300.0, None, 7000.0, 7100.0]
    low, high = figure.layout.xaxis.range
    assert low < 100 < high and high - low < 8000


def test_lead_win_rates_count_the_team_ahead(charts):
    rows = [dict(observation_time_s=600, net_worth_advantage=adv, radiant_won=won)
            for adv, won in ((3000, True), (-3000, False), (2500, False), (-1000, True), (0, True))]
    rates = {(r["time_s"], r["bin"]): r for r in charts.lead_win_rates(rows)}
    assert rates[(600, "2k–5k")]["matches"] == 3
    assert rates[(600, "2k–5k")]["win_rate"] == pytest.approx(2 / 3)
    assert rates[(600, "Under 2k")]["win_rate"] == 0
    assert rates[(1200, "Under 2k")]["win_rate"] is None


def test_map_figure_draws_the_minimap_in_world_units(charts):
    figure = charts.map_figure()
    image = figure.layout.images[0]
    assert image.source.startswith("data:image/jpeg;base64,") and image.layer == "below"
    left, right, bottom, top = charts.MAP_IMAGE_EXTENT
    assert (image.x, image.y, image.sizex, image.sizey) == (left, top, right - left, top - bottom)
    assert list(figure.layout.xaxis.range) == list(charts.MAP_VIEW[0])
    zoomed = charts.map_figure(zoom=(1000, -500, 2000))
    assert list(zoomed.layout.xaxis.range) == [-1000, 3000] and list(zoomed.layout.yaxis.range) == [-2500, 1500]


def test_replay_map_animates_every_step_with_stable_hero_ids(charts):
    figure = charts.build_replay_map(POSITIONS, DEATHS, LABELS)
    assert len(figure.frames) == len(range(0, 310, 10))
    frame = next(frame for frame in figure.frames if frame.name == "300")
    heroes, recent = frame.data
    assert list(heroes.ids) == ["0", "1", "5", "6"]
    assert list(recent.x) == [100.0]  # deaths of the last 20 s: 5:00, not yet 5:03
    assert figure.layout.sliders[0].steps[0].label == "0:00"


def test_item_timeline_marks_recipes_as_completed_items(charts):
    figure = charts.build_item_timeline(PURCHASES, sorted(LABELS), LABELS)
    traces = {trace.name: trace for trace in figure.data}
    assert [row[1] for row in traces["Item completed"].customdata] == ["Phase Boots"]
    assert [row[1] for row in traces["Item bought"].customdata] == ["Blink Dagger"]
    assert "Consumable" not in traces
    with_consumables = charts.build_item_timeline(PURCHASES, sorted(LABELS), LABELS, consumables=True)
    assert len(next(t for t in with_consumables.data if t.name == "Consumable").x) == 2


# App sections

MATCH_TABS = ["timeline", "map", "fights", "laning", "items", "damage"]


@pytest.mark.parametrize("tab", MATCH_TABS)
def test_match_sections_render_from_event_marts(dashboard_app, events_database, tab):
    app = dashboard_app
    app.query_params.update(match_id="9000000001", tab=tab)
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    assert not app.warning
    assert app.title[0].value == "Match overview"
    assert app.get("plotly_chart"), f"{tab} drew no chart"
    # The scoreboard tables belong to the Overview section only.
    assert "Radiant" not in [heading.value for heading in app.subheader]


def test_overview_keeps_the_scoreboard_and_adds_the_match_story(dashboard_app, events_database):
    app = dashboard_app
    app.query_params["match_id"] = "9000000001"
    app.run(timeout=30)
    assert not app.exception
    headings = [heading.value for heading in app.subheader]
    assert headings[1:4] == ["Radiant", "Dire", "Match story"]
    story = " ".join(element.proto.body for element in app.get("html"))
    assert "First blood" in story and "Anti-Mage killed Axe" in story
    events_database.load_match_positions.assert_not_called()


def test_unknown_section_falls_back_to_the_overview(dashboard_app, events_database):
    app = dashboard_app
    app.query_params.update(match_id="9000000001", tab="nonsense")
    app.run(timeout=30)
    assert not app.exception
    assert "Radiant" in [heading.value for heading in app.subheader]


def test_event_mart_failure_warns_without_breaking_the_page(dashboard_app, events_database, dashboard_database):
    app = dashboard_app
    events_database.load_match_deaths.side_effect = dashboard_database.DashboardDatabaseError("missing")
    app.query_params.update(match_id="9000000001", tab="timeline")
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    assert any("Deaths could not be loaded" in warning.value for warning in app.warning)
    app.query_params["tab"] = "overview"
    app.run(timeout=30)
    assert not app.exception
    assert "Radiant" in [heading.value for heading in app.subheader]


@pytest.mark.parametrize("tab", ["story", "map", "items"])
def test_player_sections_render_for_one_or_two_players(dashboard_app, events_database, tab):
    app = dashboard_app
    app.query_params.update(match_id="9000000001", player_slot="0", player_tab=tab)
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.title[0].value == "Anti-Mage"
    assert app.get("plotly_chart")
    app.selectbox[0].select(5).run(timeout=30)
    assert not app.exception
    assert not app.error
    assert any("compared with Axe" in caption.value for caption in app.caption)


def test_player_summary_is_unchanged_by_the_new_sections(dashboard_app, events_database, dashboard_database):
    app = dashboard_app
    app.query_params.update(match_id="9000000001", player_slot="0")
    app.run(timeout=30)
    assert not app.exception
    assert len(app.get("plotly_chart")) == 3
    events_database.load_match_positions.assert_not_called()


def test_replay_insights_route(dashboard_app, events_database, dashboard_database):
    app = dashboard_app
    app.query_params["view"] = "insights"
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    assert app.title[0].value == "Replay insights"
    assert len(app.get("plotly_chart")) == 4  # leads, deaths by minute, killing blows, item timings
    dashboard_database.load_scoreboard.assert_not_called()
    events_database.load_hero_item_timings.assert_called_with(
        events_database.load_hero_item_timings.call_args.args[0], "npc_dota_hero_antimage")


def test_switching_to_insights_clears_match_selections(dashboard_app, events_database):
    app = dashboard_app
    app.query_params.update(match_id="9000000001", player_slot="0", tab="map", player_tab="map")
    app.run(timeout=30)
    app.radio(key="dashboard_view").set_value("insights").run(timeout=30)
    assert not app.exception
    assert app.title[0].value == "Replay insights"
    for key in ("match_id", "player_slot", "tab", "player_tab"):
        assert key not in app.query_params


def test_insights_report_an_empty_replay_set(dashboard_app, events_database):
    app = dashboard_app
    events_database.load_replay_overview.return_value = dict(matches=0, avg_duration_s=None, deaths=0,
                                                             teamfights=0, avg_first_blood_s=None, avg_dead_s=None)
    app.query_params["view"] = "insights"
    app.run(timeout=30)
    assert not app.exception
    assert any("No parsed replays" in info.value for info in app.info)
