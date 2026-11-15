"""Check hero mart records and the aggregate page with controlled, offline data."""

import math
from copy import deepcopy
from importlib import import_module
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def hero_cursor(warehouse_cursor):
    """Provide observed heroes with different sample sizes and a missing display name."""
    warehouse_cursor.fetchall.return_value = [
        (2, "Axe", 50, 0, 100, 0.5, 0.0),
        (1, "Anti-Mage", 20, 12, 100, 0.2, 0.6),
        (3, None, 1, 1, 100, 0.01, 1.0),
    ]
    return warehouse_cursor


def test_hero_loader_preserves_counts_and_derives_losses(dashboard_database, warehouse_config, hero_cursor):
    heroes = dashboard_database.load_hero_statistics(warehouse_config)
    assert heroes[0] == {
        "hero_id": 2, "hero_label": "Axe", "picks": 50, "wins": 0, "losses": 50,
        "ranked_matches": 100, "pick_rate": 0.5, "win_rate": 0.0,
    }
    assert heroes[1]["losses"] == 8
    assert heroes[2]["hero_label"] == "Hero 3"
    assert "marts.hero_pick_win_rate" in hero_cursor.execute.call_args.args[0]
    dashboard_database.open_postgres_database.assert_called_once_with(warehouse_config)


def test_hero_loader_handles_an_empty_mart(dashboard_database, warehouse_config, hero_cursor):
    hero_cursor.fetchall.return_value = []
    assert dashboard_database.load_hero_statistics(warehouse_config) == []


@pytest.mark.parametrize("rate", [None, math.nan, math.inf, -0.1, 1.1])
def test_hero_loader_marks_invalid_rates_unavailable(
    dashboard_database, warehouse_config, hero_cursor, rate,
):
    hero_cursor.fetchall.return_value = [(1, "Anti-Mage", 20, 12, 100, rate, rate)]
    hero = dashboard_database.load_hero_statistics(warehouse_config)[0]
    assert hero["pick_rate"] is None
    assert hero["win_rate"] is None
    assert hero["wins"] == 12


@pytest.mark.parametrize("row", [
    (0, "Unknown", 1, 0, 100, 0.01, 0.0),
    (1, "Anti-Mage", 0, 0, 100, 0.0, None),
    (1, "Anti-Mage", -1, 0, 100, None, None),
    (1, "Anti-Mage", 10, 11, 100, 0.1, 1.1),
    (1, "Anti-Mage", 10, -1, 100, 0.1, None),
    (1, "Anti-Mage", 10, 1, 5, 2.0, 0.1),
    (1, "Anti-Mage", None, 1, 100, None, None),
    (1, "Anti-Mage", 10.5, 1, 100, None, None),
])
def test_hero_loader_rejects_invalid_identifiers_and_counts(
    dashboard_database, warehouse_config, hero_cursor, row,
):
    hero_cursor.fetchall.return_value = [row]
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="invalid hero IDs or counts"):
        dashboard_database.load_hero_statistics(warehouse_config)


def test_hero_loader_rejects_duplicate_ids(dashboard_database, warehouse_config, hero_cursor):
    hero_cursor.fetchall.return_value = [
        (1, "Anti-Mage", 20, 12, 100, 0.2, 0.6),
        (1, "Anti-Mage", 20, 12, 100, 0.2, 0.6),
    ]
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="duplicate hero IDs"):
        dashboard_database.load_hero_statistics(warehouse_config)


def test_hero_loader_rejects_inconsistent_denominators(dashboard_database, warehouse_config, hero_cursor):
    hero_cursor.fetchall.return_value = [
        (1, "Anti-Mage", 20, 12, 100, 0.2, 0.6),
        (2, "Axe", 50, 0, 200, 0.25, 0.0),
    ]
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="inconsistent match totals"):
        dashboard_database.load_hero_statistics(warehouse_config)


@pytest.fixture
def hero_app(dashboard_database, warehouse_config, hero_cursor, monkeypatch):
    """Render the real entrypoint while keeping warehouse reads controlled."""
    testing = pytest.importorskip("streamlit.testing.v1")
    heroes = dashboard_database.load_hero_statistics(warehouse_config)
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "offline-test-password")
    monkeypatch.setattr(dashboard_database, "load_hero_statistics", MagicMock(return_value=heroes))
    monkeypatch.setattr(dashboard_database, "load_hero_performance", MagicMock(return_value=None))
    monkeypatch.setattr(dashboard_database, "load_hero_matchups", MagicMock(return_value=[]))
    monkeypatch.setattr(dashboard_database, "load_hero_duration", MagicMock(return_value=[]))
    monkeypatch.setattr(dashboard_database, "load_hero_allies", MagicMock(return_value=[]))
    monkeypatch.setattr(import_module("events_db"), "load_hero_item_timings_by_id", MagicMock(return_value=[]))
    monkeypatch.setattr(dashboard_database, "load_default_match_id", MagicMock(
        side_effect=AssertionError("Hero statistics must not query replay data"),
    ))
    return testing.AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))


def test_hero_page_shows_dataset_and_selected_hero_metrics(hero_app):
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    metrics = {metric.label: metric.value for metric in hero_app.metric}
    assert metrics == {
        "Qualifying matches": "100", "Heroes represented": "3", "Win rate": "60.0%",
        "Pick rate": "20.0%", "Picks": "20", "Wins": "12", "Losses": "8",
    }
    table = hero_app.dataframe[0].value
    assert table["Hero"].tolist() == ["Axe", "Anti-Mage", "Hero 3"]
    assert table["Win rate"].tolist() == [0.0, 60.0, 100.0]
    assert table["Picks"].tolist() == [50, 20, 1]


def test_hero_selection_keeps_zero_wins_distinct_from_missing(hero_app):
    hero_app.run(timeout=15)
    hero_app.selectbox(key="hero_stats_selected_id").select(2).run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    metrics = {metric.label: metric.value for metric in hero_app.metric}
    assert metrics["Win rate"] == "0.0%"
    assert metrics["Wins"] == "0"
    assert metrics["Losses"] == "50"
    assert hero_app.query_params["hero_id"] == "2"


def test_minimum_picks_changes_only_the_comparison_table(hero_app):
    hero_app.run(timeout=15)
    hero_app.number_input(key="hero_stats_minimum_picks").set_value(21).run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.selectbox(key="hero_stats_selected_id").value == 1
    metrics = {metric.label: metric.value for metric in hero_app.metric}
    assert metrics["Qualifying matches"] == "100"
    assert metrics["Heroes represented"] == "3"
    assert metrics["Picks"] == "20"
    assert hero_app.dataframe[0].value["Hero"].tolist() == ["Axe"]
    assert hero_app.query_params["hero_id"] == "1"

    hero_app.number_input(key="hero_stats_minimum_picks").set_value(51).run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert not hero_app.dataframe
    assert "No heroes meet" in hero_app.info[0].value
    assert any(metric.label == "Win rate" for metric in hero_app.metric)


def test_hero_page_uses_an_honest_empty_state(hero_app, dashboard_database):
    dashboard_database.load_hero_statistics.return_value = []
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.info[0].value == "No hero statistics available yet."
    assert not hero_app.metric
    assert not hero_app.dataframe


def test_hero_link_restores_selection_in_a_new_session(hero_app):
    from streamlit.testing.v1 import AppTest

    hero_app.query_params["view"] = "heroes"
    hero_app.query_params["hero_id"] = "2"
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.selectbox(key="hero_stats_selected_id").value == 2

    hero_app.selectbox(key="hero_stats_selected_id").select(3).run(timeout=15)
    assert hero_app.query_params["hero_id"] == "3"
    refreshed = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))
    refreshed.query_params.update(hero_app.query_params)
    refreshed.run(timeout=15)
    assert not refreshed.exception
    assert not refreshed.error
    assert refreshed.selectbox(key="hero_stats_selected_id").value == 3
    assert any(metric.label == "Win rate" and metric.value == "100.0%" for metric in refreshed.metric)


def test_a_new_hero_link_overrides_previous_widget_state(hero_app):
    hero_app.run(timeout=15)
    hero_app.query_params["hero_id"] = "2"
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.selectbox(key="hero_stats_selected_id").value == 2


@pytest.mark.parametrize(("selection", "message"), [
    ("invalid", "positive integer"),
    ("0", "positive integer"),
    ("-1", "positive integer"),
    (str(2**31), "supported range"),
    (["1", "2"], "repeated hero selections"),
])
def test_invalid_hero_links_allow_explicit_recovery(hero_app, selection, message):
    hero_app.query_params["hero_id"] = selection
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert message in hero_app.error[0].value
    assert hero_app.selectbox(key="hero_stats_selected_id").value is None
    assert {metric.label for metric in hero_app.metric} == {"Qualifying matches", "Heroes represented"}

    # A new choice replaces the malformed URL instead of silently displaying a different hero.
    hero_app.selectbox(key="hero_stats_selected_id").select(2).run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.query_params["hero_id"] == "2"
    assert any(metric.label == "Wins" and metric.value == "0" for metric in hero_app.metric)


def test_unobserved_hero_is_not_presented_as_zero_win_rate(hero_app):
    hero_app.query_params["hero_id"] = "999"
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert "No recorded observations" in hero_app.warning[0].value
    assert hero_app.selectbox(key="hero_stats_selected_id").value is None
    assert not any(metric.label == "Win rate" for metric in hero_app.metric)
    assert hero_app.dataframe


def test_removed_hero_does_not_leave_stale_details(hero_app, dashboard_database):
    hero_app.query_params["hero_id"] = "2"
    hero_app.run(timeout=15)
    dashboard_database.load_hero_statistics.return_value = [
        hero for hero in dashboard_database.load_hero_statistics.return_value if hero["hero_id"] != 2
    ]
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert "No recorded observations" in hero_app.warning[0].value
    assert not any(metric.label == "Win rate" for metric in hero_app.metric)


def test_duplicate_names_remain_selectable_by_hero_id(hero_app, dashboard_database):
    for hero in dashboard_database.load_hero_statistics.return_value:
        if hero["hero_id"] in (1, 2):
            hero["hero_label"] = "Shared name"
    hero_app.query_params["hero_id"] = "1"
    hero_app.run(timeout=15)
    assert not hero_app.exception
    options = hero_app.selectbox(key="hero_stats_selected_id").options
    assert "Shared name · Hero 1" in options
    assert "Shared name · Hero 2" in options
    hero_app.selectbox(key="hero_stats_selected_id").select(2).run(timeout=15)
    assert not hero_app.exception
    assert hero_app.query_params["hero_id"] == "2"
    assert any(metric.label == "Picks" and metric.value == "50" for metric in hero_app.metric)


def test_missing_name_and_rates_remain_unavailable_on_the_page(hero_app, dashboard_database):
    hero = dashboard_database.load_hero_statistics.return_value[2]
    hero.update(pick_rate=None, win_rate=None)
    hero_app.query_params["hero_id"] = "3"
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert any(subheader.value == "Hero 3" for subheader in hero_app.subheader)
    metrics = {metric.label: metric.value for metric in hero_app.metric}
    assert metrics["Win rate"] == "—"
    assert metrics["Pick rate"] == "—"
    assert metrics["Picks"] == "1"
    rates = hero_app.dataframe[0].value.set_index("Hero").loc["Hero 3", ["Pick rate", "Win rate"]]
    assert rates.isna().all()


def test_filter_recovery_preserves_source_records_and_hero_link(hero_app, dashboard_database):
    original = deepcopy(dashboard_database.load_hero_statistics.return_value)
    hero_app.query_params["hero_id"] = "2"
    hero_app.run(timeout=15)
    hero_app.number_input(key="hero_stats_minimum_picks").set_value(1000).run(timeout=15)
    assert "No heroes meet" in hero_app.info[0].value
    hero_app.number_input(key="hero_stats_minimum_picks").set_value(1).run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.query_params["hero_id"] == "2"
    assert len(hero_app.dataframe[0].value) == 3
    assert dashboard_database.load_hero_statistics.return_value == original


def test_populated_hero_page_survives_an_empty_replay_section(hero_app, dashboard_database):
    dashboard_database.load_default_match_id.side_effect = None
    dashboard_database.load_default_match_id.return_value = None
    hero_app.query_params["hero_id"] = "2"
    hero_app.run(timeout=15)
    hero_app.radio(key="dashboard_view").set_value("matches").run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert "No matches" in hero_app.info[0].value
    assert "hero_id" not in hero_app.query_params
    hero_app.radio(key="dashboard_view").set_value("heroes").run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert hero_app.title[0].value == "Hero statistics"
    assert len(hero_app.dataframe[0].value) == 3


def performance_record(database):
    profile = dict(hero_id=1, appearances=20, wins=12, losses=8, win_rate=0.6, avg_match_duration_s=2000.0)
    for metric, count in database.HERO_PERFORMANCE_METRICS:
        profile[metric] = 0.0
        profile[count] = 20
    profile.update(avg_gpm=500.0, gpm_observations=10, avg_hero_damage_per_min=None, hero_damage_observations=0)
    return profile


def test_performance_loader_preserves_zero_missing_and_coverage(dashboard_database, warehouse_config, warehouse_cursor):
    expected = performance_record(dashboard_database)
    warehouse_cursor.fetchall.return_value = [tuple(expected[column] for column in dashboard_database.HERO_PERFORMANCE_COLUMNS)]
    assert dashboard_database.load_hero_performance(warehouse_config, 1) == expected
    sql, parameters = warehouse_cursor.execute.call_args.args
    assert "marts.hero_performance_summary" in sql
    assert parameters == (1,)


@pytest.mark.parametrize("changes", [
    {"gpm_observations": 21}, {"gpm_observations": -1}, {"gpm_observations": 0},
    {"avg_gpm": None}, {"avg_gpm": math.nan}, {"avg_gpm": math.inf}, {"avg_gpm": -1.0},
    {"wins": 21}, {"losses": 9}, {"win_rate": 0.1}, {"avg_match_duration_s": 0.0},
])
def test_performance_loader_rejects_inconsistent_metrics(
    dashboard_database, warehouse_config, warehouse_cursor, changes,
):
    profile = performance_record(dashboard_database)
    profile.update(changes)
    warehouse_cursor.fetchall.return_value = [tuple(profile[column] for column in dashboard_database.HERO_PERFORMANCE_COLUMNS)]
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_hero_performance(warehouse_config, 1)


def test_performance_loader_handles_missing_and_duplicate_rows(dashboard_database, warehouse_config, warehouse_cursor):
    warehouse_cursor.fetchall.return_value = []
    assert dashboard_database.load_hero_performance(warehouse_config, 1) is None
    profile = performance_record(dashboard_database)
    row = tuple(profile[column] for column in dashboard_database.HERO_PERFORMANCE_COLUMNS)
    warehouse_cursor.fetchall.return_value = [row, row]
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="duplicate"):
        dashboard_database.load_hero_performance(warehouse_config, 1)


def test_performance_profile_shows_coverage_and_preserves_selection(hero_app, dashboard_database):
    dashboard_database.load_hero_performance.return_value = performance_record(dashboard_database)
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    metrics = {metric.label: metric.value for metric in hero_app.metric}
    assert metrics["Gold / min"] == "500.0"
    assert metrics["Kills"] == "0.0"
    assert metrics["Hero damage / min"] == "—"
    assert any("Recorded in 10 of 20" in caption.value for caption in hero_app.caption)
    assert any("Recorded in 0 of 20" in caption.value for caption in hero_app.caption)
    hero_app.number_input(key="hero_stats_minimum_picks").set_value(21).run(timeout=15)
    assert hero_app.query_params["hero_id"] == "1"
    assert any(metric.label == "Gold / min" and metric.value == "500.0" for metric in hero_app.metric)
    hero_app.query_params["hero_id"] = "invalid"
    dashboard_database.load_hero_performance.reset_mock()
    hero_app.run(timeout=15)
    dashboard_database.load_hero_performance.assert_not_called()


def test_performance_failure_preserves_pick_statistics(hero_app, dashboard_database):
    dashboard_database.load_hero_performance.side_effect = dashboard_database.DashboardDatabaseError("missing mart")
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert "could not be loaded" in hero_app.warning[0].value
    assert any(metric.label == "Picks" and metric.value == "20" for metric in hero_app.metric)
    assert len(hero_app.dataframe[0].value) == 3


def test_out_of_sync_profile_is_not_displayed(hero_app, dashboard_database):
    profile = performance_record(dashboard_database)
    profile["appearances"] = 21
    dashboard_database.load_hero_performance.return_value = profile
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert "out of sync" in hero_app.warning[0].value
    assert not any(metric.label == "Gold / min" for metric in hero_app.metric)


def matchup_record(opponent=2, name="Axe", matches=12, wins=8):
    return dict(hero_id=1, opponent_hero_id=opponent, opponent_name=name, matches=matches,
                wins=wins, losses=matches - wins, win_rate=wins / matches,
                overall_matches=20, overall_win_rate=0.6,
                win_rate_difference_pp=100 * (wins / matches - 0.6))


def test_matchup_loader_preserves_zero_rates_and_unknown_names(dashboard_database, warehouse_config, warehouse_cursor):
    record = matchup_record(3, None, 1, 0)
    warehouse_cursor.fetchall.return_value = [tuple(record[c] for c in dashboard_database.HERO_MATCHUP_COLUMNS)]
    result = dashboard_database.load_hero_matchups(warehouse_config, 1)
    assert result[0]["opponent_label"] == "Hero 3"
    assert result[0]["win_rate"] == 0.0
    assert result[0]["win_rate_difference_pp"] == -60.0
    assert warehouse_cursor.execute.call_args.args[1] == (1,)


@pytest.mark.parametrize("changes", [
    {"matches": 0}, {"matches": 21}, {"wins": 13}, {"losses": 9},
    {"win_rate": math.nan}, {"overall_win_rate": math.inf}, {"win_rate_difference_pp": 99},
    {"opponent_hero_id": 1}, {"opponent_hero_id": None}, {"hero_id": 2},
])
def test_matchup_loader_rejects_invalid_records(dashboard_database, warehouse_config, warehouse_cursor, changes):
    record = matchup_record()
    record.update(changes)
    warehouse_cursor.fetchall.return_value = [tuple(record[c] for c in dashboard_database.HERO_MATCHUP_COLUMNS)]
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_hero_matchups(warehouse_config, 1)


def test_matchup_loader_handles_empty_and_duplicate_opponents(dashboard_database, warehouse_config, warehouse_cursor):
    warehouse_cursor.fetchall.return_value = []
    assert dashboard_database.load_hero_matchups(warehouse_config, 1) == []
    record = matchup_record()
    row = tuple(record[c] for c in dashboard_database.HERO_MATCHUP_COLUMNS)
    warehouse_cursor.fetchall.return_value = [row, row]
    with pytest.raises(dashboard_database.DashboardDatabaseError, match="duplicate"):
        dashboard_database.load_hero_matchups(warehouse_config, 1)


def mocked_matchups(database):
    records = [matchup_record(3, None, 1, 0), matchup_record()]
    for record in records:
        name = record.pop("opponent_name")
        record["opponent_label"] = name or "Hero 3"
    database.load_hero_matchups.return_value = records
    return records


def test_matchup_filters_details_and_recovery(hero_app, dashboard_database):
    mocked_matchups(dashboard_database)
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    table = hero_app.dataframe[0].value
    assert table["Opponent"].tolist() == ["Axe", "Hero 3"]
    assert table["Matches"].tolist() == [12, 1]
    hero_app.selectbox(key="hero_matchups_1_opponent").select(3).run(timeout=15)
    assert any(metric.label == "Matchup win rate" and metric.value == "0.0%" for metric in hero_app.metric)
    hero_app.number_input(key="hero_matchups_1_minimum").set_value(2).run(timeout=15)
    assert hero_app.selectbox(key="hero_matchups_1_opponent").value is None
    assert hero_app.dataframe[0].value["Opponent"].tolist() == ["Axe"]
    hero_app.text_input(key="hero_matchups_1_search").set_value("nobody").run(timeout=15)
    assert any("No opponents meet" in notice.value for notice in hero_app.info)
    assert any(metric.label == "Picks" and metric.value == "20" for metric in hero_app.metric)
    hero_app.text_input(key="hero_matchups_1_search").set_value(" AXE ").run(timeout=15)
    assert not hero_app.exception
    assert hero_app.dataframe[0].value["Opponent"].tolist() == ["Axe"]
    assert hero_app.query_params["hero_id"] == "1"


def test_matchup_failure_and_stale_context_preserve_other_sections(hero_app, dashboard_database):
    dashboard_database.load_hero_matchups.side_effect = dashboard_database.DashboardDatabaseError("missing mart")
    hero_app.run(timeout=15)
    assert not hero_app.exception
    assert not hero_app.error
    assert any("Opponent results could not" in notice.value for notice in hero_app.warning)
    assert hero_app.dataframe[0].value["Hero"].tolist() == ["Axe", "Anti-Mage", "Hero 3"]
    dashboard_database.load_hero_matchups.side_effect = None
    records = mocked_matchups(dashboard_database)
    records[0]["overall_matches"] = 21
    hero_app.run(timeout=15)
    assert any("out of sync" in notice.value for notice in hero_app.warning)
    assert len(hero_app.dataframe) == 1


def test_matchup_selection_scoped_to_hero_and_invalid_links_skip_query(hero_app, dashboard_database):
    records = mocked_matchups(dashboard_database)
    dashboard_database.load_hero_matchups.side_effect = lambda config, hero_id: records if hero_id == 1 else []
    hero_app.run(timeout=15)
    hero_app.selectbox(key="hero_matchups_1_opponent").select(2).run(timeout=15)
    hero_app.selectbox(key="hero_stats_selected_id").select(2).run(timeout=15)
    assert not hero_app.exception
    assert hero_app.query_params["hero_id"] == "2"
    assert not any(metric.label == "Matchup matches" for metric in hero_app.metric)
    hero_app.query_params["hero_id"] = "invalid"
    dashboard_database.load_hero_matchups.reset_mock()
    hero_app.run(timeout=15)
    dashboard_database.load_hero_matchups.assert_not_called()


# Hero meta, game length, allies and items

DURATION = [dict(bucket_order=1, duration_bucket="Under 30 min", matches=10, wins=7, win_rate=0.7),
            dict(bucket_order=3, duration_bucket="40–50 min", matches=10, wins=4, win_rate=0.4)]


def ally(ally_id, matches, wins, overall=0.6):
    return dict(ally_hero_id=ally_id, ally_label=f"Hero {ally_id}", matches=matches, wins=wins,
                losses=matches - wins, win_rate=wins / matches, overall_win_rate=overall,
                win_rate_difference_pp=100 * (wins / matches - overall))


def test_duration_and_ally_loaders_check_rows(dashboard_database, warehouse_config, warehouse_cursor):
    warehouse_cursor.fetchall.return_value = [(1, "Under 30 min", 10, 7, 0.7)]
    assert dashboard_database.load_hero_duration(warehouse_config, 1)[0]["wins"] == 7
    assert warehouse_cursor.execute.call_args.args[1] == (1,)
    warehouse_cursor.fetchall.return_value = [(1, "Under 30 min", 10, 7, 0.5)]
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_hero_duration(warehouse_config, 1)
    warehouse_cursor.fetchall.return_value = [(5, None, 4, 3, 1, 0.75, 0.6, 15.0)]
    assert dashboard_database.load_hero_allies(warehouse_config, 1)[0]["ally_label"] == "Hero 5"
    warehouse_cursor.fetchall.return_value = [(1, "Self", 4, 3, 1, 0.75, 0.6, 15.0)]
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_hero_allies(warehouse_config, 1)
    warehouse_cursor.fetchall.return_value = [(1, "short row")]
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_hero_allies(warehouse_config, 1)
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_hero_duration(warehouse_config, 0)


def test_hero_charts(dashboard_database):
    hc = import_module("hero_charts")
    heroes = [dict(hero_id=1, hero_label="Anti-Mage", picks=20, wins=12, pick_rate=0.2, win_rate=0.6),
              dict(hero_id=2, hero_label="Axe", picks=50, wins=0, pick_rate=0.5, win_rate=0.0),
              dict(hero_id=3, hero_label="Hero 3", picks=1, wins=1, pick_rate=None, win_rate=None)]
    meta = hc.build_meta_chart(heroes, 1)
    assert [trace.customdata[0][0] for trace in meta.data] == [2, 1]  # selected hero drawn last, on top
    assert list(meta.data[1].text) == ["Anti-Mage"]
    duration = hc.build_duration_chart(DURATION, 0.6)
    assert list(duration.data[0].marker.color) == [hc.UP_COLOR, hc.DOWN_COLOR]
    rows = [ally(slot, 30, wins) for slot, wins in enumerate(range(5, 25), start=10)] + [ally(99, 2, 2)]
    shown = hc.best_and_worst(rows, minimum_matches=20, count=3)
    assert [row["ally_hero_id"] for row in shown] == [10, 11, 12, 27, 28, 29]
    assert len(hc.best_and_worst(rows[:4], minimum_matches=20, count=3)) == 4
    chart = hc.build_pair_chart(shown, "ally_label")
    assert chart.data[0].marker.color[0] == hc.DOWN_COLOR and chart.data[0].marker.color[-1] == hc.UP_COLOR


def test_hero_page_tabs_show_meta_length_allies_and_items(hero_app, dashboard_database):
    dashboard_database.load_hero_duration.return_value = DURATION
    dashboard_database.load_hero_allies.return_value = [ally(slot, 30, 10 + slot) for slot in range(10, 20)]
    import_module("events_db").load_hero_item_timings_by_id.return_value = [
        dict(item_name="item_blink", item_display_name="Blink Dagger", hero_matches=3, matches_bought=2,
             purchase_rate=2 / 3, median_first_purchase_s=900.0, fastest_first_purchase_s=800.0)]
    app = hero_app
    app.query_params["view"] = "heroes"
    app.query_params["hero_id"] = "1"
    app.run(timeout=15)
    assert not app.exception and not app.error
    assert [tab.label for tab in app.tabs] == ["Profile", "Game length", "Matchups", "Allies", "Items"]
    headings = [element.value for element in app.subheader]
    for heading in ("Hero meta", "Win rate by game length", "Results with allied heroes", "Item timings"):
        assert heading in headings
    assert len(app.get("plotly_chart")) == 4  # meta, game length, allies, items (no matchups here)
    assert dashboard_database.load_hero_allies.call_args.args[1] == 1
