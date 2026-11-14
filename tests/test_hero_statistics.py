"""Check hero mart records and the aggregate page with controlled, offline data."""

import math
from copy import deepcopy
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
