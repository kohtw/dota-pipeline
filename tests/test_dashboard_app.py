"""Exercise warehouse startup, routing and player views without a live database."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


APP_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


@pytest.fixture
def dashboard_app(dashboard_database, warehouse_config, scoreboard_cursor, monkeypatch):
    """Keep app interaction tests independent of database availability and credentials."""
    testing = pytest.importorskip("streamlit.testing.v1")
    pytest.importorskip("plotly")
    match, players = dashboard_database.load_scoreboard(warehouse_config, 9000000001)
    for variable, value in {
        "WAREHOUSE_HOST": "localhost", "WAREHOUSE_PORT": "5433",
        "WAREHOUSE_DB": "warehouse", "WAREHOUSE_USER": "warehouse",
        "WAREHOUSE_PASSWORD": "offline-test-password",
    }.items():
        monkeypatch.setenv(variable, value)
    monkeypatch.setattr(dashboard_database, "load_default_match_id", MagicMock(return_value=9000000001))
    monkeypatch.setattr(dashboard_database, "load_available_matches", MagicMock(return_value=[9000000001]))
    monkeypatch.setattr(dashboard_database, "load_hero_statistics", MagicMock(return_value=[]))
    monkeypatch.setattr(dashboard_database, "load_scoreboard", MagicMock(return_value=(match, players)))
    monkeypatch.setattr(dashboard_database, "load_player_snapshots", MagicMock(return_value={
        player["slot"]: [
            {"game_time_s": 0.0, "net_worth": 600, "total_xp": 0},
            {"game_time_s": 600.0, "net_worth": player["net_worth"], "total_xp": 5000},
        ] for player in players
    }))
    monkeypatch.setattr(dashboard_database, "load_player_damage", MagicMock(return_value={
        0: [{"target_slot": 5, "target_team": 3, "source": "dota_unknown",
             "total_damage": 300, "hits": 3, "share_of_team_damage": 0.5}],
        5: [{"target_slot": 0, "target_team": 2, "source": "dota_unknown",
             "total_damage": 100, "hits": 1, "share_of_team_damage": 0.5}],
    }))
    return testing.AppTest.from_file(str(APP_PATH))


def test_player_view_compares_players_and_returns_to_scoreboard(dashboard_app, dashboard_database):
    app = dashboard_app
    app.query_params["match_id"] = "9000000001"
    app.query_params["player_slot"] = "0"
    app.run(timeout=15)

    assert not app.exception
    assert not app.error
    metrics = {metric.label: metric.value for metric in app.metric}
    assert metrics["Kill participation"] == "66.7% · 2 of 3 team kills"
    assert len(app.get("plotly_chart")) == 3
    headings = [heading.value for heading in app.subheader]
    assert "Final main inventory" in headings
    assert "Purchase history" not in headings
    assert app.slider[0].label == "Progression time range (seconds)"

    app.selectbox[0].select(5).run(timeout=15)
    assert not app.exception
    assert not app.error
    stats = app.table[0].value.set_index("Metric")
    assert stats.loc["Kill participation"].tolist() == [
        "66.7% · 2 of 3 team kills", "50.0% · 1 of 2 team kills",
    ]
    assert dashboard_database.load_player_snapshots.call_args.args[1:] == (9000000001, 0, 5)
    assert len(app.get("plotly_chart")) == 3

    # Narrowing progression must not change final statistics, inventory or full-match damage.
    final_statistics = app.table[0].value.copy()
    damage_summary = app.table[1].value.copy()
    inventories = [text.value for text in app.text]
    app.slider[0].set_range(0, 300).run(timeout=15)
    assert not app.exception
    assert not app.error
    assert app.table[0].value.equals(final_statistics)
    assert app.table[1].value.equals(damage_summary)
    assert [text.value for text in app.text] == inventories
    for chart in app.get("plotly_chart")[1:]:
        figure = json.loads(chart.proto.spec)
        assert all(0 <= time <= 300 for trace in figure["data"] for time in trace["x"])
    assert "Purchase history" not in [heading.value for heading in app.subheader]

    app.button(key="player_back").click().run(timeout=15)
    assert not app.exception
    assert not app.error
    assert app.title[0].value == "Match overview"
    assert "player_slot" not in app.query_params
    assert {metric.label for metric in app.metric} == {"Match ID", "Result", "Duration"}


def test_app_reports_missing_configuration_before_querying(
    dashboard_app, dashboard_database, monkeypatch,
):
    monkeypatch.delenv("WAREHOUSE_PASSWORD")
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "WAREHOUSE_PASSWORD" in dashboard_app.error[0].value
    dashboard_database.load_default_match_id.assert_not_called()


def test_app_reports_database_unavailable(dashboard_app, dashboard_database):
    dashboard_database.load_default_match_id.side_effect = dashboard_database.DashboardDatabaseError(
        "Could not connect to or communicate with Postgres."
    )
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "Postgres" in dashboard_app.error[0].value


def test_app_handles_an_empty_warehouse(dashboard_app, dashboard_database):
    dashboard_database.load_default_match_id.return_value = None
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert "No matches" in dashboard_app.info[0].value
    dashboard_database.load_scoreboard.assert_not_called()


def test_app_rejects_an_unavailable_match(dashboard_app, dashboard_database):
    dashboard_database.load_scoreboard.return_value = ({"match_id": 123}, [])
    dashboard_app.query_params["match_id"] = "123"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "not available" in dashboard_app.error[0].value


def test_optional_damage_failure_preserves_player_statistics(dashboard_app, dashboard_database):
    dashboard_database.load_player_damage.side_effect = dashboard_database.DashboardDatabaseError(
        "The damage mart is unavailable."
    )
    dashboard_app.query_params["match_id"] = "9000000001"
    dashboard_app.query_params["player_slot"] = "0"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "damage mart" in dashboard_app.error[0].value
    assert any(metric.label == "Kill participation" for metric in dashboard_app.metric)
    assert len(dashboard_app.get("plotly_chart")) == 2


def test_invalid_port_is_reported_before_querying(dashboard_app, dashboard_database, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_PORT", "invalid")
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "WAREHOUSE_PORT" in dashboard_app.error[0].value
    dashboard_database.load_default_match_id.assert_not_called()


@pytest.mark.parametrize(("match_id", "message"), [
    ("not-a-match", "must be an integer"),
    ("0", "outside the supported range"),
    (str(2**63), "outside the supported range"),
    (["9000000001", "9000000002"], "repeated match or player"),
])
def test_invalid_match_link_can_return_to_an_available_scoreboard(
    dashboard_app, dashboard_database, match_id, message,
):
    dashboard_app.query_params["match_id"] = match_id
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert message in dashboard_app.error[0].value
    dashboard_database.load_scoreboard.assert_not_called()

    # Recovery replaces the invalid selection with an existing match and clears the player.
    dashboard_app.button(key="invalid_link_back").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"


@pytest.mark.parametrize(("player_slot", "message"), [
    ("invalid", "player slot in this link must be an integer"),
    ("999", "player requested in this link is not in this match"),
])
def test_invalid_player_link_does_not_query_player_data(
    dashboard_app, dashboard_database, player_slot, message,
):
    dashboard_app.query_params["match_id"] = "9000000001"
    dashboard_app.query_params["player_slot"] = player_slot
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert message in dashboard_app.error[0].value
    dashboard_database.load_player_snapshots.assert_not_called()
    dashboard_database.load_player_damage.assert_not_called()


def test_progression_failure_stops_player_rendering(dashboard_app, dashboard_database):
    dashboard_database.load_player_snapshots.side_effect = dashboard_database.DashboardDatabaseError(
        "The warehouse query exceeded its time limit."
    )
    dashboard_app.query_params["match_id"] = "9000000001"
    dashboard_app.query_params["player_slot"] = "0"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "time limit" in dashboard_app.error[0].value
    assert not dashboard_app.get("plotly_chart")
    dashboard_database.load_player_damage.assert_not_called()


def test_new_visit_opens_heroes_without_loading_replay_data(dashboard_app, dashboard_database):
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Hero statistics"
    assert dashboard_app.radio(key="dashboard_view").value == "heroes"
    dashboard_database.load_default_match_id.assert_not_called()
    dashboard_database.load_available_matches.assert_not_called()
    dashboard_database.load_scoreboard.assert_not_called()
    dashboard_database.load_player_snapshots.assert_not_called()
    dashboard_database.load_player_damage.assert_not_called()


def test_section_navigation_routes_and_clears_incompatible_selections(dashboard_app):
    dashboard_app.query_params["hero_id"] = "1"
    dashboard_app.run(timeout=15)
    dashboard_app.radio(key="dashboard_view").set_value("matches").run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"
    assert dashboard_app.query_params["view"] == "matches"
    assert "hero_id" not in dashboard_app.query_params

    dashboard_app.query_params["match_id"] = "9000000001"
    dashboard_app.query_params["player_slot"] = "0"
    dashboard_app.run(timeout=15)
    dashboard_app.radio(key="dashboard_view").set_value("heroes").run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Hero statistics"
    assert "match_id" not in dashboard_app.query_params
    assert "player_slot" not in dashboard_app.query_params


def test_hero_route_ignores_stale_match_parameters(dashboard_app, dashboard_database):
    dashboard_app.query_params["view"] = "heroes"
    dashboard_app.query_params["match_id"] = "invalid"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Hero statistics"
    dashboard_database.load_default_match_id.assert_not_called()


def test_match_database_failure_does_not_block_hero_route(dashboard_app, dashboard_database):
    dashboard_database.load_default_match_id.side_effect = dashboard_database.DashboardDatabaseError(
        "Replay marts are unavailable."
    )
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    assert "Replay marts" in dashboard_app.error[0].value
    dashboard_app.radio(key="dashboard_view").set_value("heroes").run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Hero statistics"
    assert dashboard_database.load_default_match_id.call_count == 1


@pytest.mark.parametrize("view", ["unknown", ["heroes", "matches"]])
def test_invalid_section_link_does_not_query_replay_data(dashboard_app, dashboard_database, view):
    dashboard_app.query_params["view"] = view
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert "invalid dashboard section" in dashboard_app.error[0].value
    dashboard_database.load_default_match_id.assert_not_called()


def test_hero_mart_failure_does_not_block_match_navigation(dashboard_app, dashboard_database):
    dashboard_database.load_hero_statistics.side_effect = dashboard_database.DashboardDatabaseError(
        "Hero statistics mart is unavailable."
    )
    dashboard_app.run(timeout=15)
    assert "Hero statistics mart" in dashboard_app.error[0].value
    dashboard_app.radio(key="dashboard_view").set_value("matches").run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"


def test_match_search_remains_visible_when_replay_data_is_empty(dashboard_app, dashboard_database):
    dashboard_database.load_default_match_id.return_value = None
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert dashboard_app.text_input(key="match_search_input").label == "Match ID"
    assert any(button.label == "View match" for button in dashboard_app.button)
    assert "No matches" in dashboard_app.info[0].value
    assert not any(button.key and button.key.startswith("example_match_") for button in dashboard_app.button)
    dashboard_database.load_available_matches.assert_not_called()


@pytest.mark.parametrize(("value", "message"), [
    ("", "Enter a match ID"),
    ("abc", "digits only"),
    (str(2**63), "supported range"),
])
def test_invalid_search_submission_stops_before_database_queries(
    dashboard_app, dashboard_database, value, message,
):
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    dashboard_database.load_default_match_id.reset_mock()
    dashboard_database.load_scoreboard.reset_mock()
    dashboard_database.load_available_matches.reset_mock()
    dashboard_app.text_input(key="match_search_input").set_value(value)
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert message in dashboard_app.error[0].value
    assert dashboard_app.text_input(key="match_search_input").value == value
    assert not dashboard_app.metric
    dashboard_database.load_default_match_id.assert_not_called()
    dashboard_database.load_scoreboard.assert_not_called()
    dashboard_database.load_available_matches.assert_not_called()


def test_valid_search_submission_opens_the_requested_scoreboard(
    dashboard_app, dashboard_database,
):
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    dashboard_database.load_default_match_id.reset_mock()
    dashboard_database.load_scoreboard.reset_mock()
    dashboard_app.text_input(key="match_search_input").set_value(" 0009000000001 ")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.query_params["view"] == "matches"
    assert dashboard_app.query_params["match_id"] == "9000000001"
    assert dashboard_app.title[0].value == "Match overview"
    assert any(metric.label == "Match ID" and metric.value == "9000000001" for metric in dashboard_app.metric)
    dashboard_database.load_default_match_id.assert_called_once()
    assert dashboard_database.load_scoreboard.call_count == 1
    assert dashboard_database.load_scoreboard.call_args.args[1] == 9000000001

    # A fresh session follows the search result's URL without needing form state.
    from streamlit.testing.v1 import AppTest

    refreshed = AppTest.from_file(str(APP_PATH))
    refreshed.query_params.update(dashboard_app.query_params)
    refreshed.run(timeout=15)
    assert not refreshed.exception
    assert not refreshed.error
    assert refreshed.title[0].value == "Match overview"


def test_unavailable_search_keeps_the_form_and_allows_correction(dashboard_app, dashboard_database):
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    available = dashboard_database.load_scoreboard.return_value
    dashboard_database.load_scoreboard.side_effect = lambda config, match_id: (
        ({"match_id": match_id}, []) if match_id == 123 else available
    )
    dashboard_app.text_input(key="match_search_input").set_value("123")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert "Match 123 is not available" in dashboard_app.error[0].value
    assert dashboard_app.query_params["match_id"] == "123"
    assert dashboard_app.text_input(key="match_search_input").value == "123"
    assert not dashboard_app.metric

    dashboard_app.text_input(key="match_search_input").set_value("9000000001")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"


def test_search_from_player_comparison_opens_the_new_match_scoreboard(dashboard_app, dashboard_database):
    dashboard_app.query_params.update({"match_id": "9000000001", "player_slot": "0", "hero_id": "2"})
    dashboard_app.run(timeout=15)
    dashboard_app.selectbox[0].select(5).run(timeout=15)
    match, players = dashboard_database.load_scoreboard.return_value
    dashboard_database.load_scoreboard.return_value = (
        {**match, "match_id": 9000000002}, [{**player, "match_id": 9000000002} for player in players],
    )
    dashboard_database.load_player_snapshots.reset_mock()
    dashboard_database.load_player_damage.reset_mock()
    dashboard_app.text_input(key="match_search_input").set_value("9000000002")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"
    assert dashboard_app.query_params["match_id"] == "9000000002"
    assert "player_slot" not in dashboard_app.query_params
    assert "hero_id" not in dashboard_app.query_params
    assert not dashboard_app.selectbox
    dashboard_database.load_player_snapshots.assert_not_called()
    dashboard_database.load_player_damage.assert_not_called()


def test_search_in_an_empty_warehouse_does_not_offer_a_nonexistent_scoreboard(dashboard_app, dashboard_database):
    dashboard_database.load_default_match_id.return_value = None
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    dashboard_app.text_input(key="match_search_input").set_value("123")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert "Match 123 is not available" in dashboard_app.error[0].value
    assert "No analysed matches" in dashboard_app.error[0].value
    assert not any(button.key == "invalid_link_back" for button in dashboard_app.button)
    assert not dashboard_app.metric
    dashboard_database.load_scoreboard.assert_not_called()


def test_search_database_failure_is_distinct_from_an_unavailable_match(dashboard_app, dashboard_database):
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    dashboard_database.load_scoreboard.side_effect = dashboard_database.DashboardDatabaseError(
        "Could not connect to or communicate with Postgres."
    )
    dashboard_app.text_input(key="match_search_input").set_value("9000000001")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert "Postgres" in dashboard_app.error[0].value
    assert "not available" not in dashboard_app.error[0].value
    assert dashboard_app.text_input(key="match_search_input").value == "9000000001"
    assert not dashboard_app.metric


def test_example_button_recovers_from_an_unavailable_match(dashboard_app, dashboard_database):
    available = dashboard_database.load_scoreboard.return_value
    dashboard_database.load_scoreboard.side_effect = lambda config, match_id: (
        ({"match_id": match_id}, []) if match_id == 123 else available
    )
    dashboard_app.query_params.update({"view": "matches", "match_id": "123"})
    dashboard_app.run(timeout=15)
    assert "not available" in dashboard_app.error[0].value
    dashboard_app.button(key="example_match_9000000001").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"
    assert dashboard_app.query_params["view"] == "matches"
    assert dashboard_app.query_params["match_id"] == "9000000001"
    assert dashboard_app.text_input(key="match_search_input").value == "9000000001"


def test_example_button_from_player_view_clears_stale_selections(dashboard_app, dashboard_database):
    dashboard_app.query_params.update({"match_id": "9000000001", "player_slot": "0", "hero_id": "2"})
    dashboard_app.run(timeout=15)
    dashboard_database.load_player_snapshots.reset_mock()
    dashboard_database.load_player_damage.reset_mock()
    dashboard_app.button(key="example_match_9000000001").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"
    assert "player_slot" not in dashboard_app.query_params
    assert "hero_id" not in dashboard_app.query_params
    assert dashboard_app.text_input(key="match_search_input").value == "9000000001"
    dashboard_database.load_player_snapshots.assert_not_called()
    dashboard_database.load_player_damage.assert_not_called()


def test_newly_available_example_uses_its_own_match_identity(dashboard_app, dashboard_database):
    match, players = dashboard_database.load_scoreboard.return_value
    dashboard_database.load_available_matches.return_value = [9000000001, 9000000002]
    dashboard_database.load_scoreboard.side_effect = lambda config, match_id: (
        {**match, "match_id": match_id}, [{**player, "match_id": match_id} for player in players],
    )
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    assert dashboard_app.title[0].value == "Match overview"
    assert any(metric.label == "Match ID" and metric.value == "9000000001" for metric in dashboard_app.metric)
    dashboard_app.button(key="example_match_9000000002").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.query_params["match_id"] == "9000000002"
    assert any(metric.label == "Match ID" and metric.value == "9000000002" for metric in dashboard_app.metric)

    # Reopening a non-default example must preserve its ID rather than reverting to the first match.
    from streamlit.testing.v1 import AppTest

    refreshed = AppTest.from_file(str(APP_PATH))
    refreshed.query_params.update(dashboard_app.query_params)
    refreshed.run(timeout=15)
    assert not refreshed.exception
    assert not refreshed.error
    assert any(metric.label == "Match ID" and metric.value == "9000000002" for metric in refreshed.metric)


def test_example_query_failure_does_not_block_a_known_match(dashboard_app, dashboard_database):
    dashboard_database.load_available_matches.side_effect = dashboard_database.DashboardDatabaseError(
        "Example lookup failed."
    )
    dashboard_app.query_params["match_id"] = "9000000001"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert "Available examples could not be loaded" in dashboard_app.warning[0].value
    assert dashboard_app.title[0].value == "Match overview"

    dashboard_app.text_input(key="match_search_input").set_value("9000000001")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"


def test_malformed_search_can_be_corrected_without_leaving_the_page(dashboard_app):
    dashboard_app.query_params["view"] = "matches"
    dashboard_app.run(timeout=15)
    dashboard_app.text_input(key="match_search_input").set_value("invalid")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert "digits only" in dashboard_app.error[0].value
    assert not dashboard_app.metric
    dashboard_app.text_input(key="match_search_input").set_value("9000000001")
    next(button for button in dashboard_app.button if button.label == "View match").click().run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert dashboard_app.title[0].value == "Match overview"
    assert dashboard_app.query_params["match_id"] == "9000000001"
