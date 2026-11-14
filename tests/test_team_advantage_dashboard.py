"""Check team chart coverage, sign conventions and player interval navigation."""

import json
import os
from copy import deepcopy
from importlib import import_module
from pathlib import Path

import pytest

from test_dashboard_app import dashboard_app


def timeline():
    rows = []
    for time in range(0, 131, 10):
        missing = time == 60
        rows.append(dict(
            observation_time_s=time, radiant_net_worth=None if missing else 3000 + 5 * time,
            dire_net_worth=3000 + 10 * time, net_worth_advantage=None if missing else -5 * time,
            radiant_players_observed=4 if missing else 5, dire_players_observed=5,
            max_snapshot_age_s=7.0 if missing else 0.0,
            net_worth_advantage_change_120s=-600 if time >= 120 else None,
        ))
    return rows


def test_team_loader_preserves_gaps_and_parameterises_match(dashboard_database, warehouse_config, warehouse_cursor):
    rows = timeline()
    warehouse_cursor.fetchall.return_value = [tuple(row[c] for c in dashboard_database.TEAM_ADVANTAGE_COLUMNS) for row in rows]
    assert dashboard_database.load_team_advantage(warehouse_config, 9000000001) == rows
    assert warehouse_cursor.execute.call_args.args[1] == (9000000001,)
    warehouse_cursor.fetchall.return_value = []
    assert dashboard_database.load_team_advantage(warehouse_config, 9000000001) == []


@pytest.mark.parametrize("changes", [
    {"observation_time_s": 10}, {"radiant_players_observed": 4}, {"radiant_net_worth": None},
    {"net_worth_advantage": 1}, {"net_worth_advantage_change_120s": 10},
    {"max_snapshot_age_s": float("nan")}, {"max_snapshot_age_s": None},
])
def test_team_loader_rejects_invalid_data(dashboard_database, warehouse_config, warehouse_cursor, changes):
    row = timeline()[0]
    row.update(changes)
    warehouse_cursor.fetchall.return_value = [tuple(row[c] for c in dashboard_database.TEAM_ADVANTAGE_COLUMNS)]
    with pytest.raises(dashboard_database.DashboardDatabaseError):
        dashboard_database.load_team_advantage(warehouse_config, 9000000001)


def test_team_chart_keeps_signs_zero_and_gaps(dashboard_database):
    charts = import_module("charts")
    rows = timeline()
    before = deepcopy(rows)
    figure = charts.build_team_advantage_chart(rows)
    assert list(figure.data[0].y) == [row["net_worth_advantage"] for row in rows]
    assert figure.data[0].y[0] == 0
    assert figure.data[0].y[6] is None
    assert not figure.data[0].connectgaps
    assert figure.layout.yaxis.zeroline
    assert "Radiant" in figure.layout.yaxis.title.text
    assert rows == before


def test_dense_progression_preserves_measurements_gaps_and_isolated_points(dashboard_database):
    charts = import_module("charts")
    rows = [dict(game_time_s=time, net_worth=time * 10) for time in range(121)]
    rows[59]["net_worth"] = None
    rows[61]["net_worth"] = None
    before = deepcopy(rows)
    figure = charts.build_progression_chart(rows, "net_worth", "Axe")
    trace = figure.data[0]
    assert list(trace.x) == list(range(121))
    assert list(trace.y) == [row["net_worth"] for row in rows]
    assert not trace.connectgaps
    assert trace.marker.size[60] == 4  # A lone observation between two gaps stays visible.
    assert trace.marker.size[30] == 0  # Dense sections do not accumulate overlapping dots.
    assert list(figure.layout.xaxis.tickvals) == [0, 30, 60, 90, 120]
    assert rows == before


def test_team_interval_updates_only_progression(dashboard_app, dashboard_database):
    app = dashboard_app
    dashboard_database.load_team_advantage.return_value = timeline()
    app.query_params.update(match_id="9000000001", player_slot="0")
    app.run(timeout=15)
    assert not app.exception
    assert not app.error
    assert len(app.get("plotly_chart")) == 5
    headings = [heading.value for heading in app.subheader]
    assert headings.index("Total XP over time") < headings.index("Team net-worth advantage")
    assert any("Complete observations: 13 of 14" in caption.value for caption in app.caption)
    final_metrics = {metric.label: metric.value for metric in app.metric}
    app.selectbox(key="team_review_interval_9000000001").select(120).run(timeout=15)
    app.button(key="apply_team_review_interval").click().run(timeout=15)
    assert not app.exception
    assert app.slider[0].value == (0, 120)
    assert {metric.label: metric.value for metric in app.metric} == final_metrics
    team_chart = json.loads(app.get("plotly_chart")[3].proto.spec)
    assert max(team_chart["data"][0]["x"]) == 130
    for chart in app.get("plotly_chart")[1:3]:
        spec = json.loads(chart.proto.spec)
        assert all(0 <= time <= 120 for trace in spec["data"] for time in trace["x"])
    app.selectbox[0].select(5).run(timeout=15)
    assert not app.exception
    assert app.slider[0].value == (0, 120)


def test_team_failure_preserves_player_page(dashboard_app, dashboard_database):
    dashboard_database.load_team_advantage.side_effect = dashboard_database.DashboardDatabaseError("missing mart")
    dashboard_app.query_params.update(match_id="9000000001", player_slot="0")
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert not dashboard_app.error
    assert any("Team advantage could not" in warning.value for warning in dashboard_app.warning)
    assert len(dashboard_app.get("plotly_chart")) == 3


def test_all_missing_and_short_timelines_have_no_review_intervals(dashboard_app, dashboard_database):
    rows = timeline()[:2]
    for row in rows:
        row.update(radiant_net_worth=None, dire_net_worth=None, net_worth_advantage=None,
                   radiant_players_observed=0, dire_players_observed=0, max_snapshot_age_s=None)
    dashboard_database.load_team_advantage.return_value = rows
    dashboard_app.query_params.update(match_id="9000000001", player_slot="0")
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert any("No complete two-minute" in caption.value for caption in dashboard_app.caption)
    assert all(box.key != "team_review_interval_9000000001" for box in dashboard_app.selectbox)


def test_team_context_available_on_match_overview(dashboard_app, dashboard_database):
    dashboard_database.load_team_advantage.return_value = timeline()
    dashboard_app.query_params["match_id"] = "9000000001"
    dashboard_app.run(timeout=15)
    assert not dashboard_app.exception
    assert dashboard_app.title[0].value == "Match overview"
    assert len(dashboard_app.get("plotly_chart")) == 2
    assert {"Radiant", "Dire"} <= {heading.value for heading in dashboard_app.subheader}
    headings = [heading.value for heading in dashboard_app.subheader]
    assert headings.index("Dire") < headings.index("Team net-worth advantage")


@pytest.mark.skipif(os.environ.get("DASHBOARD_TEST_TEAM_ADVANTAGE") != "1", reason="Opt-in real Postgres check.")
def test_live_team_timeline_and_player_interval(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import open_postgres_database
    from streamlit.testing.v1 import AppTest

    config = load_warehouse_config()
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT match_id FROM marts.team_advantage_timeline GROUP BY match_id "
                       "HAVING count(net_worth_advantage_change_120s) > 0 ORDER BY match_id LIMIT 1")
        selected = cursor.fetchone()
        assert selected, "The integration warehouse needs an analysed replay with a complete two-minute interval."
        match_id = selected[0]
        cursor.execute("SELECT " + ",".join(dashboard_database.TEAM_ADVANTAGE_COLUMNS)
                       + " FROM marts.team_advantage_timeline WHERE match_id=%s ORDER BY observation_time_s", (match_id,))
        expected = cursor.fetchall()
    records = dashboard_database.load_team_advantage(config, match_id)
    assert [tuple(row[c] for c in dashboard_database.TEAM_ADVANTAGE_COLUMNS) for row in records] == expected
    _, players = dashboard_database.load_scoreboard(config, match_id)
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))
    app.query_params.update(match_id=str(match_id), player_slot=str(players[0]["slot"]))
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    assert not app.warning
    end = next(row["observation_time_s"] for row in records if row["net_worth_advantage_change_120s"] is not None)
    app.selectbox(key=f"team_review_interval_{match_id}").select(end).run(timeout=30)
    app.button(key="apply_team_review_interval").click().run(timeout=30)
    assert not app.exception
    assert app.slider[0].value == (end - 120, end)
    for chart in app.get("plotly_chart")[1:3]:
        figure = json.loads(chart.proto.spec)
        assert all(end - 120 <= time <= end for trace in figure["data"] for time in trace["x"])
