from pathlib import Path

import pytest


APP_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def test_player_view_compares_fixture_players(dashboard_fixture_db, monkeypatch):
    testing = pytest.importorskip("streamlit.testing.v1")
    pytest.importorskip("plotly")
    monkeypatch.setenv("DUCKDB_PATH", str(dashboard_fixture_db))

    app = testing.AppTest.from_file(str(APP_PATH))
    app.query_params["match_id"] = "9000000001"
    app.query_params["player_slot"] = "0"
    app.run(timeout=15)

    assert not app.exception
    assert not app.error
    metrics = {metric.label: metric.value for metric in app.metric}
    assert metrics["Kill participation"] == "66.7% · 2 of 3 team kills"
    assert len(app.get("plotly_chart")) == 3

    app.selectbox[0].select(5).run(timeout=15)

    assert not app.exception
    assert not app.error
    stats = app.table[0].value.set_index("Metric")
    assert stats.loc["Kill participation"].tolist() == [
        "66.7% · 2 of 3 team kills", "50.0% · 1 of 2 team kills",
    ]
    assert len(app.get("plotly_chart")) == 3
