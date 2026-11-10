import shutil
from pathlib import Path

import pytest


MATCH_ID = 9000000001


@pytest.fixture
def scoreboard_loader(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "dashboard"))
    from dashboard_db import load_scoreboard

    return load_scoreboard


def test_scoreboard_maps_fixture_rows(dashboard_fixture_db, scoreboard_loader):
    match, players = scoreboard_loader(str(dashboard_fixture_db), MATCH_ID)

    assert match == {"match_id": MATCH_ID, "duration_s": 600, "winner_team": 2}
    assert [(player["slot"], player["team"]) for player in players] == [(0, 2), (1, 2), (5, 3), (6, 3)]
    antimage = players[0]
    assert antimage["hero"] == "npc_dota_hero_antimage"
    assert antimage["kills"] == 2
    assert antimage["net_worth"] == 6000
    assert antimage["gold"] == 400
    assert antimage["active_items"] == ["Blink", "Desolator"]
    assert antimage["kill_participation"] == pytest.approx(2 / 3)


def test_scoreboard_distinguishes_zero_and_unavailable_kp(dashboard_fixture_db, scoreboard_loader, tmp_path):
    import duckdb

    pytest.importorskip("streamlit")
    pytest.importorskip("plotly")
    from components import format_percentage
    from player import _kill_participation_detail

    database = tmp_path / "kp-cases.duckdb"
    shutil.copyfile(dashboard_fixture_db, database)
    with duckdb.connect(str(database)) as connection:
        connection.execute("""
            UPDATE marts.kill_participation
            SET kills_from_events = 0, assists_from_events = 0, kill_participation = 0
            WHERE match_id = ? AND player_slot = 0
        """, [MATCH_ID])
        connection.execute("""
            UPDATE marts.kill_participation
            SET team_kills = 0, kills_from_events = 0, assists_from_events = 0, kill_participation = NULL
            WHERE match_id = ? AND team = 3
        """, [MATCH_ID])
        connection.execute("""
            DELETE FROM marts.kill_participation WHERE match_id = ? AND player_slot = 1
        """, [MATCH_ID])

    _, players = scoreboard_loader(str(database), MATCH_ID)
    by_slot = {player["slot"]: player for player in players}
    assert set(by_slot) == {0, 1, 5, 6}
    assert format_percentage(by_slot[0]["kill_participation"]) == "0.0%"
    assert _kill_participation_detail(by_slot[0]) == "0.0% · 0 of 3 team kills"
    assert _kill_participation_detail(by_slot[5]) == "— · Team had zero kills"
    assert _kill_participation_detail(by_slot[1]) == "— · Unavailable"
