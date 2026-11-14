"""Read-only hero integration checks, independent of replay availability."""

import os
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("DASHBOARD_TEST_HERO_PERFORMANCE") != "1",
    reason="Set DASHBOARD_TEST_HERO_PERFORMANCE=1 and WAREHOUSE_* for Postgres checks.",
)


def test_profiles_agree_with_source_sql(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import open_postgres_database

    config = load_warehouse_config()
    heroes = dashboard_database.load_hero_statistics(config)
    for hero in heroes:
        profile = dashboard_database.load_hero_performance(config, hero["hero_id"])
        assert profile is not None
        assert profile["appearances"] == hero["picks"]
        assert profile["wins"] == hero["wins"]
        with open_postgres_database(config) as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT count(*), avg(p.gold_per_min), count(p.gold_per_min),
                          avg(p.hero_damage / (m.duration / 60.0)), count(p.hero_damage)
                   FROM stg.steam_players p JOIN stg.steam_matches m USING (match_id)
                   WHERE m.lobby_type = 7 AND m.game_mode = 22 AND m.duration >= 900
                     AND m.human_players = 10 AND m.radiant_win IS NOT NULL AND p.hero_id = %s""",
                (hero["hero_id"],),
            )
            appearances, gpm, gpm_count, damage, damage_count = cursor.fetchone()
        assert profile["appearances"] == appearances
        assert profile["gpm_observations"] == gpm_count
        assert profile["hero_damage_observations"] == damage_count
        assert profile["avg_gpm"] == (None if gpm is None else pytest.approx(float(gpm)))
        assert profile["avg_hero_damage_per_min"] == (None if damage is None else pytest.approx(float(damage)))
    assert dashboard_database.load_hero_performance(config, 2147483647) is None


def test_live_hero_profile_navigation(dashboard_database):
    from config import load_warehouse_config
    from streamlit.testing.v1 import AppTest

    heroes = dashboard_database.load_hero_statistics(load_warehouse_config())
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))
    app.run(timeout=30)
    assert not app.exception
    assert not app.error
    if not heroes:
        assert app.info[0].value == "No hero statistics available yet."
        return
    for hero in heroes[:2]:
        app.selectbox(key="hero_stats_selected_id").select(hero["hero_id"]).run(timeout=30)
        assert not app.exception
        assert not app.error
        assert not app.warning
        assert app.query_params["hero_id"] == str(hero["hero_id"])
        assert any(heading.value == "Performance profile" for heading in app.subheader)
        assert any(metric.label == "Gold / min" for metric in app.metric)


def test_live_matchup_loader_and_filters(dashboard_database):
    from config import load_warehouse_config
    from postgres_connection import open_postgres_database
    from streamlit.testing.v1 import AppTest

    config = load_warehouse_config()
    heroes = dashboard_database.load_hero_statistics(config)
    for hero in heroes:
        matchups = dashboard_database.load_hero_matchups(config, hero["hero_id"])
        with open_postgres_database(config) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT opponent_hero_id,matches,wins,losses,win_rate,overall_matches,"
                "overall_win_rate,win_rate_difference_pp FROM marts.hero_matchup_summary "
                "WHERE hero_id=%s ORDER BY matches DESC,opponent_hero_id", (hero["hero_id"],),
            )
            expected = cursor.fetchall()
        assert [tuple(row[key] for key in ("opponent_hero_id", "matches", "wins", "losses", "win_rate",
                                          "overall_matches", "overall_win_rate", "win_rate_difference_pp"))
                for row in matchups] == expected
    if not heroes:
        return
    selected_hero = next((hero for hero in heroes if dashboard_database.load_hero_matchups(config, hero["hero_id"])), None)
    if selected_hero is None:
        app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))
        app.run(timeout=30)
        assert not app.exception
        assert any("No opponent results" in caption.value for caption in app.caption)
        return
    rows = dashboard_database.load_hero_matchups(config, selected_hero["hero_id"])
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py"))
    app.query_params.update(view="heroes", hero_id=str(selected_hero["hero_id"]))
    app.run(timeout=30)
    assert not app.exception
    assert not app.warning
    scope = f"hero_matchups_{selected_hero['hero_id']}"
    app.selectbox(key=f"{scope}_opponent").select(rows[0]["opponent_hero_id"]).run(timeout=30)
    assert not app.exception
    assert any(metric.label == "Matchup matches" and metric.value == f"{rows[0]['matches']:,}" for metric in app.metric)
    app.number_input(key=f"{scope}_minimum").set_value(max(row["matches"] for row in rows) + 1).run(timeout=30)
    assert any("No opponents meet" in notice.value for notice in app.info)
    assert app.query_params["hero_id"] == str(selected_hero["hero_id"])
