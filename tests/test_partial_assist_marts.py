"""Exercise partial-assist SQL on DuckDB without a running warehouse."""
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
jinja2 = pytest.importorskip("jinja2")
ROOT = Path(__file__).resolve().parents[1]


def sql(path):
    return jinja2.Template((ROOT / "dbt" / path).read_text()).render(
        source=lambda schema, table: f"{schema}.{table}", ref=lambda table: f"marts.{table}")


@pytest.fixture
def conn():
    db = duckdb.connect()
    for path in sorted((ROOT / "warehouse/init").glob("*.sql")):
        db.execute(path.read_text())
    db.execute((ROOT / "warehouse/fixtures/sample_match.sql").read_text())
    db.execute("CREATE VIEW marts.match_scoreboard AS "
               "SELECT cast(s.match_id AS varchar) || '-' || cast(s.player_slot AS varchar) AS player_key, "
               "s.kills, s.deaths, s.assists FROM clean.player_stats s "
               "QUALIFY row_number() OVER (PARTITION BY match_id, player_slot ORDER BY tick DESC) = 1")
    yield db
    db.close()


def build_participation(conn):
    conn.execute("CREATE OR REPLACE TABLE marts.kill_participation AS "
                 + sql("models/marts/kill_participation.sql"))


def test_partial_match_keeps_totals_but_hides_participation(conn):
    conn.execute("UPDATE clean.matches SET assist_attribution_complete = FALSE, "
                 "assist_attribution_assumed = TRUE WHERE match_id = 9000000001")
    conn.execute("DELETE FROM clean.death_assists WHERE match_id = 9000000001")
    build_participation(conn)
    assert conn.execute(sql("tests/kda_matches_events.sql")).fetchall() == []
    assert conn.execute(sql("tests/kill_participation_in_range.sql")).fetchall() == []
    assert conn.execute("SELECT DISTINCT kill_participation, kill_involvements, "
                        "assist_attribution_assumed FROM marts.kill_participation "
                        "WHERE match_id = 9000000001").fetchall() == [(None, None, True)]
    assert conn.execute("SELECT assists FROM marts.match_scoreboard "
                        "WHERE player_key = '9000000001-1'").fetchone() == (2,)


@pytest.mark.parametrize("complete, mutation", [
    (True, "DELETE FROM clean.death_assists WHERE match_id = 9000000001"),
    (True, "UPDATE clean.deaths SET credited_killer_slot = NULL WHERE match_id = 9000000001 AND tick = 4800"),
    (False, "UPDATE clean.deaths SET credited_killer_slot = NULL WHERE match_id = 9000000001 AND tick = 4800"),
    (True, "UPDATE clean.deaths SET victim_slot = 9 WHERE match_id = 9000000001 AND tick = 4800"),
    (False, "UPDATE clean.deaths SET victim_slot = 9 WHERE match_id = 9000000001 AND tick = 4800"),
])
def test_complete_mismatch_or_incorrect_kills_and_deaths_still_fail(conn, complete, mutation):
    conn.execute("UPDATE clean.matches SET assist_attribution_complete = ?", [complete])
    conn.execute(mutation)
    build_participation(conn)
    assert conn.execute(sql("tests/kda_matches_events.sql")).fetchall()


def test_partial_match_cannot_publish_more_assists_than_recorded(conn):
    conn.execute("UPDATE clean.matches SET assist_attribution_complete = FALSE")
    conn.execute("UPDATE clean.player_stats SET assists = 0 WHERE player_slot = 1")
    build_participation(conn)
    assert conn.execute(sql("tests/kda_matches_events.sql")).fetchall()
