"""Fresh and upgraded warehouse schemas; no services or network required."""
from pathlib import Path
import subprocess
import sys

import pytest

duckdb = pytest.importorskip("duckdb")
ROOT = Path(__file__).resolve().parents[1]
INIT = ROOT / "warehouse" / "init"


def test_fresh_dev_database_has_credit_columns_and_updated_fixture(tmp_path):
    path = tmp_path / "fixture.duckdb"
    subprocess.run([sys.executable, str(ROOT / "warehouse/make_dev_db.py"), str(path)], check=True)
    with duckdb.connect(str(path)) as conn:
        assert conn.execute("SELECT count(*) FROM clean.deaths WHERE match_id = 9000000001 "
                            "AND link_status = 'exact' AND credited_killer_slot IS NOT NULL").fetchone()[0] == 5
        assert conn.execute("SELECT credited_killer_slot FROM clean.deaths WHERE match_id = 9000000001 "
                            "AND tick = 4800 AND killer_slot IS NULL").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM clean.death_assists WHERE match_id = 9000000001 "
                            "AND same_side = FALSE").fetchone()[0] == 3
        assert conn.execute("SELECT count(*) FROM clean.death_assists WHERE same_side = TRUE").fetchone()[0] == 1
        # Repeating the migration must not reset meaningful credit or labels.
        migration = (INIT / "02_clean_tables.sql").read_text()
        conn.execute(migration)
        conn.execute(migration)
        assert conn.execute("SELECT count(*) FROM clean.deaths WHERE link_status IS NULL").fetchone()[0] == 0


def test_upgrade_preserves_unknown_legacy_credit_without_guessing():
    old = (INIT / "02_clean_tables.sql").read_text()
    old = "\n".join(line for line in old.splitlines()
                    if not any(name in line for name in ["credited_killer_slot", "link_status", "same_side"]))
    with duckdb.connect() as conn:
        conn.execute((INIT / "01_schemas.sql").read_text())
        conn.execute(old)
        conn.execute("INSERT INTO clean.deaths VALUES (1, 80, 1.0, 5, NULL, 'tower')")
        conn.execute("INSERT INTO clean.death_assists VALUES (1, 80, 5, 6)")
        migration = (INIT / "02_clean_tables.sql").read_text()
        conn.execute(migration)
        conn.execute(migration)
        assert conn.execute("SELECT killer_name, credited_killer_slot, link_status FROM clean.deaths").fetchone() == (
            "tower", None, None)
        assert conn.execute("SELECT same_side FROM clean.death_assists").fetchone() == (None,)
        # Re-cleaned replacement data can preserve tower last hit and player credit separately.
        conn.execute("DELETE FROM clean.deaths WHERE match_id = 1")
        conn.execute("INSERT INTO clean.deaths "
                     "(match_id, tick, game_time_s, victim_slot, killer_slot, killer_name, credited_killer_slot, link_status) "
                     "VALUES (1, 80, 1.0, 5, NULL, 'tower', 0, 'participant_assumed')")
        assert conn.execute("SELECT killer_slot, credited_killer_slot, link_status FROM clean.deaths").fetchone() == (
            None, 0, "participant_assumed")
