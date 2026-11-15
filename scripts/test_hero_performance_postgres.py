"""Build and check hero marts in a throwaway database on the local Compose Postgres.
Usage: python scripts/test_hero_performance_postgres.py
Requires the warehouse service and the existing Airflow image (dbt-postgres).
The dashboard database is never seeded or rebuilt by this script.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def compose(*args: str, input_sql: str | None = None) -> None:
    subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, input=input_sql, text=True, check=True,
    )


def expect_test_failure(dbt: tuple, settings: tuple, selection: str) -> None:
    """Require a data-test failure, not an infrastructure or compilation error."""
    result = subprocess.run(
        ["docker", "compose", *dbt, "test", *settings, "--select", selection],
        cwd=ROOT, text=True, capture_output=True,
    )
    output = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout)
    # dbt 1.12 includes data-test failures in ERROR=, so use the selected test's
    # FAIL status rather than treating the summary counter as a runtime error.
    if result.returncode != 1 or not re.search(r"\b1 of 1 FAIL [1-9][0-9]*\b", output):
        print(result.stdout, flush=True)
        print(result.stderr, flush=True)
        raise RuntimeError(f"Expected a data-test failure from {selection}")
    print(f"Confirmed expected rejection: {selection}", flush=True)


def check_matchup_failures(database: str, dbt: tuple, settings: tuple) -> None:
    """Corrupt only the temporary database and confirm the targeted tests catch each case."""
    def sql(statement: str) -> None:
        compose("exec", "-T", "warehouse", "psql", "-X", "-v", "ON_ERROR_STOP=1",
                "-U", "warehouse", "-d", database, input_sql=statement)

    cases = (
        (
            "UPDATE stg.steam_players SET player_slot = 5 WHERE match_id = 9100000001 AND player_slot = 0;",
            "UPDATE stg.steam_players SET player_slot = 0 WHERE match_id = 9100000001 AND player_slot = 5;",
            "accepted_values_stg_steam_ranked_player_slot*",
        ),
        (
            "UPDATE stg.steam_players SET hero_id = 1 WHERE match_id = 9100000001 AND player_slot = 1;",
            "UPDATE stg.steam_players SET hero_id = 2 WHERE match_id = 9100000001 AND player_slot = 1;",
            "steam_ranked_hero_unique_in_match",
        ),
        (
            "CREATE TABLE marts.matchup_test_saved AS SELECT * FROM marts.hero_matchup_summary "
            "WHERE hero_id = 1 AND opponent_hero_id = 6; "
            "DELETE FROM marts.hero_matchup_summary WHERE hero_id = 1 AND opponent_hero_id = 6;",
            "INSERT INTO marts.hero_matchup_summary SELECT * FROM marts.matchup_test_saved; "
            "DROP TABLE marts.matchup_test_saved;",
            "hero_matchup_fixture_values",
        ),
    )
    for mutation, restore, selection in cases:
        try:
            sql(mutation)
            expect_test_failure(dbt, settings, selection)
        finally:
            sql(restore)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dashboard-tests", action="store_true", help="Also run the dashboard's Postgres loader and UI checks; requires host dashboard dependencies and pytest.")
    parser.add_argument("--matchup-tests", action="store_true", help="Also check exact matchup values and confirm invalid slots, duplicate heroes and missing expected pairs are rejected.")
    args = parser.parse_args()
    # Only this newly created, randomly named database is removed in finally.
    database = f"hero_performance_test_{uuid.uuid4().hex}"
    compose("exec", "-T", "warehouse", "createdb", "-U", "warehouse", database)
    try:
        for path in (
            ROOT / "warehouse/init/04_steam_tables.sql",
            ROOT / "warehouse/init/06_constants_tables.sql",
            ROOT / "warehouse/fixtures/sample_steam.sql",
        ):
            compose(
                "exec", "-T", "warehouse", "psql", "-X", "-v", "ON_ERROR_STOP=1",
                "-U", "warehouse", "-d", database, input_sql=path.read_text(encoding="utf-8"),
            )

        dbt = (
            "run", "--rm", "--no-deps", "--pull", "never", "--entrypoint", "dbt",
            "-e", f"WAREHOUSE_DB={database}",
            "-e", "DBT_TARGET_PATH=/tmp/hero-performance-test-target",
            "-e", "DBT_LOG_PATH=/tmp/hero-performance-test-logs", "airflow-scheduler",
        )
        settings = (
            "--project-dir", "/opt/airflow/dbt", "--profiles-dir", "/opt/airflow/dbt",
            "--target", "prod", "--vars", json.dumps({
                "hero_performance_fixture_tests": True,
                "hero_matchup_fixture_tests": args.matchup_tests,
            }),
        )
        compose(*dbt, "build", *settings, "--select", "+dim_heroes")
        compose(*dbt, "build", *settings, "--select", "stg_steam_ranked+")
        if args.matchup_tests:
            check_matchup_failures(database, dbt, settings)
        if args.dashboard_tests:
            subprocess.run(
                [sys.executable, "-m", "pytest", "tests/test_hero_performance_postgres.py", "-q"],
                cwd=ROOT, check=True,
                env={**os.environ, "DASHBOARD_TEST_HERO_PERFORMANCE": "1",
                     "WAREHOUSE_HOST": "127.0.0.1", "WAREHOUSE_PORT": "5433",
                     "WAREHOUSE_DB": database, "WAREHOUSE_USER": "warehouse",
                     "WAREHOUSE_PASSWORD": "warehouse"},
            )
        print("Hero mart Postgres fixture checks passed.", flush=True)
    finally:
        compose("exec", "-T", "warehouse", "dropdb", "-U", "warehouse", "--force", database)


if __name__ == "__main__":
    main()
