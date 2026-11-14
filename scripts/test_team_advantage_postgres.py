"""Build and check team advantage fixtures in a temporary Compose Postgres database.
Usage: python scripts/test_team_advantage_postgres.py
Requires the warehouse service and existing Airflow image. Does not seed the dashboard database.
"""

import json
import re
import subprocess
import uuid

from test_hero_performance_postgres import ROOT, compose, expect_test_failure


def main() -> None:
    database = f"team_advantage_test_{uuid.uuid4().hex}"
    compose("exec", "-T", "warehouse", "createdb", "-U", "warehouse", database)
    try:
        def sql(statement: str) -> None:
            compose("exec", "-T", "warehouse", "psql", "-X", "-v", "ON_ERROR_STOP=1",
                    "-U", "warehouse", "-d", database, input_sql=statement)

        for relative in ("warehouse/init/01_schemas.sql", "warehouse/init/02_clean_tables.sql",
                         "warehouse/fixtures/sample_team_advantage.sql"):
            sql((ROOT / relative).read_text(encoding="utf-8"))

        dbt = (
            "run", "--rm", "--no-deps", "--pull", "never", "--entrypoint", "dbt",
            "-e", f"WAREHOUSE_DB={database}", "-e", "DBT_TARGET_PATH=/tmp/team-advantage-test-target",
            "-e", "DBT_LOG_PATH=/tmp/team-advantage-test-logs", "airflow-scheduler",
        )
        settings = ("--project-dir", "/opt/airflow/dbt", "--profiles-dir", "/opt/airflow/dbt",
                    "--target", "prod", "--vars", json.dumps({"team_advantage_fixture_tests": True}))
        compose(*dbt, "build", *settings, "--select", "stg_replay_player_observations+")

        for mutate, restore in (
            ("UPDATE clean.matches SET duration_s = 0 WHERE match_id = 9200000001;",
             "UPDATE clean.matches SET duration_s = 135 WHERE match_id = 9200000001;"),
            ("UPDATE clean.players SET team = 3 WHERE match_id = 9200000001 AND player_slot = 0;",
             "UPDATE clean.players SET team = 2 WHERE match_id = 9200000001 AND player_slot = 0;"),
        ):
            try:
                sql(mutate)
                expect_test_failure(dbt, settings, "replay_advantage_inputs_valid")
            finally:
                sql(restore)

        # Existing clean-table primary keys should reject duplicate snapshots before dbt runs.
        duplicate = subprocess.run(
            ["docker", "compose", "exec", "-T", "warehouse", "psql", "-X", "-v", "ON_ERROR_STOP=1",
             "-v", "VERBOSITY=verbose", "-U", "warehouse", "-d", database], cwd=ROOT,
            input="INSERT INTO clean.player_stats SELECT * FROM clean.player_stats "
                  "WHERE match_id = 9200000001 AND player_slot = 0 AND tick = 300;",
            text=True, capture_output=True,
        )
        if duplicate.returncode != 3 or not re.search(r"\b23505\b", duplicate.stderr):
            raise RuntimeError("Expected Postgres to reject the duplicate snapshot key: " + duplicate.stderr)
        print("Confirmed expected rejection: duplicate snapshot key (23505)", flush=True)

        try:
            sql("CREATE TABLE marts.timeline_test_saved AS SELECT * FROM marts.team_advantage_timeline "
                "WHERE match_id = 9200000001 AND observation_time_s = 120; "
                "DELETE FROM marts.team_advantage_timeline WHERE match_id = 9200000001 AND observation_time_s = 120;")
            expect_test_failure(dbt, settings, "team_advantage_fixture_values")
        finally:
            sql("INSERT INTO marts.team_advantage_timeline SELECT * FROM marts.timeline_test_saved; "
                "DROP TABLE marts.timeline_test_saved;")
        print("Team advantage Postgres fixtures and rejection checks passed.", flush=True)
    finally:
        # Remove only this database, whose name was generated above for this run.
        compose("exec", "-T", "warehouse", "dropdb", "-U", "warehouse", "--force", database)


if __name__ == "__main__":
    main()
