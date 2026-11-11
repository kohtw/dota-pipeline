import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def dashboard_fixture_db(tmp_path_factory):
    """Build the actual dbt marts once in a database dedicated to this test run."""
    pytest.importorskip("duckdb")
    pytest.importorskip("dbt.adapters.duckdb")

    directory = tmp_path_factory.mktemp("dashboard")
    database = directory / "fixture.duckdb"
    shutil.copyfile(REPO_ROOT / "dbt" / "profiles.yml", directory / "profiles.yml")
    env = {
        **os.environ,
        "DUCKDB_PATH": str(database),
        "DBT_TARGET_PATH": str(directory / "target"),
        "DBT_LOG_PATH": str(directory / "logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }

    commands = (
        ["warehouse/make_dev_db.py", str(database)],
        ["-c", "from dbt.cli.main import cli; cli()", "build",
         "--project-dir", "dbt", "--profiles-dir", str(directory), "--target", "dev"],
    )
    for command in commands:
        result = subprocess.run(
            [sys.executable, *command], cwd=REPO_ROOT, env=env,
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=90,
        )
        if result.returncode:
            pytest.fail(f"Dashboard fixture setup failed.\n{result.stdout}\n{result.stderr}")

    return database
