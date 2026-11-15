"""Every warehouse writer and the dbt build share the one-slot pool warehouse_write, so a load never
commits during a build. Airflow is not installed on the host, so these read the DAG files' source."""

import re
from pathlib import Path

import pytest

from test_replay_arrival import keyword, task_decorator

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("path, function", [
    ("load_pending.py", "apply_schema"),
    ("load_pending.py", "load_one"),
    ("dbt_build.py", "apply_schema"),
    ("dbt_build.py", "build"),
    ("steam_summary_elt.py", "load_pages"),
])
def test_warehouse_writers_share_one_pool(path, function):
    assert keyword(task_decorator(path, function), "pool") == "'warehouse_write'"


def test_airflow_init_creates_the_pool_with_one_slot():
    compose = (ROOT / "docker-compose.yml").read_text()
    assert re.search(r"airflow pools set warehouse_write 1 '[^']+'", compose)
