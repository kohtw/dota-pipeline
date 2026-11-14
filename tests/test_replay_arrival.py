"""Replays that arrive wake parse_pending at once: download_pending and every cold_restore release
emit REPLAYS_ARRIVED, and parse_pending is scheduled on it or the hourly fallback. Airflow is not
installed on the host, so these read the DAG files' source."""

import ast
import importlib
import sys
import types
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"


def calls(path: str) -> list[ast.Call]:
    tree = ast.parse((DAGS / path).read_text())
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)]


def keyword(call: ast.Call, name: str) -> str | None:
    return next((ast.unparse(k.value) for k in call.keywords if k.arg == name), None)


def named(path: str, func: str) -> list[ast.Call]:
    return [c for c in calls(path) if ast.unparse(c.func) == func]


def task_decorator(path: str, function: str) -> ast.Call:
    tree = ast.parse((DAGS / path).read_text())
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == function)
    return next(d for d in fn.decorator_list if isinstance(d, ast.Call) and ast.unparse(d.func) == "task")


def test_parse_pending_runs_on_arrival_or_hourly():
    (dag,) = named("parse_pending.py", "dag")
    schedule = keyword(dag, "schedule")
    assert schedule.startswith("AssetOrTimeSchedule(")
    assert "assets=REPLAYS_ARRIVED" in schedule
    assert "CronTriggerTimetable('0 * * * *', timezone='UTC')" in schedule
    assert keyword(dag, "max_active_runs") == "1"


def test_download_pending_emits_an_arrival_per_batch():
    assert keyword(task_decorator("download_pending.py", "download_replays"), "outlets") == "[REPLAYS_ARRIVED]"


@pytest.fixture
def assets(monkeypatch):
    class StubAsset:
        def __init__(self, uri):
            self.uri = uri

    sdk = types.ModuleType("airflow.sdk")
    sdk.Asset = StubAsset
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    monkeypatch.delitem(sys.modules, "dota.assets", raising=False)
    yield importlib.import_module("dota.assets")
    sys.modules.pop("dota.assets", None)


def test_replays_arrived_names_the_hot_replay_prefix(assets):
    assert assets.REPLAYS_ARRIVED.uri == "s3://raw/replays"
