"""refresh_constants and dbt_build wiring, with a stub airflow.sdk that records each DAG's arguments
and tasks (Airflow is not installed on the host)."""

import importlib.util
import sys
import types
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"


class StubAsset:
    def __init__(self, uri: str) -> None:
        self.uri = uri

    def __or__(self, other):
        return StubAny(self, other)


class StubAny:
    def __init__(self, *items) -> None:
        self.assets = [a for i in items for a in (i.assets if isinstance(i, StubAny) else [i])]

    def __or__(self, other):
        return StubAny(self, other)


class Skip(Exception):
    pass


class TaskOutput:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id

    def __rshift__(self, other):
        return other


def load_dag(monkeypatch: pytest.MonkeyPatch, filename: str) -> dict:
    """Import a DAG file under the stub and return {"kwargs": dag kwargs, "tasks": {id: task}}."""
    building: list[dict] = []
    built: dict = {}

    def dag(**dag_kwargs):
        def decorate(fn):
            def build():
                building.append({})
                fn()
                built.update(kwargs=dag_kwargs, tasks=building.pop())
            return build
        return decorate

    def task(fn=None, **task_kwargs):
        def decorate(f):
            def call(*args):
                building[-1][f.__name__] = {"kwargs": task_kwargs, "fn": f,
                                            "upstream": [a.task_id for a in args if isinstance(a, TaskOutput)]}
                return TaskOutput(f.__name__)
            return call
        return decorate(fn) if fn else decorate

    sdk = types.ModuleType("airflow.sdk")
    sdk.Asset, sdk.dag, sdk.task = StubAsset, dag, task
    exceptions = types.ModuleType("airflow.sdk.exceptions")
    exceptions.AirflowSkipException = Skip
    exceptions.AirflowFailException = type("AirflowFailException", (Exception,), {})
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    monkeypatch.setitem(sys.modules, "airflow.sdk.exceptions", exceptions)
    monkeypatch.delitem(sys.modules, "dota.assets", raising=False)
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), DAGS / filename)
    spec.loader.exec_module(importlib.util.module_from_spec(spec))
    sys.modules.pop("dota.assets", None)
    return built


def test_refresh_constants_is_daily_paused_and_alerting(monkeypatch):
    kwargs = load_dag(monkeypatch, "refresh_constants.py")["kwargs"]
    assert kwargs["schedule"] == "@daily"
    assert kwargs["catchup"] is False and kwargs["max_active_runs"] == 1
    assert kwargs["is_paused_upon_creation"] is True
    assert {"on_failure_callback", "on_retry_callback"} <= set(kwargs["default_args"])


def test_refresh_constants_fetches_then_loads_and_load_emits_constants_loaded(monkeypatch):
    from dota.config import API_RETRY

    tasks = load_dag(monkeypatch, "refresh_constants.py")["tasks"]
    assert list(tasks) == ["fetch", "load"]
    assert tasks["load"]["upstream"] == ["fetch"]
    assert tasks["fetch"]["kwargs"]["retries"] == API_RETRY["retries"]
    assert [a.uri for a in tasks["load"]["kwargs"]["outlets"]] == [
        "postgres://warehouse:5432/warehouse/stg/constant_heroes"]


def test_load_skips_when_nothing_changed(monkeypatch):
    import dota
    from dota import constants_load, warehouse

    tasks = load_dag(monkeypatch, "refresh_constants.py")["tasks"]
    storage = types.SimpleNamespace(read=lambda key: b"")
    monkeypatch.setitem(sys.modules, "dota.storage", storage)
    monkeypatch.setattr(dota, "storage", storage, raising=False)
    monkeypatch.setattr(warehouse, "connect", lambda: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(constants_load, "load_saved_snapshot",
                        lambda prefix, read, conn, ddl: {"heroes": 0, "items": 0, "patches": 0, "game_modes": 0})
    with pytest.raises(Skip):
        tasks["load"]["fn"]("constants/fetched_at=20261101T030000Z/")
    monkeypatch.setattr(constants_load, "load_saved_snapshot",
                        lambda prefix, read, conn, ddl: {"heroes": 1, "items": 0, "patches": 0, "game_modes": 0})
    assert tasks["load"]["fn"]("constants/fetched_at=20261101T030000Z/")["heroes"] == 1


def test_dbt_build_also_runs_on_constants_loaded(monkeypatch):
    schedule = load_dag(monkeypatch, "dbt_build.py")["kwargs"]["schedule"]
    assert "postgres://warehouse:5432/warehouse/stg/constant_heroes" in [a.uri for a in schedule.assets]
