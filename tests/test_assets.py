"""The shared Asset URIs must satisfy the Airflow providers' URI rules.

The postgres provider rejects a URI whose path is not /database/schema/table, which
made every DAG importing dota.assets fail to load. Airflow is not needed here: a stub
stands in for airflow.sdk.Asset and keeps the URI string.
"""

import importlib
import sys
import types
from urllib.parse import urlsplit

import pytest


class _StubAsset:
    def __init__(self, uri: str) -> None:
        self.uri = uri


@pytest.fixture
def assets(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    sdk = types.ModuleType("airflow.sdk")
    sdk.Asset = _StubAsset
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    monkeypatch.delitem(sys.modules, "dota.assets", raising=False)
    module = importlib.import_module("dota.assets")
    yield module
    sys.modules.pop("dota.assets", None)


@pytest.mark.parametrize(
    ("name", "scheme"),
    [("RAW_PARSED", "s3"), ("CLEAN_READY", "s3"), ("CLEAN_LOADED", "postgres"), ("STEAM_LOADED", "postgres")],
)
def test_asset_uri_scheme(assets: types.ModuleType, name: str, scheme: str) -> None:
    assert urlsplit(getattr(assets, name).uri).scheme == scheme


def test_clean_loaded_names_database_schema_and_table(assets: types.ModuleType) -> None:
    parsed = urlsplit(assets.CLEAN_LOADED.uri)
    segments = parsed.path.split("/")
    assert segments[0] == ""
    assert segments[1:] == ["warehouse", "clean", "matches"]
    assert all(segments[1:])
    assert parsed.hostname == "warehouse"


def test_steam_loaded_names_database_schema_and_table(assets: types.ModuleType) -> None:
    parsed = urlsplit(assets.STEAM_LOADED.uri)
    segments = parsed.path.split("/")
    assert segments[0] == ""
    assert segments[1:] == ["warehouse", "stg", "steam_matches"]
    assert all(segments[1:])
    assert parsed.hostname == "warehouse"
