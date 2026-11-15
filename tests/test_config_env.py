import importlib

import pytest

from dota import config


def test_env_int_defaults_when_unset_or_blank(monkeypatch):
    monkeypatch.delenv("PARSE_BATCH", raising=False)
    assert config.env_int("PARSE_BATCH", 20) == 20
    monkeypatch.setenv("PARSE_BATCH", "  ")
    assert config.env_int("PARSE_BATCH", 20) == 20


def test_env_int_reads_override(monkeypatch):
    monkeypatch.setenv("PARALLEL_PARSES", "6")
    assert config.env_int("PARALLEL_PARSES", 2) == 6


@pytest.mark.parametrize("raw", ["0", "-3", "two"])
def test_env_int_rejects_bad_values(monkeypatch, raw):
    monkeypatch.setenv("PARALLEL_CLEANS", raw)
    with pytest.raises(ValueError):
        config.env_int("PARALLEL_CLEANS", 2)


def test_laptop_defaults(monkeypatch):
    for name in ("PARSE_BATCH", "PARALLEL_PARSES", "CLEAN_BATCH", "PARALLEL_CLEANS", "LOAD_BATCH"):
        monkeypatch.delenv(name, raising=False)
    fresh = importlib.reload(config)
    assert (fresh.PARSE_BATCH, fresh.PARALLEL_PARSES, fresh.CLEAN_BATCH,
            fresh.PARALLEL_CLEANS, fresh.LOAD_BATCH) == (20, 2, 20, 2, 20)


def test_compose_passes_every_knob_to_airflow():
    from pathlib import Path
    compose = (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text()
    for name in ("PARSE_BATCH", "PARALLEL_PARSES", "CLEAN_BATCH", "PARALLEL_CLEANS", "LOAD_BATCH"):
        assert f"{name}: ${{{name}:-}}" in compose
