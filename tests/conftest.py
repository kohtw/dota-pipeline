"""Shared dashboard records and offline warehouse doubles."""

from importlib import import_module
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def dashboard_database(monkeypatch):
    """Import loaders as Streamlit does, without starting the interface."""
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "dashboard"))
    return import_module("dashboard_db")


@pytest.fixture
def warehouse_config(dashboard_database):
    from config import load_warehouse_config

    return load_warehouse_config({"WAREHOUSE_PASSWORD": "offline-test-password"})


@pytest.fixture
def scoreboard_rows():
    """Represent a controlled mart result with both teams and optional statistics."""
    players = [
        (0, "antimage", 2, 2, 1, 0, 6000, 400, ["Blink Dagger", "Desolator"], True, 2 / 3, 3, 2),
        (1, "crystal_maiden", 2, 1, 1, 2, 3000, 200, None, True, 1.0, 3, 3),
        (5, "axe", 3, 1, 2, 0, 5000, 300, None, False, 0.5, 2, 1),
        (6, "lina", 3, 1, 1, 1, 3500, 250, None, False, 1.0, 2, 2),
    ]
    return [
        dict(match_id=9000000001, slot=slot, hero=f"npc_dota_hero_{hero}",
             hero_label={"antimage": "Anti-Mage", "crystal_maiden": "Crystal Maiden", "axe": "Axe", "lina": "Lina"}[hero], team=team,
             kills=kills, deaths=deaths, assists=assists, last_hits=80, denies=10,
             net_worth=net_worth, gold=gold, final_items=items, duration_s=600.0,
             won=won, kill_participation=kp, team_kills=team_kills, kill_involvements=involvements,
             assist_attribution_complete=True, assist_attribution_assumed=False)
        for slot, hero, team, kills, deaths, assists, net_worth, gold, items, won, kp, team_kills, involvements
        in players
    ]


@pytest.fixture
def warehouse_cursor(dashboard_database, monkeypatch):
    """Supply cursor results without opening a network connection."""
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    manager = MagicMock()
    manager.__enter__.return_value = connection
    opener = MagicMock(return_value=manager)
    monkeypatch.setattr(dashboard_database, "open_postgres_database", opener)
    return cursor


@pytest.fixture
def scoreboard_cursor(warehouse_cursor, scoreboard_rows):
    warehouse_cursor.description = [(column,) for column in scoreboard_rows[0]]
    warehouse_cursor.fetchall.return_value = [tuple(row.values()) for row in scoreboard_rows]
    return warehouse_cursor
