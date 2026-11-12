import json
from pathlib import Path

from dota import constants

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_parse_heroes_one_row_per_source_entry_with_typed_columns():
    raw = load("constants_heroes.json")
    rows = constants.parse_heroes(raw)
    assert len(rows) == len(raw)
    assert [r["hero_id"] for r in rows] == sorted(r["hero_id"] for r in rows)
    for r in rows:
        assert list(r) == ["hero_id", "npc_name", "localized_name", "primary_attr", "roles"]
        assert isinstance(r["hero_id"], int)
        assert all(isinstance(r[k], str) for k in list(r)[1:])
        assert r["npc_name"].startswith("npc_dota_hero_")


def test_parse_heroes_pudge_and_roles_joined_with_pipe():
    pudge = next(r for r in constants.parse_heroes(load("constants_heroes.json")) if r["hero_id"] == 14)
    assert pudge == {
        "hero_id": 14,
        "npc_name": "npc_dota_hero_pudge",
        "localized_name": "Pudge",
        "primary_attr": "str",
        "roles": "Disabler|Initiator|Durable|Nuker",
    }


def test_parse_game_modes_one_row_per_source_entry_with_typed_columns():
    raw = load("constants_game_mode.json")
    rows = constants.parse_game_modes(raw)
    assert len(rows) == len(raw)
    assert [r["game_mode_id"] for r in rows] == sorted(r["game_mode_id"] for r in rows)
    for r in rows:
        assert list(r) == ["game_mode_id", "name", "label"]
        assert isinstance(r["game_mode_id"], int)
        assert r["name"].startswith("game_mode_")


def test_parse_game_modes_all_draft_label():
    rows = {r["game_mode_id"]: r for r in constants.parse_game_modes(load("constants_game_mode.json"))}
    assert rows[22] == {"game_mode_id": 22, "name": "game_mode_all_draft", "label": "All Draft"}


def test_parse_game_modes_label_keeps_digit_words_lowercase():
    rows = {r["game_mode_id"]: r for r in constants.parse_game_modes(load("constants_game_mode.json"))}
    assert rows[21]["label"] == "1v1 Mid"
