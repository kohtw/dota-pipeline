import json
from pathlib import Path

import pytest

from dota import constants

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def heroes() -> list[dict]:
    levels = {int(k): v for k, v in load("datafeed_role_levels.json").items()}
    return constants.parse_heroes(load("datafeed_herolist.json"), levels)


def test_parse_heroes_one_row_per_hero_with_typed_columns():
    rows = heroes()
    assert [r["hero_id"] for r in rows] == [1, 2, 3, 5, 14, 76]
    for r in rows:
        assert list(r) == ["hero_id", "npc_name", "localized_name", "primary_attr", "roles"]
        assert isinstance(r["hero_id"], int)
        assert r["npc_name"].startswith("npc_dota_hero_")


def test_parse_heroes_pudge_strongest_roles_first():
    pudge = next(r for r in heroes() if r["hero_id"] == 14)
    # role_levels [0, 0, 1, 2, 0, 2, 0, 0, 2]: three roles at 2, then Nuker at 1
    assert pudge == {
        "hero_id": 14,
        "npc_name": "npc_dota_hero_pudge",
        "localized_name": "Pudge",
        "primary_attr": "str",
        "roles": "Disabler|Durable|Initiator|Nuker",
    }


def test_parse_heroes_maps_all_four_primary_attributes():
    attrs = {r["localized_name"]: r["primary_attr"] for r in heroes()}
    assert (attrs["Axe"], attrs["Anti-Mage"], attrs["Crystal Maiden"], attrs["Bane"]) == ("str", "agi", "int", "all")


def test_parse_heroes_uses_valve_current_name():
    assert next(r for r in heroes() if r["hero_id"] == 76)["localized_name"] == "Outworld Destroyer"


def test_parse_heroes_rejects_a_changed_role_list():
    levels = {int(k): v for k, v in load("datafeed_role_levels.json").items()}
    levels[14] = levels[14] + [1]
    with pytest.raises(ValueError, match="hero 14"):
        constants.parse_heroes(load("datafeed_herolist.json"), levels)


def test_parse_heroes_rejects_an_unknown_primary_attribute():
    herolist = load("datafeed_herolist.json")
    herolist["result"]["data"]["heroes"][0]["primary_attr"] = 4
    levels = {int(k): v for k, v in load("datafeed_role_levels.json").items()}
    with pytest.raises(KeyError):
        constants.parse_heroes(herolist, levels)


def test_parse_items_flags_recipes_and_neutral_tiers():
    rows = {r["item_id"]: r for r in constants.parse_items(load("datafeed_itemlist.json"))}
    assert list(rows) == [1, 246, 574, 606]
    assert rows[1] == {"item_id": 1, "name": "item_blink", "localized_name": "Blink Dagger",
                       "is_recipe": False, "neutral_tier": None}
    assert rows[574]["neutral_tier"] == 2
    assert rows[606]["is_recipe"] and rows[246]["is_recipe"]


def test_parse_patches_oldest_first_in_utc():
    raw = load("datafeed_patchnoteslist.json")
    raw["patches"].reverse()
    assert constants.parse_patches(raw) == [
        {"patch": "7.08", "released_at": "2018-02-01 08:00:00"},
        {"patch": "7.41e", "released_at": "2026-07-30 07:00:00"},
        {"patch": "7.41f", "released_at": "2026-09-15 07:00:00"},
    ]


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
