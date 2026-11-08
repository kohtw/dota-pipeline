import json

import pytest

from dota import contract

MATCH = 9079737909
PLAYERS = [{"slot": i, "hero": f"npc_dota_hero_{i}", "team": 2 if i < 5 else 3} for i in range(10)]


def match_json(**overrides) -> str:
    data = {"match_id": MATCH, "game_mode": 22, "winner": 2, "end_time": 1, "game_start_tick": 900,
            "game_start_time": 30.0, "players": PLAYERS}
    return json.dumps({**data, **overrides})


COMBATLOG = (
    "match_id,tick,type,timestamp,attacker_name,target_name,inflictor_name,value,value_name,gold_reason\n"
    f"{MATCH},100,DOTA_COMBATLOG_DAMAGE,3.3,npc_dota_hero_axe,npc_dota_hero_lina,,57,,\n"
)
POSITIONS = f"match_id,tick,hero,team,x,y\n{MATCH},30,npc_dota_hero_axe,2,-6000.5,-5800.0\n"
NAMES = f"match_id,idx,name\n{MATCH},1,item_tango\n"


def files(**overrides: str) -> dict[str, bytes]:
    base = {"match.json": match_json(), "combatlog.csv": COMBATLOG, "positions.csv": POSITIONS, "names.csv": NAMES}
    return {contract.parsed_key(MATCH, k): v.encode() for k, v in {**base, **overrides}.items()}


def test_valid_output_passes_and_counts_rows():
    store = files()
    assert contract.check_parsed(MATCH, store.__getitem__) == \
        {"combatlog.csv": 1, "positions.csv": 1, "names.csv": 1}


def test_extra_columns_are_allowed():
    assert contract.check_csv("combatlog.csv", COMBATLOG, MATCH) == 1  # has gold_reason


def test_missing_column_fails():
    with pytest.raises(contract.ContractError, match=r"positions.csv: missing columns \['x'\]"):
        contract.check_csv("positions.csv", f"match_id,tick,hero,team,y\n{MATCH},30,h,2,1\n", MATCH)


def test_wrong_match_id_rows_fail():
    text = POSITIONS + "123,60,npc_dota_hero_axe,2,0,0\n"
    with pytest.raises(contract.ContractError, match="1 of 2 rows"):
        contract.check_csv("positions.csv", text, MATCH)


def test_empty_combatlog_fails_but_empty_names_is_allowed():
    with pytest.raises(contract.ContractError, match="no rows"):
        contract.check_csv("combatlog.csv", COMBATLOG.splitlines()[0] + "\n", MATCH)
    assert contract.check_csv("names.csv", "match_id,idx,name\n", MATCH) == 0


@pytest.mark.parametrize("overrides, message", [
    ({"match_id": 1}, "match_id is 1"),
    ({"winner": 0}, "winner is 0"),
    ({"players": PLAYERS[:9]}, "9 players"),
    ({"players": PLAYERS[:9] + [{"slot": 9, "hero": "x", "team": 4}]}, "bad player entry"),
])
def test_bad_match_json_fails(overrides: dict, message: str):
    with pytest.raises(contract.ContractError, match=message):
        contract.check_match_json(match_json(**overrides), MATCH)


def test_missing_match_json_field_fails():
    data = json.loads(match_json())
    del data["game_start_tick"]
    with pytest.raises(contract.ContractError, match="game_start_tick"):
        contract.check_match_json(json.dumps(data), MATCH)
