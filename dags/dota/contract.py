"""Checks a match's parsed output against docs/data-contract.md before anything downstream
reads it, so a parser change that breaks the contract fails here, not in Spark or dbt."""

import csv
import io
import json
from typing import Callable

PARSED_FILES = ("match.json", "combatlog.csv", "positions.csv", "names.csv")

# Required columns only: the contract allows new columns, so extra ones are fine.
REQUIRED_COLUMNS = {
    "combatlog.csv": ("match_id", "tick", "type", "timestamp", "attacker_name", "target_name",
                      "inflictor_name", "value", "value_name"),
    "positions.csv": ("match_id", "tick", "hero", "team", "x", "y"),
    "names.csv": ("match_id", "idx", "name"),
}
NON_EMPTY = ("combatlog.csv", "positions.csv")  # a real match always has these rows
MATCH_FIELDS = ("match_id", "game_mode", "winner", "end_time", "game_start_tick", "game_start_time", "players")
PLAYER_FIELDS = ("slot", "hero", "team")
TEAMS = (2, 3)


class ContractError(Exception):
    pass


def parsed_key(match_id: int, name: str) -> str:
    return f"parsed/{match_id}/{name}"


def check_csv(name: str, text: str, match_id: int) -> int:
    """Checks one CSV's header and match_id column. Returns its row count."""
    reader = csv.DictReader(io.StringIO(text))
    missing = [c for c in REQUIRED_COLUMNS[name] if c not in (reader.fieldnames or [])]
    if missing:
        raise ContractError(f"{name}: missing columns {missing}")
    rows, wrong = 0, 0
    for row in reader:
        rows += 1
        if row["match_id"] != str(match_id):
            wrong += 1
    if wrong:
        raise ContractError(f"{name}: {wrong} of {rows} rows have a match_id other than {match_id}")
    if name in NON_EMPTY and rows == 0:
        raise ContractError(f"{name}: no rows")
    return rows


def check_match_json(text: str, match_id: int) -> None:
    data = json.loads(text)
    missing = [f for f in MATCH_FIELDS if f not in data]
    if missing:
        raise ContractError(f"match.json: missing fields {missing}")
    if data["match_id"] != match_id:
        raise ContractError(f"match.json: match_id is {data['match_id']}, expected {match_id}")
    if data["winner"] not in TEAMS:
        raise ContractError(f"match.json: winner is {data['winner']}, expected 2 or 3")
    players = data["players"]
    if len(players) != 10:
        raise ContractError(f"match.json: {len(players)} players, expected 10")
    for p in players:
        if any(f not in p for f in PLAYER_FIELDS) or p["team"] not in TEAMS:
            raise ContractError(f"match.json: bad player entry {p}")


def check_parsed(match_id: int, read: Callable[[str], bytes]) -> dict[str, int]:
    """Checks all four parsed files for one match. `read(key)` returns an object's bytes.
    Returns rows per CSV, for the task log. Raises ContractError on the first problem."""
    check_match_json(read(parsed_key(match_id, "match.json")).decode(), match_id)
    return {name: check_csv(name, read(parsed_key(match_id, name)).decode(), match_id)
            for name in REQUIRED_COLUMNS}
