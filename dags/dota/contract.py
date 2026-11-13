"""Checks a match's parsed output against docs/data-contract.md before anything downstream
reads it, so a parser change that breaks the contract fails here, not in Spark or dbt.

Parsed layout: parsed/{table}/match_id={id}/part-*.parquet, plus parsed/_complete/match_id={id}/
which the parser writes last, so its presence means every table of that match was written.
pyarrow is imported inside functions: DAG files import this module at parse time."""

import io
import re
from typing import Callable

from dota import attempts

COMPLETE = "_complete"  # marker dataset, written after all tables

# Required columns only: the contract allows new columns, so extra ones are fine.
# match_id is the partition (in the path), not a column inside the files.
REQUIRED_COLUMNS = {
    "match": ("game_mode", "game_winner", "end_time"),
    "players": ("slot", "hero_name", "game_team"),
    "combatlog": ("tick", "type", "timestamp", "attacker_name_idx", "target_name_idx", "inflictor_name_idx",
                  "value"),
    "names": ("idx", "name"),
    "entity_names": ("idx", "name"),
    "gamerules": ("tick", "handle", "class", "m_pGameRules.m_nGameState", "m_pGameRules.m_flGameStartTime"),
    "heroes": ("tick", "handle", "class", "m_iPlayerID", "m_iTeamNum", "m_hReplicatingOtherHeroModel",
               "CBodyComponent.m_cellX", "CBodyComponent.m_cellY", "CBodyComponent.m_vecX",
               "CBodyComponent.m_vecY", "m_hItems.0000"),
    "player_resource": ("tick", "handle", "class", "index", "m_vecPlayerData.m_iPlayerTeam",
                        "m_vecPlayerTeamData.m_iTeamSlot", "m_vecPlayerTeamData.m_nSelectedHeroID"),
    "team_data": ("tick", "handle", "class", "index", "m_vecDataTeam.m_iNetWorth"),
    "wards": ("tick", "event", "handle", "class", "m_nPlayerOwnerID", "m_iTeamNum"),
    "item_entities": ("tick", "event", "handle", "class", "m_pEntity.m_nameStringTableIndex"),
}
# Spark writes no partition for a table with no rows; these can legitimately be empty
# (e.g. no one placed a ward). Every other table must have rows.
MAY_BE_EMPTY = ("wards",)
TEAMS = (2, 3)


class ContractError(Exception):
    pass


def partition_prefix(table: str, match_id: int) -> str:
    return f"parsed/{table}/match_id={match_id}/"


def read_parquet(keys: list[str], read: Callable[[str], bytes]):
    """All part files of one partition as one pyarrow Table."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    return pa.concat_tables([pq.read_table(io.BytesIO(read(k))) for k in keys])


def check_table(name: str, table, match_id: int) -> int:
    """Checks one table's columns (and values for match/players). Returns its row count."""
    missing = [c for c in REQUIRED_COLUMNS[name] if c not in table.column_names]
    if missing:
        raise ContractError(f"{name}: missing columns {missing}")
    rows = table.num_rows
    if rows == 0 and name not in MAY_BE_EMPTY:
        raise ContractError(f"{name}: no rows")
    if name == "match":
        if rows != 1:
            raise ContractError(f"match: {rows} rows, expected 1")
        winner = table.column("game_winner")[0].as_py()
        if winner not in TEAMS:
            raise ContractError(f"match: game_winner is {winner}, expected 2 or 3")
    if name == "players":
        if rows != 10:
            raise ContractError(f"players: {rows} players, expected 10")
        bad = [t for t in table.column("game_team").to_pylist() if t not in TEAMS]
        if bad:
            raise ContractError(f"players: game_team values {bad}, expected 2 or 3")
    return rows


def unparsed(replay_keys: list[str], parsed_keys: list[str],
             failed_keys: list[str] | tuple[str, ...] = ()) -> list[int]:
    """Match ids that have a replay but no _complete marker, newest first. A parse that died
    halfway wrote no marker, so it counts as unparsed and runs again, until it has failed in
    attempts.MAX_ATTEMPTS runs."""
    replays = {int(k.removeprefix("replays/").removesuffix(".dem"))
               for k in replay_keys if k.startswith("replays/") and k.endswith(".dem")}
    complete = set()
    for key in parsed_keys:
        m = re.match(rf"parsed/{COMPLETE}/match_id=(\d+)/", key)
        if m:
            complete.add(int(m.group(1)))
    return sorted(replays - complete - attempts.exhausted(list(failed_keys), "parsed"), reverse=True)


def check_parsed(match_id: int, read: Callable[[str], bytes],
                 list_keys: Callable[[str], list[str]]) -> dict[str, int]:
    """Checks every table of one match. `read(key)` returns an object's bytes, `list_keys(prefix)`
    the keys under a prefix. Returns rows per table, for the task log. Raises ContractError on
    the first problem."""
    if not list_keys(partition_prefix(COMPLETE, match_id)):
        raise ContractError(f"no {COMPLETE} marker: parse did not finish")
    counts = {}
    for name in REQUIRED_COLUMNS:
        parts = [k for k in list_keys(partition_prefix(name, match_id)) if k.endswith(".parquet")]
        if not parts:
            if name in MAY_BE_EMPTY:
                counts[name] = 0
                continue
            raise ContractError(f"{name}: no data for match {match_id}")
        counts[name] = check_table(name, read_parquet(parts, read), match_id)
    return counts
