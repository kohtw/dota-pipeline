"""Checks a match's clean output against docs/clean-contract.md before it is loaded into
Postgres, so a Spark clean-job change that breaks the contract fails here, not in the load or dbt.

Clean layout: clean/{table}/match_id={id}/part-*.parquet, plus clean/_complete/match_id={id}/
which the clean job writes last, so its presence means every table of that match was written.
pyarrow is imported inside functions: DAG files import this module at parse time."""

import io
import re
from typing import Callable

from dota import attempts

COMPLETE = "_complete"  # marker dataset, written after all tables
REJECTED = "_rejected"  # quarantine marker dataset: clean/_rejected/match_id={id}/_REJECTED
PARSED_COMPLETE = "parsed/_complete"  # the raw-layer marker (see contract.py)

# Required columns only: the contract allows new columns, so extra ones are fine.
# match_id is the partition (in the path), not a column inside the files.
# Names come from warehouse/init/02_clean_tables.sql.
REQUIRED_COLUMNS = {
    "matches": ("game_mode", "winner_team", "end_time", "game_start_tick", "duration_s",
                "assist_attribution_complete", "assist_attribution_assumed"),
    "players": ("player_slot", "team", "team_slot", "hero_name", "hero_id"),
    "player_stats": ("player_slot", "tick", "game_time_s", "level", "kills", "deaths", "assists", "net_worth",
                     "unspent_gold", "total_earned_gold", "total_earned_xp", "last_hits", "denies",
                     "hero_damage", "tower_damage", "healing"),
    "positions": ("player_slot", "tick", "game_time_s", "x", "y"),
    "damage_events": ("tick", "game_time_s", "attacker_name", "target_name", "inflictor_name",
                      "attacker_slot", "target_slot", "damage", "hits"),
    "deaths": ("tick", "game_time_s", "victim_slot", "killer_slot", "killer_name",
               "credited_killer_slot", "link_status"),
    "death_assists": ("tick", "victim_slot", "assist_slot", "same_side"),
    "purchases": ("tick", "game_time_s", "player_slot", "item_name", "quantity"),
    "final_inventory": ("player_slot", "inventory_slot", "item_name"),
}
# Spark writes no partition for a table with no rows. A real match always has stats, positions,
# damage, deaths and a final inventory; but a match can end with no one assisting a kill
# (death_assists) or with no purchase logged (purchases), so only those two may be empty.
MAY_BE_EMPTY = ("death_assists", "purchases")
TEAMS = (2, 3)  # 2 Radiant, 3 Dire
LINK_STATUSES = ("exact", "participant_assumed", "same_side_possible")


class CleanContractError(Exception):
    pass


def clean_prefix(table: str, match_id: int) -> str:
    return f"clean/{table}/match_id={match_id}/"


def complete_prefix(match_id: int) -> str:
    return clean_prefix(COMPLETE, match_id)


def report_key(match_id: int) -> str:
    """The clean job's per-match report (written by every run that reaches write_result)."""
    return f"clean/_review/match_id={match_id}/report.json"


def rejected_key(match_id: int) -> str:
    return f"clean/{REJECTED}/match_id={match_id}/_REJECTED"


def read_parquet(keys: list[str], read: Callable[[str], bytes]):
    """All part files of one partition as one pyarrow Table."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    return pa.concat_tables([pq.read_table(io.BytesIO(read(k))) for k in keys])


def check_table(name: str, table, match_id: int) -> int:
    """Checks one table's columns (and values for matches/players). Returns its row count."""
    missing = [c for c in REQUIRED_COLUMNS[name] if c not in table.column_names]
    if missing:
        raise CleanContractError(f"{name}: missing columns {missing}")
    rows = table.num_rows
    if rows == 0 and name not in MAY_BE_EMPTY:
        raise CleanContractError(f"{name}: no rows")
    if name == "deaths":
        import pyarrow as pa

        credit = table.column("credited_killer_slot")
        labels = table.column("link_status")
        if not pa.types.is_integer(credit.type):
            raise CleanContractError("deaths: credited_killer_slot must be integer")
        if any(value is not None and value not in range(10) for value in credit.to_pylist()):
            raise CleanContractError("deaths: credited_killer_slot must be NULL or 0-9")
        if not pa.types.is_string(labels.type) or any(value not in LINK_STATUSES for value in labels.to_pylist()):
            raise CleanContractError("deaths: link_status must be a non-null supported credit label")
    if name == "death_assists":
        import pyarrow as pa

        sides = table.column("same_side")
        if not pa.types.is_boolean(sides.type) or sides.null_count:
            raise CleanContractError("death_assists: same_side must be a non-null boolean")
    if name == "matches":
        import pyarrow as pa

        if rows != 1:
            raise CleanContractError(f"matches: {rows} rows, expected 1")
        for field in ("assist_attribution_complete", "assist_attribution_assumed"):
            flag = table.column(field)
            if not pa.types.is_boolean(flag.type) or flag.null_count:
                raise CleanContractError(f"matches: {field} must be a non-null boolean")
        winner = table.column("winner_team")[0].as_py()
        if winner not in TEAMS:
            raise CleanContractError(f"matches: winner_team is {winner}, expected 2 or 3")
    if name == "players":
        if rows != 10:
            raise CleanContractError(f"players: {rows} players, expected 10")
        bad = [t for t in table.column("team").to_pylist() if t not in TEAMS]
        if bad:
            raise CleanContractError(f"players: team values {bad}, expected 2 or 3")
    return rows


def _complete_ids(keys: list[str], prefix: str) -> set[int]:
    ids = set()
    for key in keys:
        m = re.match(rf"{re.escape(prefix)}/match_id=(\d+)/", key)
        if m:
            ids.add(int(m.group(1)))
    return ids


def uncleaned(parsed_complete_keys: list[str], clean_complete_keys: list[str],
              rejected_keys: list[str] | tuple[str, ...] = (),
              failed_keys: list[str] | tuple[str, ...] = ()) -> list[int]:
    """Match ids that finished parsing but have no clean _complete marker, newest first. Left out:
    matches with a _rejected marker, and matches whose clean crashed in attempts.MAX_ATTEMPTS runs.
    Deleting the marker or the failure keys makes a match eligible again."""
    return sorted(_complete_ids(parsed_complete_keys, PARSED_COMPLETE)
                  - _complete_ids(clean_complete_keys, f"clean/{COMPLETE}")
                  - _complete_ids(list(rejected_keys), f"clean/{REJECTED}")
                  - attempts.exhausted(list(failed_keys), "clean"), reverse=True)


def is_rejected(report: dict) -> bool:
    """True when the clean job's report.json shows a blocking rejection: not complete, not a
    preview run, and at least one error."""
    return not report.get("complete") and not report.get("preview") and bool(report.get("errors"))


def check_parsed(match_id: int, read: Callable[[str], bytes],
                 list_keys: Callable[[str], list[str]]) -> dict[str, int]:
    """Checks every clean table of one match. `read(key)` returns an object's bytes,
    `list_keys(prefix)` the keys under a prefix. Returns rows per table, for the task log.
    Raises CleanContractError on the first problem."""
    if not list_keys(complete_prefix(match_id)):
        raise CleanContractError(f"no {COMPLETE} marker: clean job did not finish")
    counts = {}
    for name in REQUIRED_COLUMNS:
        parts = [k for k in list_keys(clean_prefix(name, match_id)) if k.endswith(".parquet")]
        if not parts:
            if name in MAY_BE_EMPTY:
                counts[name] = 0
                continue
            raise CleanContractError(f"{name}: no data for match {match_id}")
        counts[name] = check_table(name, read_parquet(parts, read), match_id)
    return counts
