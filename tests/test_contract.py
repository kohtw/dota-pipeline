import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dota import contract

MATCH = 9079737909
ONE_ROW = {"tick": [30], "handle": [1], "class": ["C"], "event": ["created"], "index": [0], "idx": [0],
           "name": ["x"], "type": ["DOTA_COMBATLOG_DAMAGE"], "timestamp": [3.3]}


def table(name: str, **overrides) -> pa.Table:
    """A valid one-partition table for `name` (all required columns), with column overrides."""
    if name == "match":
        cols = {"game_mode": [22], "game_winner": [2], "end_time": [1]}
    elif name == "players":
        cols = {"slot": list(range(10)), "hero_name": [f"npc_dota_hero_{i}" for i in range(10)],
                "game_team": [2] * 5 + [3] * 5}
    else:
        cols = {c: ONE_ROW.get(c, [1]) for c in contract.REQUIRED_COLUMNS[name]}
    return pa.table({**cols, **overrides})


def parquet(t: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf)
    return buf.getvalue()


def store(**tables: pa.Table | None) -> dict[str, bytes]:
    """Object store for one match: every table + the _complete marker. None = table not written."""
    all_tables = {name: table(name) for name in contract.REQUIRED_COLUMNS}
    all_tables[contract.COMPLETE] = pa.table({"parsed_at": [1]})
    all_tables.update(tables)
    return {contract.partition_prefix(n, MATCH) + "part-00000.snappy.parquet": parquet(t)
            for n, t in all_tables.items() if t is not None}


def check(objects: dict[str, bytes]) -> dict[str, int]:
    return contract.check_parsed(MATCH, objects.__getitem__,
                                 lambda prefix: [k for k in objects if k.startswith(prefix)])


def test_valid_output_passes_and_counts_rows():
    counts = check(store())
    assert counts["players"] == 10 and counts["match"] == 1 and counts["heroes"] == 1


def test_extra_columns_are_allowed():
    assert check(store(heroes=table("heroes", m_iHealth=[100])))["heroes"] == 1


def test_per_tick_counters_are_required_for_newly_complete_parses():
    with pytest.raises(contract.ContractError, match="kda_changes: no data"):
        check(store(kda_changes=None))
    t = table("kda_changes").drop_columns(["m_vecPlayerTeamData.m_iAssists"])
    with pytest.raises(contract.ContractError, match="kda_changes: missing columns"):
        check(store(kda_changes=t))


def test_missing_column_fails():
    t = table("heroes").drop_columns(["CBodyComponent.m_vecX"])
    with pytest.raises(contract.ContractError, match=r"heroes: missing columns \['CBodyComponent.m_vecX'\]"):
        check(store(heroes=t))


def test_missing_marker_fails():
    with pytest.raises(contract.ContractError, match="_complete marker"):
        check(store(**{contract.COMPLETE: None}))


def test_missing_table_fails_but_wards_may_be_empty():
    with pytest.raises(contract.ContractError, match="combatlog: no data"):
        check(store(combatlog=None))
    assert check(store(wards=None))["wards"] == 0


@pytest.mark.parametrize("name, overrides, message", [
    ("match", {"game_winner": [0]}, "game_winner is 0"),
    ("players", {"slot": [0] * 9, "hero_name": ["h"] * 9, "game_team": [2] * 9}, "9 players"),
    ("players", {"game_team": [2] * 9 + [4]}, r"game_team values \[4\]"),
])
def test_bad_match_or_players_fails(name: str, overrides: dict, message: str):
    with pytest.raises(contract.ContractError, match=message):
        check(store(**{name: table(name, **overrides)}))


def test_unparsed_finds_replays_without_complete_marker_newest_first():
    replays = ["replays/1.dem", "replays/2.dem", "replays/3.dem", "replays/notes.txt"]
    parsed = ["parsed/_complete/match_id=1/part-0.parquet", "parsed/heroes/match_id=2/part-0.parquet"]
    assert contract.unparsed(replays, parsed) == [3, 2]


def test_unparsed_skips_exhausted_replays_but_keeps_two_failures():
    from dota import attempts

    replays = ["replays/1.dem", "replays/2.dem", "replays/3.dem"]
    runs = [f"scheduled__2026-11-12T0{h}:00:00+00:00" for h in range(3)]
    failed = [attempts.failure_key("parsed", 1, r) for r in runs]
    failed += [attempts.failure_key("parsed", 2, r) for r in runs[:2]]
    failed += [attempts.failure_key("clean", 3, r) for r in runs]
    assert contract.unparsed(replays, [], failed) == [3, 2]
