import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dota import clean_contract as cc

MATCH = 9079737909


def table(name: str, **overrides) -> pa.Table:
    """A valid table for `name` (all required columns, no match_id), with column overrides."""
    if name == "matches":
        cols = {c: [1] for c in cc.REQUIRED_COLUMNS[name]} | {"winner_team": [2]}
    elif name == "players":
        cols = {c: [1] * 10 for c in cc.REQUIRED_COLUMNS[name]} | {"team": [2] * 5 + [3] * 5}
    else:
        cols = {c: [1] for c in cc.REQUIRED_COLUMNS[name]}
    return pa.table({**cols, **overrides})


def parquet(t: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf)
    return buf.getvalue()


def store(**tables: pa.Table | None) -> dict[str, bytes]:
    """Object store for one match: every table + the _complete marker. None = table not written."""
    all_tables = {name: table(name) for name in cc.REQUIRED_COLUMNS}
    all_tables[cc.COMPLETE] = pa.table({"cleaned_at": [1]})
    all_tables.update(tables)
    return {cc.clean_prefix(n, MATCH) + "part-00000.snappy.parquet": parquet(t)
            for n, t in all_tables.items() if t is not None}


def check(objects: dict[str, bytes]) -> dict[str, int]:
    return cc.check_parsed(MATCH, objects.__getitem__,
                           lambda prefix: [k for k in objects if k.startswith(prefix)])


def test_tables_are_the_nine_clean_tables_without_match_id():
    assert set(cc.REQUIRED_COLUMNS) == {"matches", "players", "player_stats", "positions", "damage_events",
                                        "deaths", "death_assists", "purchases", "final_inventory"}
    assert all("match_id" not in cols for cols in cc.REQUIRED_COLUMNS.values())


def test_valid_output_passes_and_counts_rows():
    counts = check(store())
    assert counts["players"] == 10 and counts["matches"] == 1 and counts["deaths"] == 1


def test_extra_columns_are_allowed():
    assert check(store(positions=table("positions", z=[0.0])))["positions"] == 1


def test_missing_column_fails():
    t = table("positions").drop_columns(["game_time_s"])
    with pytest.raises(cc.CleanContractError, match=r"positions: missing columns \['game_time_s'\]"):
        check(store(positions=t))


def test_empty_table_fails_unless_allowed():
    with pytest.raises(cc.CleanContractError, match="deaths: no rows"):
        cc.check_table("deaths", table("deaths").slice(0, 0), MATCH)
    for name in cc.MAY_BE_EMPTY:
        assert cc.check_table(name, table(name).slice(0, 0), MATCH) == 0


def test_missing_partition_fails_unless_allowed():
    with pytest.raises(cc.CleanContractError, match="final_inventory: no data"):
        check(store(final_inventory=None))
    counts = check(store(death_assists=None, purchases=None))
    assert counts["death_assists"] == 0 and counts["purchases"] == 0


def test_matches_must_have_one_row_and_a_real_winner():
    with pytest.raises(cc.CleanContractError, match="matches: 2 rows, expected 1"):
        check(store(matches=table("matches", winner_team=[2, 3], **{c: [1, 1] for c in cc.REQUIRED_COLUMNS["matches"]
                                                                     if c != "winner_team"})))
    with pytest.raises(cc.CleanContractError, match="winner_team is 0, expected 2 or 3"):
        check(store(matches=table("matches", winner_team=[0])))


def test_players_must_be_ten_with_valid_teams():
    with pytest.raises(cc.CleanContractError, match="players: 9 players, expected 10"):
        cc.check_table("players", table("players").slice(0, 9), MATCH)
    with pytest.raises(cc.CleanContractError, match=r"team values \[1\], expected 2 or 3"):
        check(store(players=table("players", team=[2] * 5 + [3] * 4 + [1])))


def test_missing_complete_marker_fails():
    with pytest.raises(cc.CleanContractError, match="no _complete marker"):
        check(store(**{cc.COMPLETE: None}))


def test_prefixes():
    assert cc.clean_prefix("players", 5) == "clean/players/match_id=5/"
    assert cc.complete_prefix(5) == "clean/_complete/match_id=5/"


def test_uncleaned_is_parsed_minus_clean_newest_first():
    parsed = [f"parsed/_complete/match_id={i}/part-0.parquet" for i in (3, 9, 5, 7)] + \
             ["parsed/players/match_id=11/part-0.parquet"]
    clean = [f"clean/_complete/match_id={i}/part-0.parquet" for i in (5, 1)] + \
            ["clean/players/match_id=7/part-0.parquet"]
    assert cc.uncleaned(parsed, clean) == [9, 7, 3]


def test_uncleaned_omits_rejected_ids():
    parsed = [f"parsed/_complete/match_id={i}/part-0.parquet" for i in (3, 9, 5, 7)]
    rejected = ["clean/_rejected/match_id=9/_REJECTED", "clean/_rejected/match_id=3/_REJECTED"]
    assert cc.uncleaned(parsed, [], rejected) == [7, 5]


def test_rejected_marker_constant():
    assert cc.REJECTED == "_rejected"


@pytest.mark.parametrize(("report", "expected"), [
    ({"complete": True, "preview": False, "errors": []}, False),
    ({"complete": False, "preview": True, "errors": ["bad deaths"]}, False),
    ({"complete": False, "preview": False, "errors": ["bad deaths"]}, True),
    ({"complete": False, "preview": False, "errors": []}, False),
])
def test_is_rejected(report, expected):
    assert cc.is_rejected(report) is expected


def test_report_and_rejected_keys():
    assert cc.report_key(5) == "clean/_review/match_id=5/report.json"
    assert cc.rejected_key(5) == "clean/_rejected/match_id=5/_REJECTED"
    # the rejected marker must be one that uncleaned() recognises
    assert cc.uncleaned(["parsed/_complete/match_id=5/_SUCCESS"], [], [cc.rejected_key(5)]) == []
