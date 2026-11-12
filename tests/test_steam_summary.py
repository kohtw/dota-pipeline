import gzip
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dota import steam_summary

PAGE = json.loads((Path(__file__).parent / "fixtures" / "steam_page.json").read_text())
DDL_FILE = Path(__file__).parents[1] / "warehouse" / "init" / "04_steam_tables.sql"


def by_id(rows, mid):
    return [r for r in rows if r["match_id"] == mid]


def test_one_match_row_per_match_and_one_player_row_per_listed_player():
    matches, players = steam_summary.parse_page(PAGE)
    assert len(matches) == 5
    assert len(players) == 10 + 10 + 8 + 10 + 10


def test_match_rows_have_exactly_the_table_columns():
    matches, players = steam_summary.parse_page(PAGE)
    assert set(matches[0]) == {"match_id", "match_seq_num", "start_time", "duration", "game_mode",
                               "lobby_type", "radiant_win", "human_players", "radiant_score",
                               "dire_score", "first_blood_time", "pre_game_duration", "cluster",
                               "leagueid"}
    assert set(players[0]) == {"match_id", "player_slot", "hero_id", "kills", "deaths", "assists",
                               "leaver_status", "team_number", "last_hits", "denies", "gold_per_min",
                               "xp_per_min", "level", "net_worth", "hero_damage", "tower_damage",
                               "hero_healing", "gold", "gold_spent", "item_0", "item_1", "item_2",
                               "item_3", "item_4", "item_5", "backpack_0", "backpack_1", "backpack_2",
                               "item_neutral"}


def test_new_fields_are_read_from_the_page():
    matches, players = steam_summary.parse_page(PAGE)
    assert matches[0]["radiant_score"] == 30 and matches[0]["first_blood_time"] == 90
    assert players[0]["gold_per_min"] == 300 and players[0]["team_number"] == 0
    assert players[5]["team_number"] == 1 and players[0]["item_0"] == 63


def ddl_columns(table):
    """Column order of a table built by the DDL file: CREATE columns, then each ADD COLUMN."""
    sql = DDL_FILE.read_text()
    body = re.search(rf"CREATE TABLE IF NOT EXISTS stg\.{table} \((.*?)\n\);", sql, re.S).group(1)
    cols = [line.split()[0] for line in body.strip().splitlines()
            if not line.strip().startswith("PRIMARY KEY")]
    return cols + re.findall(rf"ALTER TABLE stg\.{table} ADD COLUMN IF NOT EXISTS (\w+) ", sql)


def insert_columns(sql):
    return re.match(r"INSERT INTO stg\.\w+ \((.*)\) VALUES %s$", sql).group(1).split(", ")


@pytest.mark.parametrize("table, sql", [("steam_matches", "MATCH_SQL"), ("steam_players", "PLAYER_SQL"),
                                        ("steam_picks_bans", "PICK_BAN_SQL")])
def test_insert_columns_are_every_ddl_column_in_ddl_order(table, sql):
    assert insert_columns(getattr(steam_summary, sql)) == ddl_columns(table)


def test_ddl_adds_the_new_columns_after_create_so_old_tables_upgrade():
    assert ddl_columns("steam_matches")[:8] == list(steam_summary.MATCH_COLUMNS[:8])
    assert ddl_columns("steam_matches")[-2:] == ["source_page", "loaded_at"]
    assert "gold_per_min" in ddl_columns("steam_players")


def test_no_account_id_anywhere():
    matches, players = steam_summary.parse_page(PAGE)
    picks_bans = steam_summary.parse_picks_bans(PAGE)
    assert all("account_id" not in r for r in matches + players + picks_bans)
    for sql in (steam_summary.MATCH_SQL, steam_summary.PLAYER_SQL, steam_summary.PICK_BAN_SQL):
        assert "account_id" not in sql


def test_types_are_int_and_bool():
    matches, players = steam_summary.parse_page(PAGE)
    m = matches[0]
    assert m["match_id"] == 9000000001 and m["radiant_win"] is True
    assert all(type(m[k]) is int for k in m if k != "radiant_win")
    assert all(type(v) is int for v in players[0].values())
    assert matches[1]["radiant_win"] is False


def test_short_match_keeps_its_listed_players_only():
    matches, players = steam_summary.parse_page(PAGE)
    (m,) = by_id(matches, 9000000003)
    assert m["human_players"] == 8
    assert len(by_id(players, 9000000003)) == 8


def test_zero_duration_is_kept_not_filtered():
    matches, _ = steam_summary.parse_page(PAGE)
    assert by_id(matches, 9000000004)[0]["duration"] == 0


def test_missing_radiant_win_becomes_none():
    matches, _ = steam_summary.parse_page(PAGE)
    assert by_id(matches, 9000000005)[0]["radiant_win"] is None


def test_missing_player_field_becomes_none():
    page = {"matches": [{"match_id": 1, "players": [{"player_slot": 0}]}]}
    matches, players = steam_summary.parse_page(page)
    assert matches[0]["duration"] is None
    assert matches[0]["radiant_score"] is None and matches[0]["leagueid"] is None
    assert players == [{"match_id": 1, **{c: None for c in steam_summary.PLAYER_COLUMNS}, "player_slot": 0}]


def test_page_without_matches_is_empty():
    assert steam_summary.parse_page({"status": 1}) == ([], [])
    assert steam_summary.parse_picks_bans({"status": 1}) == []


def test_picks_bans_one_row_per_entry_with_order_as_ord():
    rows = steam_summary.parse_picks_bans(PAGE)
    assert len(rows) == 14 + 14 + 12 + 14
    assert rows[0] == {"match_id": 9000000001, "ord": 0, "is_pick": True, "hero_id": 1, "team": 0}
    assert {"match_id": 9000000001, "ord": 10, "is_pick": False, "hero_id": 60, "team": 0} in rows


def test_match_without_picks_bans_gives_no_pick_ban_rows():
    assert by_id(steam_summary.parse_picks_bans(PAGE), 9000000005) == []
    assert steam_summary.parse_picks_bans({"matches": [{"match_id": 1, "picks_bans": None}]}) == []


def test_missing_pick_ban_field_becomes_none():
    rows = steam_summary.parse_picks_bans({"matches": [{"match_id": 1, "picks_bans": [{"order": 3}]}]})
    assert rows == [{"match_id": 1, "ord": 3, "is_pick": None, "hero_id": None, "team": None}]


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=None):
        self.log.append(("execute", sql, params))

    def close(self):
        self.log.append(("close",))


class FakeConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.log.append(("commit",))

    def rollback(self):
        self.log.append(("rollback",))


DDL = "CREATE SCHEMA IF NOT EXISTS stg; -- fake ddl"


def run_load(matches, players, conn=None, **kw):
    conn = conn or FakeConn()
    inserts = []
    n = steam_summary.load_steam(matches, players, conn, ddl=DDL, **kw,
                                 execute_values=lambda cur, sql, rows: (
                                     inserts.append((sql, rows)), conn.log.append(("insert", sql))))
    return n, conn, inserts


def steam_rows():
    return steam_summary.parse_page(PAGE)


def test_ddl_runs_before_any_delete():
    matches, players = steam_rows()
    _, conn, _ = run_load(matches, players)
    stmts = [e for e in conn.log if e[0] in ("execute", "insert")]
    assert stmts[0] == ("execute", DDL, None)
    assert all("DELETE" in e[1] for e in stmts[1:3])


def test_delete_then_insert_in_one_commit_and_returns_match_count():
    matches, players = steam_rows()
    n, conn, inserts = run_load(matches, players, picks_bans=steam_summary.parse_picks_bans(PAGE))
    assert n == 5
    kinds = [e[0] if e[0] != "execute" else e[1].split()[0] for e in conn.log]
    assert kinds == ["CREATE", "DELETE", "DELETE", "DELETE", "insert", "insert", "insert", "commit", "close"]
    deletes = [e for e in conn.log if e[0] == "execute" and e[1].startswith("DELETE")]
    assert {d[1] for d in deletes} == {"DELETE FROM stg.steam_picks_bans WHERE match_id = ANY(%s)",
                                       "DELETE FROM stg.steam_players WHERE match_id = ANY(%s)",
                                       "DELETE FROM stg.steam_matches WHERE match_id = ANY(%s)"}
    assert all(sorted(d[2][0]) == sorted(m["match_id"] for m in matches) for d in deletes)
    assert len(inserts[0][1]) == 5 and len(inserts[1][1]) == 48 and len(inserts[2][1]) == 54
    assert inserts[0][0].startswith("INSERT INTO stg.steam_matches (match_id, match_seq_num")
    assert inserts[2][0].startswith("INSERT INTO stg.steam_picks_bans (match_id, ord")


def test_no_picks_bans_inserts_none_but_still_clears_old_ones():
    matches, players = steam_rows()
    _, conn, inserts = run_load(matches, players)
    assert len(inserts) == 2
    assert any(e[0] == "execute" and e[1].startswith("DELETE FROM stg.steam_picks_bans") for e in conn.log)


def test_duplicate_pick_ban_in_a_batch_inserts_once_with_later_values():
    matches, players = steam_rows()
    picks_bans = steam_summary.parse_picks_bans(PAGE)
    later = {**picks_bans[0], "hero_id": 99}
    _, _, inserts = run_load(matches, players, picks_bans=picks_bans + [later])
    rows = inserts[2][1]
    assert len(rows) == 54
    (row,) = [r for r in rows if r[:2] == (picks_bans[0]["match_id"], picks_bans[0]["ord"])]
    assert row[3] == 99


def test_source_page_and_loaded_at_reach_every_match_row():
    matches, players = steam_rows()
    before = datetime.now(timezone.utc).replace(tzinfo=None)
    _, _, inserts = run_load(matches, players, source_page="steam_raw/seq_1.json.gz")
    rows = inserts[0][1]
    assert {r[-2] for r in rows} == {"steam_raw/seq_1.json.gz"}
    (loaded_at,) = {r[-1] for r in rows}
    assert loaded_at.tzinfo is None  # naive UTC for a TIMESTAMP column
    assert before <= loaded_at <= before + timedelta(seconds=5)


def test_source_page_defaults_to_none():
    matches, players = steam_rows()
    _, _, inserts = run_load(matches, players)
    assert {r[-2] for r in inserts[0][1]} == {None}


def test_duplicate_match_in_a_batch_inserts_once_with_later_values():
    matches, players = steam_rows()
    later = {**matches[0], "duration": 1234}
    n, _, inserts = run_load(matches + [later], players)
    assert n == 5
    rows = inserts[0][1]
    assert len(rows) == 5
    (row,) = [r for r in rows if r[0] == matches[0]["match_id"]]
    assert 1234 in row


def test_duplicate_player_in_a_batch_inserts_once_with_later_values():
    matches, players = steam_rows()
    later = {**players[0], "kills": 99}
    _, _, inserts = run_load(matches, players + [later])
    rows = inserts[1][1]
    assert len(rows) == 48
    (row,) = [r for r in rows if r[:2] == (players[0]["match_id"], players[0]["player_slot"])]
    assert 99 in row


def test_second_call_with_same_batch_issues_same_statements():
    matches, players = steam_rows()
    _, first, _ = run_load(matches, players)
    _, second, _ = run_load(matches, players)
    assert first.log == second.log


def test_failure_rolls_back_and_raises():
    matches, players = steam_rows()
    conn = FakeConn()

    def boom(cur, sql, rows):
        raise RuntimeError("db down")

    with pytest.raises(RuntimeError):
        steam_summary.load_steam(matches, players, conn, ddl=DDL, execute_values=boom)
    assert ("rollback",) in conn.log and ("commit",) not in conn.log


def test_empty_batch_loads_nothing_and_inserts_nothing():
    n, conn, inserts = run_load([], [])
    assert n == 0 and inserts == []


def test_unloaded_pages_skips_loaded_and_sorts() -> None:
    page_keys = ["steam_raw/seq_30.json.gz", "steam_raw/seq_10.json.gz", "steam_raw/seq_20.json.gz",
                 "steam_raw/_loaded/seq_10.json.gz"]
    loaded = ["steam_raw/_loaded/seq_10.json.gz"]
    assert steam_summary.unloaded_pages(page_keys, loaded) == ["steam_raw/seq_20.json.gz", "steam_raw/seq_30.json.gz"]


def test_unloaded_pages_orders_by_sequence_number_not_text() -> None:
    keys = ["steam_raw/seq_9.json.gz", "steam_raw/seq_100.json.gz"]
    assert steam_summary.unloaded_pages(keys, []) == keys


def test_loaded_marker_key_uses_page_basename() -> None:
    assert steam_summary.loaded_marker_key("steam_raw/seq_10.json.gz") == "steam_raw/_loaded/seq_10.json.gz"


def _gz(page: dict) -> bytes:
    import gzip

    return gzip.compress(json.dumps(page).encode())


def _run(pages: dict[str, bytes], fail_load_for: set[str] | None = None):
    events: list[tuple[str, str]] = []
    calls: list[tuple[str, str, int]] = []
    keys = list(pages)
    current = {}

    def read(key: str) -> bytes:
        current["key"] = key
        return pages[key]

    def load(matches: list[dict], players: list[dict], *, picks_bans: list[dict], source_page: str) -> None:
        if current["key"] in (fail_load_for or set()):
            raise RuntimeError("db down")
        events.append(("load", current["key"]))
        calls.append((current["key"], source_page, len(picks_bans)))

    def mark(key: str) -> None:
        events.append(("mark", key))

    return steam_summary.load_pages_isolated(keys, read, load, mark), events, calls


def test_load_pages_isolated_loads_and_marks_all() -> None:
    pages = {f"steam_raw/seq_{i}.json.gz": _gz(PAGE) for i in (1, 2, 3)}
    (loaded, failed), events, _ = _run(pages)
    assert loaded == list(pages) and failed == {}
    assert [e for e in events if e[0] == "mark"] == [("mark", k) for k in pages]


def test_load_gets_the_page_key_as_source_page_and_its_picks_bans() -> None:
    pages = {f"steam_raw/seq_{i}.json.gz": _gz(PAGE) for i in (1, 2)}
    _, _, calls = _run(pages)
    assert calls == [("steam_raw/seq_1.json.gz", "steam_raw/seq_1.json.gz", 54),
                     ("steam_raw/seq_2.json.gz", "steam_raw/seq_2.json.gz", 54)]


def test_bad_middle_page_does_not_stop_later_pages() -> None:
    pages = {"steam_raw/seq_1.json.gz": _gz(PAGE), "steam_raw/seq_2.json.gz": b"not gzip",
             "steam_raw/seq_3.json.gz": _gz(PAGE)}
    (loaded, failed), events, _ = _run(pages)
    assert loaded == ["steam_raw/seq_1.json.gz", "steam_raw/seq_3.json.gz"]
    assert list(failed) == ["steam_raw/seq_2.json.gz"] and failed["steam_raw/seq_2.json.gz"]
    assert ("mark", "steam_raw/seq_3.json.gz") in events


def test_failed_load_is_not_marked() -> None:
    pages = {"steam_raw/seq_1.json.gz": _gz(PAGE), "steam_raw/seq_2.json.gz": _gz(PAGE)}
    (loaded, failed), events, _ = _run(pages, fail_load_for={"steam_raw/seq_1.json.gz"})
    assert "RuntimeError" in failed["steam_raw/seq_1.json.gz"]
    assert ("mark", "steam_raw/seq_1.json.gz") not in events
    assert loaded == ["steam_raw/seq_2.json.gz"]


def test_marker_written_only_after_load_returns() -> None:
    pages = {"steam_raw/seq_1.json.gz": _gz(PAGE)}
    _, events, _ = _run(pages)
    assert events == [("load", "steam_raw/seq_1.json.gz"), ("mark", "steam_raw/seq_1.json.gz")]


def test_failure_message_is_none_when_load_pages_gave_no_result() -> None:
    assert steam_summary.failure_message(None) is None


def test_failure_message_is_none_when_no_page_failed() -> None:
    assert steam_summary.failure_message({"loaded": ["a"], "failed": {}}) is None


def test_failure_message_names_the_failed_pages() -> None:
    msg = steam_summary.failure_message({"loaded": [], "failed": {"steam_raw/seq_2.json.gz": "boom"}})
    assert msg is not None and msg.startswith("1 Steam pages failed") and "seq_2" in msg


def test_save_page_writes_gzipped_json_under_the_key_the_loader_reads():
    written = {}
    key = steam_summary.save_page(123, PAGE, lambda k, body: written.update({k: body}))
    assert key == "steam_raw/seq_123.json.gz"
    assert json.loads(gzip.decompress(written[key])) == PAGE
    assert steam_summary.unloaded_pages([key], []) == [key]
