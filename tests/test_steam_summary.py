import json
from pathlib import Path

import pytest

from dota import steam_summary

PAGE = json.loads((Path(__file__).parent / "fixtures" / "steam_page.json").read_text())


def by_id(rows, mid):
    return [r for r in rows if r["match_id"] == mid]


def test_one_match_row_per_match_and_one_player_row_per_listed_player():
    matches, players = steam_summary.parse_page(PAGE)
    assert len(matches) == 5
    assert len(players) == 10 + 10 + 8 + 10 + 10


def test_match_rows_have_exactly_the_table_columns():
    matches, players = steam_summary.parse_page(PAGE)
    assert set(matches[0]) == {"match_id", "match_seq_num", "start_time", "duration", "game_mode",
                               "lobby_type", "radiant_win", "human_players"}
    assert set(players[0]) == {"match_id", "player_slot", "hero_id", "kills", "deaths", "assists",
                               "leaver_status"}


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
    assert players == [{"match_id": 1, "player_slot": 0, "hero_id": None, "kills": None,
                        "deaths": None, "assists": None, "leaver_status": None}]


def test_page_without_matches_is_empty():
    assert steam_summary.parse_page({"status": 1}) == ([], [])


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


def run_load(matches, players, conn=None):
    conn = conn or FakeConn()
    inserts = []
    n = steam_summary.load_steam(matches, players, conn, ddl=DDL,
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
    n, conn, inserts = run_load(matches, players)
    assert n == 5
    kinds = [e[0] if e[0] != "execute" else e[1].split()[0] for e in conn.log]
    assert kinds == ["CREATE", "DELETE", "DELETE", "insert", "insert", "commit", "close"]
    deletes = [e for e in conn.log if e[0] == "execute" and e[1].startswith("DELETE")]
    assert {d[1] for d in deletes} == {"DELETE FROM stg.steam_players WHERE match_id = ANY(%s)",
                                       "DELETE FROM stg.steam_matches WHERE match_id = ANY(%s)"}
    assert all(sorted(d[2][0]) == sorted(m["match_id"] for m in matches) for d in deletes)
    assert len(inserts[0][1]) == 5 and len(inserts[1][1]) == 48
    assert inserts[0][0].startswith("INSERT INTO stg.steam_matches (match_id, match_seq_num")


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
