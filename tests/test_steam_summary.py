import json
from pathlib import Path

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
