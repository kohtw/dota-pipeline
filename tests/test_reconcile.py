import reconcile_steam_replay as rec


def replay(slot: int, hero: int = 1, k: int = 0, d: int = 0, a: int = 0, match_id: int = 1) -> dict:
    return {"match_id": match_id, "player_slot": slot, "hero_id": hero, "kills": k, "deaths": d, "assists": a}


def steam(slot: int, hero: int = 1, k: int = 0, d: int = 0, a: int = 0, match_id: int = 1) -> dict:
    return {"match_id": match_id, "player_slot": slot, "hero_id": hero, "kills": k, "deaths": d, "assists": a}


def test_steam_slot_maps_to_clean_slot():
    assert [rec.clean_slot(s) for s in (0, 4, 128, 130, 132)] == [0, 4, 5, 7, 9]


def resource(tick: int, team: int, team_slot: int, hero: int, k: int) -> dict:
    p = "m_vecPlayerTeamData."
    return {"tick": tick, "m_vecPlayerData.m_iPlayerTeam": team, f"{p}m_iTeamSlot": team_slot,
            f"{p}m_nSelectedHeroID": hero, f"{p}m_iKills": k, f"{p}m_iDeaths": 0, f"{p}m_iAssists": 0}


def test_final_scoreboard_takes_the_last_snapshot_and_skips_non_players():
    rows = [resource(60, 3, 2, 14, 5), resource(30, 3, 2, 14, 4), resource(60, 1, 0, 99, 0),
            resource(30, 2, 0, 1, 1)]
    assert rec.final_scoreboard(7, rows) == [
        {"match_id": 7, "player_slot": 0, "hero_id": 1, "kills": 1, "deaths": 0, "assists": 0},
        {"match_id": 7, "player_slot": 7, "hero_id": 14, "kills": 5, "deaths": 0, "assists": 0}]


def test_compare_pairs_dire_steam_slot_130_with_replay_slot_7():
    (row,) = rec.compare([replay(7, hero=14, k=3, d=2, a=9)], [steam(130, hero=14, k=3, d=2, a=9)])
    assert (row["match_id"], row["player_slot"], row["steam_player_slot"]) == (1, 7, 130)
    assert row["in_both"] and row["all_equal"]
    assert (row["values_compared"], row["values_equal"]) == (4, 4)
    assert row["mismatched"] == ""


def test_compare_flags_a_hero_mismatch_and_counts_the_other_values_equal():
    (row,) = rec.compare([replay(0, hero=5, k=1, d=1, a=1)], [steam(0, hero=6, k=1, d=1, a=1)])
    assert (row["hero_id_replay"], row["hero_id_steam"]) == (5, 6)
    assert not row["all_equal"]
    assert (row["values_compared"], row["values_equal"]) == (4, 3)
    assert row["mismatched"] == "hero_id"


def test_compare_counts_each_differing_stat():
    (row,) = rec.compare([replay(2, k=4, d=0, a=7)], [steam(2, k=5, d=0, a=6)])
    assert (row["values_compared"], row["values_equal"]) == (4, 2)
    assert row["mismatched"] == "kills,assists"


def test_compare_keeps_players_seen_by_one_source_without_comparing_them():
    rows = rec.compare([replay(0), replay(1, match_id=2)], [steam(0), steam(129, match_id=3)])
    keyed = {(r["match_id"], r["player_slot"]): r for r in rows}
    assert sorted(keyed) == [(1, 0), (2, 1), (3, 6)]
    assert keyed[(1, 0)]["in_both"]
    assert not keyed[(2, 1)]["in_both"] and keyed[(2, 1)]["hero_id_steam"] is None
    assert not keyed[(3, 6)]["in_both"] and keyed[(3, 6)]["kills_replay"] is None
    assert all(keyed[k]["values_compared"] == 0 for k in [(2, 1), (3, 6)])


def test_compare_counts_a_missing_value_as_compared_but_not_equal():
    (row,) = rec.compare([replay(3, hero=None)], [steam(3, hero=None)])
    assert (row["values_compared"], row["values_equal"]) == (4, 3)
    assert row["mismatched"] == "hero_id"


def test_summarize_counts_only_players_in_both_sources():
    rows = rec.compare(
        [replay(0), replay(5, hero=2), replay(0, match_id=2), replay(1, match_id=9)],
        [steam(0), steam(128, hero=3), steam(0, match_id=2)])
    assert rec.summarize(rows) == {"matches": 2, "players": 3, "values_compared": 12, "values_equal": 11,
                                   "players_all_equal": 2}
