"""Synthetic credit cases without starting Spark or using network services."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark_jobs"))
import clean_replays as kda


def run(events, changes, **kwargs):
    players = [{"player_slot": s, "team": 2 + s // 5} for s in range(10)]
    values = {s: dict(kills=0, deaths=0, assists=0) for s in range(10)}
    counters = [{"tick": 0, "player_slot": s, **v} for s, v in values.items()]
    for tick, slot, increments in changes:
        for stat, delta in increments.items():
            values[slot][stat] += delta
        counters.append({"tick": tick, "player_slot": slot, **values[slot]})
    final = [{"player_slot": s, **v} for s, v in values.items()]
    return kda.credit_kda(players, counters, events, final, **kwargs)


def death(tick=10, victim=5, killer=0, **fields):
    return dict(tick=tick, victim_slot=victim, killer_slot=killer,
                killer_name=None if killer is None else f"hero_{killer}",
                attacker_illusion=False, target_illusion=False, **fields)


def test_tower_credit_uses_counter_and_excludes_killer_from_assists():
    result = run([death(killer=None, assist_players=[0])],
                 [(12, 5, {"deaths": 1}), (12, 0, {"kills": 1})])
    assert result.deaths[0]["killer_slot"] is None
    assert result.deaths[0]["credited_killer_slot"] == 0
    assert result.death_assists == []


def test_illusion_attacker_is_kept_and_owner_credited():
    event = death(killer=None, source_owner_slot=0)
    event["attacker_illusion"] = True
    result = run([event], [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1})])
    assert len(result.deaths) == 1
    assert result.deaths[0]["credited_killer_slot"] == 0


def test_summon_owner_is_credited():
    result = run([death(killer=None, source_owner_slot=0)],
                 [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1})])
    assert result.deaths[0]["credited_killer_slot"] == 0


def test_illusion_victim_is_not_counted():
    event = death()
    event["target_illusion"] = True
    result = run([event], [])
    assert not result.deaths
    assert result.review[0]["reason"] == "illusion_victim"


def test_reincarnation_without_counter_rise_is_review_only():
    result = run([death(will_reincarnate=True)], [])
    assert not result.deaths
    assert result.review[0]["reason"] == "no_death_counter_rise"


def test_meepo_self_source_record_is_merged():
    result = run([death(killer=5, will_reincarnate=True), death(will_reincarnate=False)],
                 [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1})])
    assert len(result.deaths) == 1
    assert result.deaths[0]["killer_slot"] == 0


def test_assist_list_is_not_the_credit_ledger():
    result = run([death(assist_players=[2])],
                 [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1}),
                  (12, 1, {"assists": 1})])
    assert [r["assist_slot"] for r in result.death_assists] == [1]


def test_neutral_death_listed_participant_gets_no_assist_without_counter():
    result = run([death(killer=None, assist_players=[0])], [(14, 5, {"deaths": 1})])
    assert result.deaths[0]["credited_killer_slot"] is None
    assert not result.death_assists


def test_same_team_assist_is_retained_and_labelled():
    result = run([death()], [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1}),
                            (11, 6, {"assists": 1})])
    assert result.death_assists[0]["same_side"] is True


def test_game_recorded_victim_assist_is_retained_as_same_side_credit():
    # Real counter history in 9079738574: victim 0 gains a death AND an assist
    # at tick 24040 after the tick-24038 death. Do not erase game-recorded credit.
    result = run([death(victim=0, killer=6, assist_players=[6])],
                 [(12, 0, {"deaths": 1, "assists": 1}),
                  (12, 6, {"kills": 1}), (12, 5, {"assists": 1})])
    assert {(r["assist_slot"], r["same_side"]) for r in result.death_assists} == {
        (0, True), (5, False)}


def test_multiple_deaths_and_counter_delta_two():
    result = run([death(victim=5), death(victim=6)],
                 [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
                  (11, 0, {"kills": 2}), (11, 1, {"assists": 2})])
    assert len(result.deaths) == 2
    assert len(result.death_assists) == 2


def test_two_equally_valid_kill_assignments_block():
    with pytest.raises(kda.CreditError, match="Ambiguous"):
        run([death(victim=5, killer=None), death(victim=6, killer=None)],
            [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
             (11, 0, {"kills": 1}), (11, 1, {"kills": 1})])


def test_participant_dependent_links_are_labelled():
    result = run([death(victim=5, killer=None, assist_players=[0]),
                  death(victim=6, killer=None, assist_players=[1])],
                 [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
                  (11, 0, {"kills": 1}), (11, 1, {"kills": 1})])
    assert {r["link_status"] for r in result.deaths} == {"participant_assumed"}


def test_same_side_alternative_is_labelled():
    result = run([death(victim=5, killer=None), death(victim=0, killer=None)],
                 [(11, 5, {"deaths": 1}), (11, 0, {"deaths": 1}),
                  (11, 1, {"kills": 1}), (11, 6, {"kills": 1})])
    assert {r["link_status"] for r in result.deaths} == {"same_side_possible"}


@pytest.mark.parametrize("changes", [
    [(11, 5, {"deaths": 1})],
    [(11, 0, {"kills": 1})],
    [(11, 1, {"assists": 1})],
])
def test_orphan_counter_increase_blocks(changes):
    with pytest.raises(kda.CreditError, match="no matching death window"):
        run([], changes)


def test_counter_decrease_blocks():
    with pytest.raises(kda.CreditError, match="goes down"):
        run([], [(10, 0, {"kills": 1}), (20, 0, {"kills": -1})])


def test_conflicting_sources_block():
    with pytest.raises(kda.CreditError, match="Conflicting death sources"):
        run([death(), death(killer=1)], [(11, 5, {"deaths": 1})])


def test_outside_four_tick_window_blocks():
    with pytest.raises(kda.CreditError, match="no matching death window"):
        run([death()], [(15, 5, {"deaths": 1}), (15, 0, {"kills": 1})])


def test_search_limit_blocks():
    with pytest.raises(kda.CreditError, match="search limit"):
        run([death()], [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1})], max_search_states=1)


def test_initial_nonzero_counter_blocks():
    players = [{"player_slot": s, "team": 2 + s // 5} for s in range(10)]
    rows = [{"player_slot": s, "tick": 0, "kills": int(s == 0), "deaths": 0, "assists": 0}
            for s in range(10)]
    with pytest.raises(kda.CreditError, match="start at zero"):
        kda.credit_kda(players, rows, [], rows)


def test_independent_scoreboard_mismatch_blocks():
    players = [{"player_slot": s, "team": 2 + s // 5} for s in range(10)]
    rows = [{"player_slot": s, "tick": 0, "kills": 0, "deaths": 0, "assists": 0} for s in range(10)]
    final = [dict(r) for r in rows]
    final[0]["kills"] = 1
    with pytest.raises(kda.CreditError, match="scoreboard disagrees"):
        kda.credit_kda(players, rows, [], final)


def test_ambiguous_assist_to_death_links_block_even_when_totals_match():
    with pytest.raises(kda.CreditError, match="Ambiguous"):
        run([death(victim=5), death(victim=6)],
            [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
             (11, 0, {"kills": 2}), (11, 1, {"assists": 1})])


def test_assist_participants_resolve_ambiguity_and_label_the_component():
    result = run([death(victim=5, assist_players=[1]), death(victim=6)],
                 [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
                  (11, 0, {"kills": 2}), (11, 1, {"assists": 1})])
    assert [(r["victim_slot"], r["assist_slot"]) for r in result.death_assists] == [(5, 1)]
    assert {r["link_status"] for r in result.deaths} == {"participant_assumed"}


@pytest.mark.parametrize("participants", [[], [1]])
def test_unhelpful_assist_lists_preserve_original_ambiguity(participants):
    # Empty lists rule out every allocation; shared lists still allow both.
    with pytest.raises(kda.CreditError, match="Ambiguous credit assignment"):
        run([death(victim=5, assist_players=participants),
             death(victim=6, assist_players=participants)],
            [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
             (11, 0, {"kills": 2}), (11, 1, {"assists": 1})])


def test_assist_fallback_searches_beyond_first_two_answers():
    result = run([death(victim=5, assist_players=[1]),
                  death(victim=6), death(victim=7)],
                 [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
                  (11, 7, {"deaths": 1}), (11, 0, {"kills": 3}),
                  (11, 1, {"assists": 1})])
    assert [(r["victim_slot"], r["assist_slot"]) for r in result.death_assists] == [(5, 1)]


def test_assist_fallback_preserves_same_side_search_mode():
    result = run([death(victim=5, assist_players=[7]), death(victim=6)],
                 [(11, 5, {"deaths": 1}), (11, 6, {"deaths": 1}),
                  (11, 0, {"kills": 2}), (11, 7, {"assists": 1})])
    assert [(r["victim_slot"], r["assist_slot"], r["same_side"])
            for r in result.death_assists] == [(5, 7, True)]
    assert {r["link_status"] for r in result.deaths} == {"participant_assumed"}


def test_separate_fights_do_not_share_credit():
    result = run([death(), death(tick=100, victim=6, killer=1)],
                 [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1}),
                  (104, 6, {"deaths": 1}), (104, 1, {"kills": 1})])
    assert [r["credited_killer_slot"] for r in result.deaths] == [0, 1]
    assert all(r["link_status"] == "exact" for r in result.deaths)


def test_missing_illusion_flag_blocks_instead_of_silently_dropping():
    event = death()
    del event["attacker_illusion"]
    with pytest.raises(kda.CreditError, match="Explicit illusion flags"):
        run([event], [])


def test_raw_assist_string_requires_explicit_normalization():
    with pytest.raises(kda.CreditError, match="normalized integer slots"):
        run([death(assist_players="0;1")], [])


def test_duplicate_counter_tick_blocks():
    with pytest.raises(kda.CreditError, match="Duplicate counter tick"):
        run([], [(0, 0, {})])


def test_inputs_are_not_modified():
    import copy
    events = [death(assist_players=[1])]
    original = copy.deepcopy(events)
    run(events, [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1}), (11, 1, {"assists": 1})])
    assert events == original


def test_explicit_null_owner_still_merges_self_source_duplicate():
    result = run([death(killer=5, source_owner_slot=None), death(source_owner_slot=None)],
                 [(11, 5, {"deaths": 1}), (11, 0, {"kills": 1})])
    assert len(result.deaths) == 1
