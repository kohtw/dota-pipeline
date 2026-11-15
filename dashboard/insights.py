"""Replay insights: patterns across every parsed replay, from the event-level marts.

The Steam-based Hero statistics section covers thousands of match summaries; this section covers
the much smaller set of parsed replays, but sees inside each game: where and when heroes die, how
fights and leads turn into wins, and when heroes finish their items.
"""

from collections import Counter, defaultdict

import streamlit as st

import event_charts as ec
import events_db
from components import format_duration, format_number
from match_sections import _cards
from postgres_connection import DashboardDatabaseError


def render_replay_insights(config) -> None:
    st.title("Replay insights")
    st.caption("Patterns across every parsed replay. A smaller sample than Hero statistics, "
               "but one that sees inside each game.")
    try:
        overview = events_db.load_replay_overview(config)
    except DashboardDatabaseError as error:
        st.error(str(error))
        return
    if not overview["matches"]:
        st.info("No parsed replays are available yet.")
        return
    matches = overview["matches"]
    _cards([
        ("Parsed matches", format_number(matches), "replays through parse, clean and dbt"),
        ("Average length", format_duration(overview["avg_duration_s"]), "game clock"),
        ("Deaths per match", f"{(overview['deaths'] or 0) / matches:.1f}", "scoreboard deaths"),
        ("Teamfights per match", f"{(overview['teamfights'] or 0) / matches:.1f}", "3+ deaths, under 15 s apart"),
        ("Average first blood", format_duration(max(0, overview["avg_first_blood_s"] or 0)), "game clock"),
        ("Average time dead", f"{overview['avg_dead_s'] or 0:.0f} s", "per death, until respawn or buyback"),
    ])

    st.subheader("Does an early lead hold?")
    st.caption("How often the team ahead in net worth at 10, 20 and 30 minutes went on to win, by how far ahead "
               "it was. The dotted line is a coin flip; n is the number of matches.")
    try:
        rates = ec.lead_win_rates(events_db.load_lead_outcomes(config))
        st.plotly_chart(ec.build_lead_chart(rates), key="insights_leads")
    except DashboardDatabaseError:
        st.warning("Lead outcomes could not be loaded.")

    _render_lane_outcomes(config)

    st.subheader("When do heroes die?")
    st.caption("Deaths per match in each game minute (bars) and how long heroes stay dead (line). "
               "Minutes reached by fewer than five matches are left out.")
    try:
        st.plotly_chart(ec.build_deaths_by_minute(events_db.load_deaths_by_minute(config)), key="insights_minutes")
    except DashboardDatabaseError:
        st.warning("Deaths by minute could not be loaded.")

    st.subheader("What lands the killing blow?")
    st.caption("Who or what landed the last hit on each death (log scale). Kill credit can still go to a hero: "
               "a tower's last hit counts for the hero who earned it.")
    try:
        st.plotly_chart(ec.build_killer_kinds(events_db.load_killer_kinds(config)), key="insights_killers")
    except DashboardDatabaseError:
        st.warning("Killing blows could not be loaded.")

    _render_item_timings(config)


def _render_lane_outcomes(config) -> None:
    st.subheader("Does winning lane win the game?")
    try:
        rows = events_db.load_lane_outcomes(config)
    except DashboardDatabaseError:
        st.warning("Lane outcomes could not be loaded.")
        return
    lanes = defaultdict(dict)
    for row in rows:
        if row["measured"] == row["players"]:
            lanes[(row["match_id"], row["lane"])][row["team"]] = row
    results = Counter()
    totals = Counter()
    for (match_id, lane), sides in lanes.items():
        if set(sides) != {2, 3}:
            continue
        lead = sides[2]["net_worth_10"] - sides[3]["net_worth_10"]
        if lead == 0:
            continue
        winner = 2 if lead > 0 else 3
        totals[lane] += 1
        results[lane] += bool(sides[winner]["won"])
    if not totals:
        st.caption("Not enough laning data yet.")
        return
    st.caption("For each lane, the side with more net worth there at 10:00 won the lane. "
               "This shows how often the lane winner's team then won the match.")
    _cards([(f"{label} lane", f"{results[lane] / totals[lane]:.0%}", f"of {totals[lane]} lanes won led to a win")
            for lane, label in (("top", "Top"), ("mid", "Mid"), ("bot", "Bot")) if totals[lane]])


def _render_item_timings(config) -> None:
    st.subheader("Item timings by hero")
    try:
        heroes = events_db.load_item_timing_heroes(config)
    except DashboardDatabaseError:
        st.warning("Item timings could not be loaded.")
        return
    if not heroes:
        st.caption("No purchases recorded yet.")
        return
    names = {row["hero_name"]: (row["hero_localized_name"] or row["hero_name"].removeprefix("npc_dota_hero_")
                                .replace("_", " ").title()) for row in heroes}
    counts = {row["hero_name"]: row["hero_matches"] for row in heroes}
    left, right = st.columns([3, 2])
    hero = left.selectbox("Hero", list(names), key="insights_item_hero",
                          format_func=lambda name: f"{names[name]} · {counts[name]} matches")
    minimum = right.slider("Bought in at least", min_value=0, max_value=100, value=40, step=10, format="%d%%",
                           key="insights_item_rate")
    try:
        rows = events_db.load_hero_item_timings(config, hero)
    except DashboardDatabaseError as error:
        st.warning(str(error))
        return
    shown = [row for row in rows if row["purchase_rate"] * 100 >= minimum]
    if not shown:
        st.info("No items meet that threshold. Lower it to see more.")
        return
    st.caption(f"When {names[hero]} first bought each item, as the median over the matches where it did. "
               "Colour shows how often it was bought. Consumables are left out; a recipe counts as its finished item.")
    st.plotly_chart(ec.build_item_timing_chart(shown, minimum_rate=minimum / 100), key="insights_item_chart")
