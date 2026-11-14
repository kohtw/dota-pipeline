"""Render observed whole-match opponent results supplied by the application."""

from collections import Counter
from collections.abc import Callable

import streamlit as st

from components import format_number, format_percentage
from postgres_connection import DashboardDatabaseError


def render_hero_matchups(hero: dict, loader: Callable[[int], list[dict]]) -> None:
    st.subheader("Results against enemy heroes")
    st.caption(
        "How often this hero's team won matches against each enemy hero."
    )
    try:
        matchups = loader(hero["hero_id"])
    except DashboardDatabaseError:
        st.warning("Opponent results could not be loaded. Hero statistics and the performance profile remain available.")
        return
    if not matchups:
        st.caption("No opponent results available for this hero yet.")
        return
    if any(row["overall_matches"] != hero["picks"] or hero["win_rate"] is None
           or abs(row["overall_win_rate"] - hero["win_rate"]) > 0.000001 for row in matchups):
        st.warning("The hero summaries are out of sync. Rebuild the hero marts before comparing opponent results.")
        return

    labels = {row["opponent_hero_id"]: row["opponent_label"] for row in matchups}
    duplicates = Counter(labels.values())
    labels = {hero_id: f"{label} · Hero {hero_id}" if duplicates[label] > 1 else label
              for hero_id, label in labels.items()}
    scope = f"hero_matchups_{hero['hero_id']}"
    search_column, count_column = st.columns(2)
    search = search_column.text_input("Search opponents", key=f"{scope}_search").strip().casefold()
    minimum = count_column.number_input("Minimum matchup matches", min_value=1, value=1, step=1, key=f"{scope}_minimum")
    visible = [row for row in matchups if row["matches"] >= minimum
               and search in f"{labels[row['opponent_hero_id']]} {row['opponent_hero_id']}".casefold()]
    visible.sort(key=lambda row: (-row["matches"], row["opponent_hero_id"]))
    if not visible:
        st.info("No opponents meet these filters. Clear the search or lower Minimum matchup matches.")
        return
    st.caption(
        "Difference (pp) = matchup win rate minus the hero's overall win rate shown above. "
        "For example, 55% against an opponent versus 50% overall is +5 pp. "
        "Opponents with the most matches appear first."
    )
    st.dataframe(
        [{"Opponent": labels[row["opponent_hero_id"]], "Matches": row["matches"],
          "Wins": row["wins"], "Losses": row["losses"], "Matchup win rate": 100 * row["win_rate"],
          "Difference (pp)": row["win_rate_difference_pp"]} for row in visible],
        hide_index=True, width="stretch", key="hero_matchups_table",
        column_config={
            "Matchup win rate": st.column_config.NumberColumn(format="%.1f%%"),
            "Difference (pp)": st.column_config.NumberColumn(format="%+.1f pp"),
        },
    )
    options = [row["opponent_hero_id"] for row in visible]
    selection_key = f"{scope}_opponent"
    if st.session_state.get(selection_key) not in options:
        st.session_state[selection_key] = None
    selected = st.selectbox("Inspect an opponent", options, index=None, placeholder="Choose an opponent",
                            format_func=labels.__getitem__, key=selection_key)
    if selected is not None:
        row = next(row for row in visible if row["opponent_hero_id"] == selected)
        st.markdown("**Selected matchup**")
        st.text(f"{hero['hero_label']} against {labels[selected]}")
        for column, (label, value) in zip(st.columns(3), (
            ("Matchup matches", format_number(row["matches"])),
            ("Matchup win rate", format_percentage(row["win_rate"])),
            ("Difference (pp)", f"{row['win_rate_difference_pp']:+.1f} pp"),
        )):
            column.metric(label, value)
        st.caption(f"{row['wins']:,} wins and {row['losses']:,} losses, from {hero['hero_label']}'s perspective.")
