"""Render aggregate hero statistics from records supplied by the application."""

from collections import Counter

import streamlit as st

from components import format_number, format_percentage


def select_hero() -> None:
    """Keep the chosen hero in the URL and remove unrelated replay selections."""
    selected_id = st.session_state["hero_stats_selected_id"]
    st.query_params["view"] = "heroes"
    if selected_id is not None:
        st.query_params["hero_id"] = str(selected_id)
    elif "hero_id" in st.query_params:
        del st.query_params["hero_id"]
    for key in ("match_id", "player_slot"):
        if key in st.query_params:
            del st.query_params[key]


def _render_hero_details(hero: dict) -> None:
    """Present one observed hero's counts and rates without applying table filters."""
    st.subheader(hero["hero_label"])
    win_column, pick_column, picks_column, wins_column, losses_column = st.columns(5)
    win_column.metric("Win rate", format_percentage(hero["win_rate"]), help="Wins divided by the hero's picks.")
    pick_column.metric("Pick rate", format_percentage(hero["pick_rate"]), help="Hero picks divided by qualifying matches.")
    picks_column.metric("Picks", format_number(hero["picks"]))
    wins_column.metric("Wins", format_number(hero["wins"]))
    losses_column.metric("Losses", format_number(hero["losses"]))
    st.caption("Rates are shown alongside their pick counts; small samples can give unstable win rates. — means unavailable.")


def render_hero_statistics(heroes: list[dict]) -> None:
    """Show dataset coverage, one hero's results and a sortable comparison table."""
    st.title("Hero statistics")
    st.caption(
        "Collected ranked All Pick matches: at least 15 minutes, ten human players and a known result. "
        "These statistics describe the collected sample."
    )
    if not heroes:
        st.info("No hero statistics available yet.")
        return

    # The loader validates the shared denominator; filters only change the comparison table.
    match_column, hero_column = st.columns(2)
    match_column.metric("Qualifying matches", format_number(heroes[0]["ranked_matches"]))
    hero_column.metric("Heroes represented", format_number(len(heroes)))

    by_id = {hero["hero_id"]: hero for hero in heroes}
    labels = {hero_id: hero["hero_label"] for hero_id, hero in by_id.items()}
    repeated = Counter(labels.values())
    for hero_id, label in labels.items():
        if repeated[label] > 1:
            labels[hero_id] = f"{label} · Hero {hero_id}"
    options = sorted(by_id, key=lambda hero_id: (labels[hero_id].casefold(), hero_id))
    requested = st.query_params.get_all("hero_id")
    selected_id = options[0]
    if len(requested) > 1:
        st.error("This link contains repeated hero selections. Choose one hero.")
        selected_id = None
    elif requested:
        try:
            selected_id = int(requested[0])
            if not 0 < selected_id < 2**31:
                raise ValueError
        except ValueError:
            st.error("The hero ID in this link must be a positive integer within the supported range.")
            selected_id = None
        else:
            if selected_id not in by_id:
                st.warning("No recorded observations are available for the requested hero. Choose another hero.")
                selected_id = None

    # URL selection takes precedence over a previous widget value, including an invalid old link.
    st.session_state["hero_stats_selected_id"] = selected_id
    selected_id = st.selectbox(
        "Hero", options=options, index=None, placeholder="Choose a hero",
        format_func=labels.__getitem__, key="hero_stats_selected_id", on_change=select_hero,
    )
    if selected_id is not None:
        if not requested:
            # Recording the default also makes a refreshed link independent of later label ordering.
            select_hero()
        _render_hero_details(by_id[selected_id])

    st.subheader("Compare heroes")
    minimum_picks = st.number_input("Minimum picks", min_value=1, value=1, step=1, key="hero_stats_minimum_picks")
    visible = [hero for hero in heroes if hero["picks"] >= minimum_picks]
    if not visible:
        st.info("No heroes meet the minimum picks. Lower the threshold to compare heroes.")
        return

    # Only the displayed table converts ratios to percentage points; source records remain unchanged.
    rows = [
        {"Hero": labels[hero["hero_id"]], "Picks": hero["picks"], "Wins": hero["wins"],
         "Losses": hero["losses"],
         "Pick rate": None if hero["pick_rate"] is None else 100 * hero["pick_rate"],
         "Win rate": None if hero["win_rate"] is None else 100 * hero["win_rate"]}
        for hero in sorted(visible, key=lambda hero: (-hero["picks"], hero["hero_id"]))
    ]
    st.caption("Click a column heading to sort. Pick rate = picks ÷ qualifying matches; win rate = wins ÷ picks.")
    st.dataframe(
        rows, hide_index=True, width="stretch", key="hero_stats_comparison",
        column_config={
            "Pick rate": st.column_config.NumberColumn(format="%.1f%%"),
            "Win rate": st.column_config.NumberColumn(format="%.1f%%"),
        },
    )
