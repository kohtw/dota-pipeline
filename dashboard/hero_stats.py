"""Render aggregate hero statistics from records supplied by the application."""

from collections import Counter
from collections.abc import Callable

import streamlit as st

import event_charts as ec
import hero_charts as hc
from components import format_number, format_percentage
from postgres_connection import DashboardDatabaseError
from hero_matchups import render_hero_allies, render_hero_matchups

# Heroes need this many picks to be ranked, so a rarely played hero cannot top the table by chance.
RANK_MINIMUM_PICKS = 100


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


def _rank(heroes: list[dict], hero: dict, key: str) -> str | None:
    """'#3 of 120' among heroes with RANK_MINIMUM_PICKS picks, or None when the hero has fewer."""
    ranked = sorted((other for other in heroes if other["picks"] >= RANK_MINIMUM_PICKS and other[key] is not None),
                    key=lambda other: -other[key])
    ids = [other["hero_id"] for other in ranked]
    return f"#{ids.index(hero['hero_id']) + 1} of {len(ids)}" if hero["hero_id"] in ids else None


def _render_hero_details(hero: dict, heroes: list[dict] | None = None) -> None:
    """Present one observed hero's counts and rates without applying table filters."""
    st.subheader(hero["hero_label"])
    win_column, pick_column, picks_column, wins_column, losses_column = st.columns(5)
    heroes = heroes or [hero]
    win_rank, pick_rank = _rank(heroes, hero, "win_rate"), _rank(heroes, hero, "pick_rate")
    win_column.metric("Win rate", format_percentage(hero["win_rate"]), help="Wins divided by the hero's picks.")
    pick_column.metric("Pick rate", format_percentage(hero["pick_rate"]), help="Hero picks divided by qualifying matches.")
    picks_column.metric("Picks", format_number(hero["picks"]))
    wins_column.metric("Wins", format_number(hero["wins"]))
    losses_column.metric("Losses", format_number(hero["losses"]))
    if win_rank and pick_rank:
        st.caption(f"Win rate rank {win_rank} · pick rate rank {pick_rank}, among heroes with at least "
                   f"{RANK_MINIMUM_PICKS} picks. — means unavailable.")
    else:
        st.caption(f"Not ranked: fewer than {RANK_MINIMUM_PICKS} picks. — means unavailable.")


def _render_meta_chart(heroes: list[dict], selected_id: int | None) -> None:
    """Every hero by pick and win rate; clicking a bubble opens that hero."""
    st.subheader("Hero meta")
    st.caption("Each bubble is a hero: further right is picked more, higher wins more; bigger means more picks. "
               "The dotted line is a 50% win rate. Click a bubble to open that hero.")
    event = st.plotly_chart(hc.build_meta_chart(heroes, selected_id), key="hero_meta_chart",
                            on_select="rerun", selection_mode="points")
    selection = getattr(event, "selection", None) or (event.get("selection") if isinstance(event, dict) else None)
    points = (selection.get("points") if isinstance(selection, dict) else getattr(selection, "points", None)) or []
    clicked = (points[0].get("customdata") or [None])[0] if points else None
    # A selection persists across reruns; act only on a new click so the selector still works.
    if clicked is not None and clicked != st.session_state.get("hero_meta_clicked"):
        st.session_state["hero_meta_clicked"] = clicked
        if clicked != selected_id:
            st.query_params["hero_id"] = str(clicked)
            st.rerun()


def _render_duration(hero: dict, loader: Callable[[int], list[dict]]) -> None:
    st.subheader("Win rate by game length")
    st.caption("Win rate in matches that ended within each length range. Green bars beat the hero's overall "
               "win rate (dashed line), red bars fall short; n is the number of matches.")
    try:
        rows = loader(hero["hero_id"])
    except DashboardDatabaseError:
        st.warning("Win rates by game length could not be loaded. Rebuild the hero marts.")
        return
    if not rows:
        st.caption("No game-length results for this hero yet.")
        return
    st.plotly_chart(hc.build_duration_chart(rows, hero["win_rate"]), key=f"hero_duration_{hero['hero_id']}")


def _render_items(hero: dict, loader: Callable[[int], list[dict]]) -> None:
    st.subheader("Item timings")
    try:
        rows = loader(hero["hero_id"])
    except DashboardDatabaseError:
        st.warning("Item timings could not be loaded. They need the replay event marts.")
        return
    if not rows:
        st.caption("No parsed replays with this hero yet.")
        return
    matches = rows[0]["hero_matches"]
    st.caption(f"From {matches} parsed replay{'s' if matches != 1 else ''} with this hero, a much smaller sample "
               "than the win rates above. Median time of the first purchase of each item bought in at least a "
               "third of those matches; colour shows how often it was bought.")
    st.plotly_chart(ec.build_item_timing_chart(rows, minimum_rate=1 / 3), key=f"hero_items_{hero['hero_id']}")


def _render_performance_profile(hero: dict, loader: Callable[[int], dict | None]) -> None:
    """Keep an optional profile failure independent of pick/win statistics."""
    st.subheader("Performance profile")
    try:
        profile = loader(hero["hero_id"])
    except DashboardDatabaseError:
        st.warning("The performance profile could not be loaded. Pick/win statistics remain available. Try again after the hero marts have been built.")
        return
    if profile is None:
        st.caption("No performance profile available for this hero yet.")
        return
    if profile["appearances"] != hero["picks"] or profile["wins"] != hero["wins"]:
        st.warning("The hero summaries are out of sync. Rebuild both hero marts before comparing performance.")
        return
    appearances = profile["appearances"]
    st.caption(
        f"Full-match averages across {appearances:,} appearances. "
        f"Average match length: {profile['avg_match_duration_s'] / 60:.1f} minutes. "
    )
    groups = (
        ("Combat", (("Kills", "avg_kills", "kills"), ("Deaths", "avg_deaths", "deaths"),
                    ("Assists", "avg_assists", "assists"))),
        ("Economy", (("Gold / min", "avg_gpm", "gpm"), ("XP / min", "avg_xpm", "xpm"))),
        ("Contribution", (("Hero damage / min", "avg_hero_damage_per_min", "hero_damage"),
                          ("Hero healing / min", "avg_hero_healing_per_min", "hero_healing"),
                          ("Tower damage / min", "avg_tower_damage_per_min", "tower_damage"))),
    )
    for column, (title, metrics) in zip(st.columns(3), groups):
        with column.container(border=True, height="stretch"):
            st.markdown(f"### {title}")
            st.caption("Per match" if title == "Combat" else "Per minute")
            for index, (label, metric, coverage) in enumerate(metrics):
                if index:
                    st.divider()
                average = profile[metric]
                st.metric(label, "—" if average is None else f"{average:,.1f}",
                          help="Average among appearances with this metric recorded. Missing values are excluded; recorded zeros count.")
                observed = profile[coverage + "_observations"]
                if observed != appearances:
                    st.caption(f"Recorded in {observed:,} of {appearances:,} appearances")
    st.caption(
        "Each match has equal weight. Missing values are excluded; — means unavailable. "
        "Metrics use all appearances above unless a smaller recorded count is shown."
    )


def render_hero_statistics(
    heroes: list[dict], *, performance_loader: Callable[[int], dict | None] | None = None,
    matchup_loader: Callable[[int], list[dict]] | None = None,
    duration_loader: Callable[[int], list[dict]] | None = None,
    ally_loader: Callable[[int], list[dict]] | None = None,
    item_loader: Callable[[int], list[dict]] | None = None,
) -> None:
    """Show dataset coverage, one hero's results and a sortable comparison table."""
    st.title("Hero statistics")
    st.caption(
        "Ranked All Pick matches: at least 15 minutes, ten human players and a known result."
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

    _render_meta_chart(heroes, selected_id)

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
        hero = by_id[selected_id]
        _render_hero_details(hero, heroes)
        sections = [(name, render) for name, loader, render in (
            ("Profile", performance_loader, lambda: _render_performance_profile(hero, performance_loader)),
            ("Game length", duration_loader, lambda: _render_duration(hero, duration_loader)),
            ("Matchups", matchup_loader, lambda: render_hero_matchups(hero, matchup_loader)),
            ("Allies", ally_loader, lambda: render_hero_allies(hero, ally_loader)),
            ("Items", item_loader, lambda: _render_items(hero, item_loader)),
        ) if loader is not None]
        if sections:
            for tab, (_, render) in zip(st.tabs([name for name, _ in sections]), sections):
                with tab:
                    render()

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
