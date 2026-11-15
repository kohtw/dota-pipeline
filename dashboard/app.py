"""Route between hero statistics, match scoreboards and individual player views."""

from collections import Counter

import streamlit as st

import events_db
from components import render_match_header, render_team_scoreboard, style_sidebar_navigation
from config import DashboardConfigurationError, WarehouseConfig, load_warehouse_config
from dashboard_db import (
    DashboardDatabaseError,
    load_available_matches,
    load_default_match_id,
    load_hero_statistics,
    load_hero_performance,
    load_hero_matchups,
    load_hero_duration,
    load_hero_allies,
    load_player_damage,
    load_player_inventory,
    load_player_snapshots,
    load_scoreboard,
    load_team_advantage,
)
from player import render_player_page
from hero_stats import render_hero_statistics
from insights import render_replay_insights
from match_sections import (
    MATCH_SECTIONS, PLAYER_SECTIONS, hero_labels, render_match_section, render_match_story, render_player_section,
    section_switcher,
)
from team_advantage import render_team_advantage
from match_search import render_match_examples, render_match_search


VIEW_LABELS = {"heroes": "Hero statistics", "matches": "Match analysis", "insights": "Replay insights"}


def switch_view() -> None:
    """Write the selected section to the URL and remove the other section's selection."""
    view = st.session_state["dashboard_view"]
    st.query_params["view"] = view
    match_keys = ("match_id", "player_slot", "tab", "player_tab")
    unrelated = {"heroes": match_keys, "insights": (*match_keys, "hero_id")}.get(view, ("hero_id",))
    for key in unrelated:
        if key in st.query_params:
            del st.query_params[key]


def open_match(match_id: int) -> None:
    """Route to a match scoreboard without retaining a player or aggregate-hero selection."""
    st.query_params["view"] = "matches"
    st.query_params["match_id"] = str(match_id)
    for key in ("player_slot", "hero_id", "player_tab"):
        if key in st.query_params:
            del st.query_params[key]


def return_to_scoreboard(match_id: int) -> None:
    """Open an available scoreboard and align the search field with its match."""
    # Navigation callbacks run before the form is created, so its input can be updated safely.
    st.session_state["match_search_input"] = str(match_id)
    open_match(match_id)


def show_navigation_error(message: str, match_id: int | None) -> None:
    """Explain a navigation failure and offer an available scoreboard when present."""
    st.error(message)
    if match_id is not None:
        st.button(
            "Back to scoreboard", on_click=return_to_scoreboard,
            args=(match_id,), key="invalid_link_back",
        )


def render_match_analysis(config: WarehouseConfig) -> None:
    """Load and validate replay data only when the match section is selected."""
    submitted, match_id = render_match_search()
    if submitted:
        if match_id is None:
            # Invalid input must not leave the previous match visible as an apparent search result.
            return
        open_match(match_id)
        # The new run follows the canonical URL and loads the requested scoreboard once.
        st.rerun()
    try:
        default_match_id = load_default_match_id(config)
    except DashboardDatabaseError as error:
        st.error(str(error))
        return
    if default_match_id is not None:
        try:
            examples = load_available_matches(config)
        except DashboardDatabaseError:
            # Examples are a convenience; their failure must not block a known match or search.
            st.warning("Available examples could not be loaded. You can still search for a match.")
        else:
            render_match_examples(examples, return_to_scoreboard)
    if default_match_id is None and "match_id" not in st.query_params:
        st.info("No matches are available in the scoreboard mart.")
        return

    # Treat URL parameters as untrusted selections. Reject malformed, repeated,
    # or unavailable match identifiers rather than silently showing another match.
    if any(len(st.query_params.get_all(key)) > 1 for key in ("match_id", "player_slot")):
        show_navigation_error("This link contains repeated match or player selections.", default_match_id)
        return
    try:
        requested_match = int(st.query_params.get("match_id", str(default_match_id)))
    except ValueError:
        show_navigation_error("The match ID in this link must be an integer.", default_match_id)
        return
    if not 0 < requested_match < 2**63:
        show_navigation_error("The match ID in this link is outside the supported range.", default_match_id)
        return

    if default_match_id is None:
        show_navigation_error(
            f"Match {requested_match} is not available in this dashboard. No analysed matches are currently available.",
            None,
        )
        return

    try:
        match, players = load_scoreboard(config, requested_match)
    except DashboardDatabaseError as error:
        st.error(str(error))
        return
    if not players:
        show_navigation_error(
            f"Match {requested_match} is not available in this dashboard. Enter another match ID or choose an available example.",
            default_match_id,
        )
        return

    # Reject inconsistent identities before displaying any statistics so the page
    # cannot silently combine different matches or show a participant twice.
    slots = [player.get("slot") for player in players]
    if any(not isinstance(slot, int) or isinstance(slot, bool) for slot in slots):
        st.error("Cannot display the scoreboard: every participant needs an integer player slot.")
        return
    if len(slots) != len(set(slots)):
        st.error("Cannot display the scoreboard: duplicate player slots were found.")
        return
    if any(player.get("match_id") != match["match_id"] for player in players):
        st.error("Cannot display the scoreboard: a participant belongs to a different match.")
        return
    if any(player.get("team") not in (2, 3) for player in players):
        st.error("Cannot display the scoreboard: a participant has an invalid team.")
        return

    if any(p.get("assist_attribution_complete") is False for p in players):
        st.info("Some assists could not be linked to a specific death. Recorded K/D/A is retained; "
                "kill participation is unavailable for this match.")
    elif any(p.get("assist_attribution_complete") is None for p in players):
        st.info("Assist attribution has not been verified for this match. Kill participation is unavailable.")
    if any(p.get("assist_attribution_assumed") is True for p in players):
        st.caption("Some event links depend on combat-log participant assumptions.")

    # Route from the URL rather than a second selection in Session State, so a
    # refreshed or bookmarked link always identifies the same participant.
    if "player_slot" in st.query_params:
        try:
            requested_slot = int(st.query_params["player_slot"])
        except ValueError:
            show_navigation_error("The player slot in this link must be an integer.", match["match_id"])
            return
        selected_player = next((player for player in players if player["slot"] == requested_slot), None)
        if selected_player is None:
            show_navigation_error("The player requested in this link is not in this match.", match["match_id"])
            return
        st.button(
            "Back to scoreboard", on_click=return_to_scoreboard,
            args=(match["match_id"],), key="player_back",
        )

        # Use slots as option values so players with similar labels stay distinct.
        # Scope the widget to the primary player and match to keep selections local.
        team_labels = {2: "Radiant", 3: "Dire"}
        comparison_labels = {None: "None"}
        for player in players:
            slot = player["slot"]
            if slot == requested_slot:
                continue
            hero = player.get("hero_label") or player.get("hero") or f"Player slot {slot}"
            comparison_labels[slot] = f"{hero} · {team_labels[player['team']]}"

        # Streamlit uses the displayed labels to resolve selections, so heroes
        # with the same label and team need distinct entries in the dropdown.
        label_counts = Counter(comparison_labels.values())
        for slot, label in comparison_labels.items():
            if label_counts[label] > 1:
                comparison_labels[slot] = f"{label} · Slot {slot}"
        comparison_slot = st.selectbox(
            "Compare with",
            options=list(comparison_labels),
            format_func=comparison_labels.__getitem__,
            key=f"comparison_{match['match_id']}_{requested_slot}",
        )

        # Resolve the comparison slot against this match's roster before loading it.
        comparison_player = next(
            (player for player in players
             if player["slot"] == comparison_slot and player["slot"] != requested_slot),
            None,
        )
        other_slot = comparison_player["slot"] if comparison_player is not None else None

        # Summary is the original player page; the other sections draw the event marts.
        player_section = section_switcher(
            PLAYER_SECTIONS, "player_tab", f"player_section_{match['match_id']}_{requested_slot}",
        )
        if player_section != "summary":
            render_player_section(player_section, config, match, selected_player, players, comparison_player)
            return
        try:
            snapshots_by_slot = load_player_snapshots(config, requested_match, requested_slot, other_slot)
        except DashboardDatabaseError as error:
            st.error(str(error))
            return

        # Hero damage is an optional section with its own data query.
        damage_by_slot = None
        try:
            damage_by_slot = load_player_damage(config, requested_match, requested_slot, other_slot)
        except DashboardDatabaseError as error:
            st.error(str(error))

        inventory_by_slot = None
        try:
            inventory_by_slot = load_player_inventory(config, requested_match, requested_slot, other_slot)
        except DashboardDatabaseError:
            st.warning("Final inventory could not be loaded. Other player analysis remains available.")

        render_player_page(
            match,
            selected_player,
            snapshots_by_slot.get(requested_slot),
            comparison_player=comparison_player,
            comparison_snapshots=snapshots_by_slot.get(other_slot),
            damage_by_slot=damage_by_slot,
            roster=players,
            team_advantage=_load_team_advantage(config, requested_match),
            inventory_by_slot=inventory_by_slot,
            fight_context=_load_fight_context(config, requested_match, players),
        )
        return

    # Missing optional statistics are formatted by the components rather than
    # rejected here, allowing incomplete records to remain visible and honest.
    render_match_header(match)
    section = section_switcher(MATCH_SECTIONS, "tab", f"match_section_{match['match_id']}")
    if section != "overview":
        render_match_section(section, config, requested_match, players)
        return
    st.caption("Select a hero to open their individual match analysis.")
    st.divider()

    # Both teams use the same renderer so their column layouts and formatting
    # stay consistent. Filtering creates new lists without changing source data.
    _add_damage_taken(config, requested_match, players)
    radiant_players = [player for player in players if player["team"] == 2]
    dire_players = [player for player in players if player["team"] == 3]
    render_team_scoreboard("Radiant", radiant_players)
    render_team_scoreboard("Dire", dire_players)
    team_advantage = _load_team_advantage(config, requested_match)
    fight_context = _load_fight_context(config, requested_match, players)
    render_match_story(config, requested_match, players, team_advantage,
                       deaths=fight_context.get("deaths"), fights=fight_context.get("fights"))
    st.divider()
    render_team_advantage(team_advantage, requested_match, **fight_context)


def _add_damage_taken(config: WarehouseConfig, match_id: int, players: list[dict]) -> None:
    """Damage each hero took from enemy heroes, from the optional damage mart; left missing (—) without it."""
    try:
        timeline = events_db.load_damage_timeline(config, match_id)
    except DashboardDatabaseError:
        return
    taken = Counter()
    for row in timeline:
        taken[row["player_slot"]] += row["damage_from_heroes"]
    for player in players:
        if timeline:
            player["damage_taken"] = taken[player["slot"]]


def _load_fight_context(config: WarehouseConfig, match_id: int, players: list[dict]) -> dict:
    """Deaths and fights for the key-teamfight markers; empty when the event marts are unavailable."""
    try:
        return {"deaths": events_db.load_match_deaths(config, match_id),
                "fights": events_db.load_match_fights(config, match_id),
                "labels": hero_labels(players)}
    except DashboardDatabaseError:
        return {}


def _load_team_advantage(config: WarehouseConfig, match_id: int) -> list[dict] | None:
    """An unavailable optional team mart must not hide scoreboards or player analysis."""
    try:
        return load_team_advantage(config, match_id)
    except DashboardDatabaseError:
        st.warning("Team advantage could not be loaded. Other match analysis remains available.")
        return None


def main() -> None:
    """Configure the app and route each section independently of the replay dataset."""
    st.set_page_config(page_title="Dota 2 Analytics", layout="wide")
    # Existing match/player links keep their original route; a new visit opens hero statistics.
    default_view = "matches" if any(key in st.query_params for key in ("match_id", "player_slot")) else "heroes"
    requested_view = st.query_params.get("view", default_view)
    view = requested_view if requested_view in VIEW_LABELS else default_view

    # The URL owns routing, including links opened after a previous widget interaction.
    st.session_state["dashboard_view"] = view
    style_sidebar_navigation()
    with st.sidebar.container(key="dashboard_navigation"):
        st.header("Explore")
        st.radio(
            "Explore", options=list(VIEW_LABELS), format_func=VIEW_LABELS.__getitem__,
            index=None, key="dashboard_view", on_change=switch_view, label_visibility="collapsed",
        )
    if len(st.query_params.get_all("view")) > 1 or requested_view not in VIEW_LABELS:
        st.error("This link contains an invalid dashboard section. Choose a section from the navigation.")
        return

    try:
        # Resolve shared settings without querying either section's marts during startup.
        config = load_warehouse_config()
    except DashboardConfigurationError as error:
        st.error(str(error))
        return

    if view == "insights":
        render_replay_insights(config)
        return
    if view == "heroes":
        try:
            heroes = load_hero_statistics(config)
        except DashboardDatabaseError as error:
            st.title("Hero statistics")
            st.error(str(error))
            return
        render_hero_statistics(
            heroes, performance_loader=lambda hero_id: load_hero_performance(config, hero_id),
            matchup_loader=lambda hero_id: load_hero_matchups(config, hero_id),
            duration_loader=lambda hero_id: load_hero_duration(config, hero_id),
            ally_loader=lambda hero_id: load_hero_allies(config, hero_id),
            item_loader=lambda hero_id: events_db.load_hero_item_timings_by_id(config, hero_id),
        )
        return
    render_match_analysis(config)


# Importing the module exposes main without starting the interface; Streamlit's
# script execution calls it to assemble the page for each run.
if __name__ == "__main__":
    main()
