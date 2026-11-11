"""Route between the match scoreboard and an individual participant's view."""

from collections import Counter
from os import getenv
from pathlib import Path

import streamlit as st

from components import render_match_header, render_team_scoreboard
from dashboard_db import (
    DashboardDatabaseError,
    load_default_match_id,
    load_player_damage,
    load_player_snapshots,
    load_scoreboard,
)
from player import render_player_page


# The repo's fixture path identifies which database gets the fictional-data label.
DEV_DATABASE_PATH = Path(__file__).resolve().parents[1] / "output" / "dashboard-dev.duckdb"


def return_to_scoreboard(match_id: int) -> None:
    """Keep match context in the URL while removing the player selection."""
    # A button callback updates the URL before Streamlit reruns the application,
    # allowing the router to render the scoreboard immediately.
    st.query_params["match_id"] = str(match_id)
    if "player_slot" in st.query_params:
        del st.query_params["player_slot"]


def show_navigation_error(message: str, match_id: int) -> None:
    """Explain an invalid link and provide a route to the available scoreboard."""
    st.error(message)
    st.button(
        "Back to scoreboard", on_click=return_to_scoreboard,
        args=(match_id,), key="invalid_link_back",
    )


def main() -> None:
    """Validate match and participant identity before rendering the selected view."""
    # Give the scoreboard enough horizontal room for names, statistics, and items.
    # Display functions receive records and do not need to know how they were loaded.
    st.set_page_config(page_title="Dota 2 Match Analysis", layout="wide")
    database_path = getenv("DUCKDB_PATH")
    if not database_path:
        st.error("Set `DUCKDB_PATH` to your dbt-built DuckDB file and restart Streamlit.")
        return
    try:
        default_match_id = load_default_match_id(database_path)
    except DashboardDatabaseError as error:
        st.error(str(error))
        return
    if default_match_id is None:
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

    try:
        match, players = load_scoreboard(database_path, requested_match)
    except DashboardDatabaseError as error:
        st.error(str(error))
        return
    if not players:
        show_navigation_error("The match requested in this link is not available.", default_match_id)
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

    if Path(database_path).resolve() == DEV_DATABASE_PATH:
        st.info("Development database — this match and its statistics are fictional.")

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
        try:
            snapshots_by_slot = load_player_snapshots(database_path, requested_match, requested_slot, other_slot)
        except DashboardDatabaseError as error:
            st.error(str(error))
            return

        # Hero damage is an optional section with its own data query.
        damage_by_slot = None
        try:
            damage_by_slot = load_player_damage(database_path, requested_match, requested_slot, other_slot)
        except DashboardDatabaseError as error:
            st.error(str(error))

        render_player_page(
            match,
            selected_player,
            snapshots_by_slot.get(requested_slot),
            comparison_player=comparison_player,
            comparison_snapshots=snapshots_by_slot.get(other_slot),
            damage_by_slot=damage_by_slot,
            roster=players,
        )
        return

    # Missing optional statistics are formatted by the components rather than
    # rejected here, allowing incomplete records to remain visible and honest.
    render_match_header(match)
    st.caption("Select a hero or player name to open their individual match analysis.")
    st.divider()

    # Both teams use the same renderer so their column layouts and formatting
    # stay consistent. Filtering creates new lists without changing source data.
    radiant_players = [player for player in players if player["team"] == 2]
    dire_players = [player for player in players if player["team"] == 3]
    render_team_scoreboard("Radiant", radiant_players)
    render_team_scoreboard("Dire", dire_players)


# Importing the module exposes main without starting the interface; Streamlit's
# script execution calls it to assemble the page for each run.
if __name__ == "__main__":
    main()
