"""Presentation helpers shared by the dashboard's display components.

Formatting returns text without changing the numeric values or inventory lists
used for sorting, filtering, and analysis.
"""

from html import escape
from urllib.parse import urlencode

import streamlit as st


# Use one missing-value label across numerical displays so an absent measurement
# cannot be mistaken for a measured zero.
MISSING_VALUE = "—"


def style_sidebar_navigation() -> None:
    """Match navigation typography to page headings while retaining native radio controls."""
    # The named container keeps navigation styling separate from page controls.
    st.html(
        """
        <style>
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioGroup"] {
            width: 100%; gap: .5rem;
        }
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioGroup"] > div {
            width: 100%;
        }
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioOption"] {
            display: block; width: 100%; box-sizing: border-box; padding: .8rem .9rem;
            margin: 0; border: 1px solid transparent; border-radius: .65rem;
            font-family: inherit; font-size: 1.125rem;
            font-weight: 600; transition: background-color .15s ease, border-color .15s ease;
        }
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioOption"] [data-testid="stMarkdownContainer"],
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioOption"] p {
            font-family: inherit; font-size: 1.125rem; font-weight: 600;
            line-height: 1.5; margin: 0;
        }
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioOption"]:hover {
            background-color: rgba(128,128,128,.08);
        }
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioOption"][data-selected] {
            background-color: rgba(128,128,128,.14); border-color: rgba(128,128,128,.35);
        }
        [data-testid="stSidebar"] .st-key-dashboard_navigation [data-testid="stRadioOption"][data-focus-visible] {
            outline: 2px solid currentColor; outline-offset: 2px;
        }
        </style>
        """
    )


def format_number(value: int | None) -> str:
    """Format an integer statistic with thousands separators, or mark it missing."""
    # Check None explicitly: zero is a valid value for gold, kills, and denies.
    if value is None:
        return MISSING_VALUE
    return f"{value:,}"


def format_percentage(value: float | None) -> str:
    """Display a share as a percentage, preserving missing values."""
    return MISSING_VALUE if value is None else f"{value:.1%}"


def format_duration(duration_s: int | float | None) -> str:
    """Display elapsed whole seconds as M:SS, or H:MM:SS for an hour or more."""
    if duration_s is None:
        return MISSING_VALUE
    if duration_s < 0:
        raise ValueError("Match duration must not be negative.")

    # Preserve hours for long matches rather than wrapping the clock at 60
    # minutes. Padding the smaller units makes times unambiguous and easy to scan.
    hours, remainder = divmod(int(duration_s), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def format_items(items: list[str] | None) -> str:
    """Display main-inventory items, distinguishing missing and empty snapshots."""
    # An unavailable inventory describes missing data; a known empty list describes
    # the player's inventory state. Keeping these labels separate preserves that
    # distinction for the user.
    if items is None:
        return "Unavailable"
    if not items:
        return "No items in main inventory"
    return ", ".join(items)


def render_match_header(match: dict) -> None:
    """Render match context using metadata supplied by the application's loader."""
    st.title("Match overview")

    # Resolve the raw team ID to a readable label without assigning a winner
    # when the supplied outcome is missing or unrecognised.
    winner = {2: "Radiant", 3: "Dire"}.get(match.get("winner_team"))
    winner_label = f"{winner} victory" if winner else "Unavailable"
    match_id = match.get("match_id")

    # Present the main match facts together. IDs remain ungrouped identifiers,
    # while duration uses the shared formatter for consistent clock notation.
    match_column, winner_column, duration_column = st.columns(3)
    match_column.metric("Match ID", str(match_id) if match_id is not None else MISSING_VALUE)
    winner_column.metric("Result", winner_label)
    duration_column.metric("Duration", format_duration(match.get("duration_s")))


def render_team_scoreboard(team_label: str, players: list[dict]) -> None:
    """Render the supplied team's participants in a consistent scoreboard layout."""
    st.subheader(team_label)
    if not players:
        st.info("No participants are available for this team.")
        return

    st.caption(
        "K / D / A: kills / deaths / assists · LH: last hits · DN: denies. "
        "KP is the percentage of team kills the player scored or assisted. "
        "KP shows — when the team had zero kills or the data is unavailable. "
        "Gold is unspent gold. Final main inventory shows items held at match end, "
        "including passive items; backpack, stash, neutral-slot items, "
        "and consumed upgrades are excluded. — means unavailable."
    )

    # A native HTML table keeps headers and cells aligned while giving short
    # statistics only the space they need. Escaping all record values prevents
    # hero and item names from being interpreted as HTML.
    labels = ["Hero", "K", "D", "A", "LH", "DN", "Net worth", "Gold", "KP", "Final main inventory"]
    header = "".join(f'<th scope="col">{escape(label)}</th>' for label in labels)
    # Keep these fields in header order. KP and inventory use their own formatters.
    stat_fields = ["kills", "deaths", "assists", "last_hits", "denies", "net_worth", "gold"]
    rows = []
    for player in sorted(players, key=lambda participant: participant["slot"]):
        hero = escape(player.get("hero_label") or player.get("hero") or MISSING_VALUE)
        # Encode identity in an ordinary same-tab link so refreshes retain the
        # selected participant without requiring JavaScript inside the table.
        query = urlencode({"match_id": player["match_id"], "player_slot": player["slot"]})
        link = f'<a href="?{escape(query, quote=True)}" target="_self">{hero}</a>'
        cells = f'<td class="hero">{link}</td>'
        for field in stat_fields:
            cells += f'<td class="number">{escape(format_number(player.get(field)))}</td>'
        cells += f'<td class="number">{format_percentage(player.get("kill_participation"))}</td>'
        cells += f'<td class="items">{escape(format_items(player.get("main_inventory")))}</td>'
        rows.append(f"<tr>{cells}</tr>")

    # Scope styles to the scoreboard so the match header and other Streamlit
    # elements retain their normal spacing. Numeric columns can grow for unusual
    # values; narrower windows scroll the table instead of clipping its contents.
    st.html(
        """
        <style>
        .dota-scoreboard { width: 100%; max-width: 1280px; overflow-x: auto; }
        .dota-scoreboard table { width: 100%; min-width: 680px; border-collapse: collapse;
            font-family: inherit; font-size: .9rem; color: inherit; }
        .dota-scoreboard th, .dota-scoreboard td { padding: .5rem .65rem;
            border-bottom: 1px solid rgba(128,128,128,.25); vertical-align: middle;
            text-align: left; }
        .dota-scoreboard th { font-weight: 600; white-space: nowrap; }
        .dota-scoreboard .hero { min-width: 145px; }
        .dota-scoreboard .hero a { display: block; color: inherit; text-decoration: underline;
            text-underline-offset: .17em; }
        .dota-scoreboard .hero a:hover { text-decoration-thickness: 2px; }
        .dota-scoreboard .hero a:focus-visible { outline: 2px solid currentColor;
            outline-offset: 3px; }
        .dota-scoreboard .number { width: 1%; white-space: nowrap;
            text-align: right; font-variant-numeric: tabular-nums; }
        .dota-scoreboard th:nth-child(n+2):nth-child(-n+9) { text-align: right; }
        .dota-scoreboard .items { width: 42%; min-width: 240px; overflow-wrap: anywhere; }
        @media (max-width: 800px) {
            .dota-scoreboard th, .dota-scoreboard td { padding-inline: .45rem; }
            .dota-scoreboard .hero { min-width: 125px; }
            .dota-scoreboard .items { min-width: 200px; }
        }
        </style>
        """
        f'<div class="dota-scoreboard"><table aria-label="{escape(team_label, quote=True)} scoreboard">'
        f"<thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )
