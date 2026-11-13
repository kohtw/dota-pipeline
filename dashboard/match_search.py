"""Render the match search form and validate submitted warehouse identifiers."""

from re import fullmatch
from collections.abc import Callable

import streamlit as st


MAX_MATCH_ID = 2**63 - 1


def parse_match_id(value: str) -> int:
    """Accept an ASCII decimal identifier within Postgres's positive BIGINT range."""
    text = value.strip()
    if not text:
        raise ValueError("Enter a match ID.")
    if fullmatch(r"[0-9]+", text) is None:
        raise ValueError("Use digits only for the match ID.")

    # Bound conversion first so very long input cannot reach Python's integer-string limit.
    digits = text.lstrip("0") or "0"
    if len(digits) > len(str(MAX_MATCH_ID)):
        raise ValueError("The match ID is outside the supported range.")
    match_id = int(digits)
    if not 1 <= match_id <= MAX_MATCH_ID:
        raise ValueError("Enter a positive match ID within the supported range.")
    return match_id


def render_match_search() -> tuple[bool, int | None]:
    """Return the submission state and valid ID, displaying validation errors in place."""
    st.subheader("Find a match")
    st.caption("Search matches already available in this dashboard.")
    # A form batches typing until submission, and a text field preserves large identifiers exactly.
    with st.form("match_search", clear_on_submit=False):
        value = st.text_input("Match ID", placeholder="9079738443", key="match_search_input")
        submitted = st.form_submit_button("View match")
    if not submitted:
        return False, None
    try:
        return True, parse_match_id(value)
    except ValueError as error:
        st.error(str(error))
        return True, None


def render_match_examples(match_ids: list[int], on_select: Callable[[int], None]) -> None:
    """Offer known analysed matches using the application's scoreboard navigation callback."""
    if not match_ids:
        return
    st.caption("Available examples — already analysed matches")
    for column, match_id in zip(st.columns(len(match_ids)), match_ids):
        column.button(
            f"Match {match_id}", key=f"example_match_{match_id}",
            on_click=on_select, args=(match_id,),
        )
