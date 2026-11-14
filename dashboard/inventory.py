"""Render final recorded inventory slots supplied by the application."""

import streamlit as st


def _render_slots(items: dict[int, str], slots: list[int], *, first_slot: int) -> None:
    """Keep positions visible even when the source has no record for a slot."""
    for offset in range(0, len(slots), 3):
        row = slots[offset:offset + 3]
        for column, slot in zip(st.columns(len(row)), row):
            with column.container(border=True, height="stretch"):
                st.caption(f"Slot {slot - first_slot + 1}")
                st.text(items.get(slot, "Not recorded"))


def render_inventory(label: str, records: list[dict] | None) -> None:
    """Display one player's loadout without treating unrecorded slots as empty."""
    st.text(label)
    if not records:
        st.info("Final inventory unavailable for this player.")
        return
    items = {record["inventory_slot"]: record["item_display_name"] for record in records}
    st.markdown("**Main inventory**")
    _render_slots(items, list(range(6)), first_slot=0)
    st.markdown("**Backpack**")
    _render_slots(items, [6, 7, 8], first_slot=6)
    for column, (title, slot) in zip(st.columns(2), (("Neutral item", 16), ("TP scroll", 15))):
        with column.container(border=True, height="stretch"):
            st.caption(title)
            st.text(items.get(slot, "Not recorded"))
    with st.expander("Stash"):
        _render_slots(items, list(range(9, 15)), first_slot=9)
