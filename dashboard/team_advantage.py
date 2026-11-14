"""Render relative team economy and send a selected review interval to player progression."""

import streamlit as st

from charts import build_team_advantage_chart
from components import format_duration


def _apply_interval(key: str, start: int, end: int) -> None:
    st.session_state[key] = (start, end)


def render_team_advantage(
    records: list[dict] | None, match_id: int, *, progression_key: str | None = None,
    progression_bounds: tuple[int, int] | None = None,
) -> None:
    if records is None:
        return
    st.subheader("Team net-worth advantage")
    if not records:
        st.caption("No team advantage timeline available for this match yet.")
        return
    st.caption(
        "Full match, Radiant minus Dire. Above zero: Radiant ahead. Below zero: Dire ahead. "
        "Zero: equal net worth."
    )
    complete = sum(row["net_worth_advantage"] is not None for row in records)
    st.caption(f"Complete observations: {complete:,} of {len(records):,}. "
               "Each team needs five fresh player snapshots, no older than five seconds. Missing coverage leaves gaps.")
    st.plotly_chart(build_team_advantage_chart(records), key="team_advantage_chart")
    if complete < len(records):
        with st.expander("Inspect incomplete coverage"):
            st.dataframe([
                {"Game time": format_duration(row["observation_time_s"]),
                 "Radiant players": row["radiant_players_observed"],
                 "Dire players": row["dire_players_observed"],
                 "Oldest snapshot age (s)": row["max_snapshot_age_s"]}
                for row in records if row["net_worth_advantage"] is None
            ], hide_index=True, width="stretch")
    st.markdown("**Economic swings over two minutes**")
    st.caption("Change in Radiant's net-worth advantage over the preceding two minutes. "
               "Positive: moved toward Radiant. Negative: moved toward Dire.")
    changes = [row for row in records if row["net_worth_advantage_change_120s"] is not None]
    if not changes:
        st.caption("No complete two-minute intervals available. Both endpoints need full team coverage.")
        return
    st.plotly_chart(build_team_advantage_chart(records, change=True), key="team_advantage_change_chart")
    if progression_key is None or progression_bounds is None:
        st.caption("Open a player to review a two-minute interval in their net-worth and XP charts.")
        return
    options = [row["observation_time_s"] for row in changes
               if progression_bounds[0] <= row["observation_time_s"] - 120
               and row["observation_time_s"] <= progression_bounds[1]]
    by_time = {row["observation_time_s"]: row for row in changes}
    def label(time: int) -> str:
        change = by_time[time]["net_worth_advantage_change_120s"]
        direction = "Toward Radiant" if change > 0 else "Toward Dire" if change < 0 else "No net shift"
        return f"{format_duration(time - 120)}–{format_duration(time)} · {direction}: {abs(change):,} gold"
    selected = st.selectbox("Two-minute interval to review", options, index=None,
                            placeholder="Choose an interval", format_func=label,
                            key=f"team_review_interval_{match_id}")
    if selected is not None:
        st.button("Review this interval", key="apply_team_review_interval", on_click=_apply_interval,
                  args=(progression_key, selected - 120, selected))
        st.caption("Updates the player net-worth and XP charts above. Team charts and final statistics stay unchanged.")
