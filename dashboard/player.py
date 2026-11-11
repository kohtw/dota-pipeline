"""Render an individual participant's view from records supplied by the app.

The application owns data loading and navigation. This module owns presentation
of the selected participant, keeping those responsibilities separate.
"""

from collections import Counter
from math import ceil, floor, fsum

import streamlit as st

from charts import build_damage_chart, build_progression_chart
from components import MISSING_VALUE, format_duration, format_items, format_number, format_percentage


TEAM_LABELS = {2: "Radiant", 3: "Dire"}


def _format_game_time(seconds: int) -> str:
    """Label game-relative time, including purchases made before the horn."""
    clock = format_duration(abs(seconds))
    return f"−{clock}" if seconds < 0 else clock


def _kill_participation_detail(player: dict) -> str:
    """Show the mart's percentage alongside its event-based kill counts."""
    percentage = format_percentage(player.get("kill_participation"))
    team_kills = player.get("team_kills")
    involvements = player.get("kill_involvements")
    if team_kills == 0:
        return f"{MISSING_VALUE} · Team had zero kills"
    if percentage == MISSING_VALUE or involvements is None or team_kills is None:
        return f"{MISSING_VALUE} · Unavailable"
    kill_label = "kill" if team_kills == 1 else "kills"
    return f"{percentage} · {involvements} of {team_kills} team {kill_label}"


def _render_hero_damage(
    selected_players: list[tuple[int, str]],
    damage_by_slot: dict[int, list[dict]],
    roster: list[dict],
) -> None:
    """Display full-match hero damage summaries, target bars and source rows."""
    st.subheader("Hero damage dealt")
    st.caption(
        "Full match. Team share uses all hero damage dealt by the attacker's team. "
        "Creep and tower damage are excluded."
    )

    target_labels = {}
    for participant in roster:
        hero = participant.get("hero_label") or participant.get("hero") or "Unknown hero"
        team = TEAM_LABELS.get(participant["team"], "Unknown team")
        target_labels[participant["slot"]] = f"{hero} · {team}"
    label_counts = Counter(target_labels.values())
    for slot, label in target_labels.items():
        if label_counts[label] > 1:
            target_labels[slot] = f"{label} · Slot {slot}"

    summaries = []
    chart_series = []
    source_rows = []
    for slot, label in selected_players:
        rows = damage_by_slot.get(slot, [])
        if not rows:
            st.info(f"No hero damage recorded for {label}.")
            continue

        # The bars combine damage sources per target. The detail table keeps each source separate.
        target_totals = Counter()
        shares = []
        for row in rows:
            target_slot = row["target_slot"]
            target_team = TEAM_LABELS.get(row["target_team"], "Unknown team")
            target = target_labels.get(target_slot, f"Player slot {target_slot} · {target_team}")
            target_totals[target] += row["total_damage"]
            share = row["share_of_team_damage"]
            shares.append(share)
            source = row["source"]
            source_rows.append({
                "Attacker": label,
                "Target": target,
                "Source": "Auto attack" if source == "dota_unknown" else source.replace("_", " ").title(),
                "Damage": format_number(row["total_damage"]),
                "Recorded hits": format_number(row["hits"]),
                "Share of team hero damage": format_percentage(share),
            })

        # One player's source shares all use the same team total as their denominator.
        summaries.append({
            "Player": label,
            "Hero damage dealt": format_number(sum(target_totals.values())),
            "Share of team hero damage": format_percentage(None if None in shares else fsum(shares)),
        })
        chart_series.append((label, target_totals))

    if chart_series:
        st.table(summaries, hide_index=True)
        st.plotly_chart(build_damage_chart(chart_series), key="hero_damage_by_target")
        st.caption("Damage by source")
        st.table(source_rows, hide_index=True)


def _render_purchase_history(purchases: list[dict] | None, start: int, end: int) -> None:
    """Display one participant's purchase events within the selected interval."""
    # A missing feed and an empty interval have different meanings. Handle each
    # within this participant's view so the other player's history can still render.
    if purchases is None:
        st.info("Purchase history is unavailable for this participant.")
        return

    # Sort individual events without grouping by item so repeat purchases stay visible.
    visible_purchases = [
        event for event in sorted(purchases, key=lambda event: event["game_time_s"])
        if start <= event["game_time_s"] <= end
    ]
    if not visible_purchases:
        st.info("No purchases in this interval.")
        return

    st.table(
        [{"Game time": _format_game_time(event["game_time_s"]), "Purchased item": event["item_name"]}
         for event in visible_purchases],
        hide_index=True,
    )


def render_player_page(
    match: dict,
    player: dict,
    snapshots: list[dict] | None = None,
    purchases: list[dict] | None = None,
    *,
    comparison_player: dict | None = None,
    comparison_snapshots: list[dict] | None = None,
    comparison_purchases: list[dict] | None = None,
    damage_by_slot: dict[int, list[dict]] | None = None,
    roster: list[dict] | None = None,
) -> None:
    """Display the player page from match records supplied by the app.

    The optional comparison inputs keep the second player's data separate. A
    comparison player can be present even if either of their histories is missing.
    """
    # The hero remains identifiable even when the replay does not provide a
    # player name. Plain text preserves names containing Markdown punctuation.
    st.title(player.get("hero_label") or player.get("hero") or "Player analysis")
    if player.get("player_name"):
        st.text(player["player_name"])

    # Team and match context distinguish the participant without assuming a
    # gameplay role or requiring a Steam account ID.
    team = TEAM_LABELS.get(player.get("team"), "Unavailable")
    match_id = match.get("match_id")
    match_label = str(match_id) if match_id is not None else MISSING_VALUE
    st.caption(f"{team} · Match {match_label}")

    # Label these as final values so they cannot be confused with measurements
    # at an intermediate game time. Reuse scoreboard formatting to preserve the
    # same distinction between a measured zero and an unavailable statistic.
    st.subheader("Final match statistics")
    st.caption("Values at match end. — means unavailable; Gold is unspent gold.")
    combat_stats = [("Kills", "kills"), ("Deaths", "deaths"), ("Assists", "assists")]
    economy_stats = [
        ("Last hits", "last_hits"), ("Denies", "denies"),
        ("Net worth", "net_worth"), ("Gold", "gold"),
    ]

    if comparison_player is not None:
        # Use the same labels for stats and inventories so each value has an
        # unambiguous owner, including when both players picked the same hero.
        participants = (player, comparison_player)
        comparison_labels = []
        for participant in participants:
            hero = participant.get("hero_label") or participant.get("hero") or "Unknown hero"
            side = TEAM_LABELS.get(participant.get("team"), "Unavailable")
            comparison_labels.append(f"{hero} · {side}")
        if comparison_labels[0] == comparison_labels[1]:
            comparison_labels = [
                f"{label} · Slot {participant['slot']}"
                for label, participant in zip(comparison_labels, participants)
            ]

        # Align the two players on each metric and reuse the scoreboard's value
        # formatting so unavailable values are distinct from measured zeros.
        primary_label, secondary_label = comparison_labels
        stat_rows = [
            {
                "Metric": label,
                primary_label: format_number(player.get(field)),
                secondary_label: format_number(comparison_player.get(field)),
            }
            for label, field in combat_stats + economy_stats
        ]
        stat_rows.append({
            "Metric": "Kill participation",
            primary_label: _kill_participation_detail(player),
            secondary_label: _kill_participation_detail(comparison_player),
        })
        st.table(stat_rows, hide_index=True)
    else:
        # Separate combat counts from farming and economy values to keep each
        # row readable without crowding all seven metric cards into one strip.
        for group in (combat_stats, economy_stats):
            columns = st.columns(len(group))
            for column, (label, field) in zip(columns, group):
                column.metric(label, format_number(player.get(field)))
        st.metric("Kill participation", _kill_participation_detail(player))

    # Display the supplied final main-inventory snapshot independently of
    # purchase history: buying an item does not establish where it ended up.
    # Plain text preserves item names without interpreting them as Markdown.
    st.subheader("Active items at match end")
    st.caption(
        "Main inventory only, including passive items. Backpack, stash, "
        "neutral-slot items, and consumed upgrades are excluded."
    )
    if comparison_player is not None:
        for column, label, participant in zip(st.columns(2), comparison_labels, participants):
            column.text(label)
            column.text(format_items(participant.get("active_items")))
    else:
        st.text(format_items(player.get("active_items")))

    # The damage mart contains full-match totals with no event timestamps to filter.
    if damage_by_slot is not None:
        if comparison_player is not None:
            selected_players = [(player["slot"], primary_label), (comparison_player["slot"], secondary_label)]
        else:
            hero = player.get("hero_label") or player.get("hero") or "Selected player"
            selected_players = [(player["slot"], f"{hero} · {team}")]
        _render_hero_damage(selected_players, damage_by_slot, roster or [])

    # Base the shared clock range on both participants' available records.
    # Pregame purchases can extend the start and purchase-only data still
    # warrants a slider, even when neither player has progression samples.
    observations = snapshots or []
    comparison_observations = comparison_snapshots or []
    timed_records = [*observations, *(purchases or [])]
    if comparison_player is not None:
        timed_records.extend(comparison_observations)
        timed_records.extend(comparison_purchases or [])

    timestamps = [record["game_time_s"] for record in timed_records]
    # Keep whole-second slider bounds while including fractional observations.
    start = floor(min(0, min(timestamps, default=0)))
    end = ceil(max(0, match.get("duration_s") or 0, max(timestamps, default=0)))
    if timestamps and end > start:
        start, end = st.slider(
            "Game time range (seconds)",
            min_value=start,
            max_value=end,
            value=(start, end),
            key=f"progression_range_{match_id}_{player['slot']}",
        )
        st.caption(
            f"Showing {_format_game_time(start)} to {_format_game_time(end)}. "
            "Final match statistics show the full match."
        )

    # Use the comparison labels from the stats table in both chart legends.
    # Keep each participant's observations separate as the range is applied.
    label = player.get("hero_label") or player.get("hero") or "Selected player"
    progression_players = [(label, observations)]
    if comparison_player is not None:
        progression_players = [
            (primary_label, observations),
            (secondary_label, comparison_observations),
        ]
    visible_by_player = [
        [sample for sample in records if start <= sample["game_time_s"] <= end]
        for _, records in progression_players
    ]

    # Evaluate availability for each player and metric independently so a
    # missing series leaves the other player's line visible.
    progression_sections = [
        ("net_worth", "Net worth over time", "Net worth is shown in gold value."),
        ("total_xp", "Total XP over time", "Total XP is experience accumulated since game start."),
    ]
    for metric, heading, description in progression_sections:
        st.subheader(heading)
        chart_series = []
        for (player_label, records), visible in zip(progression_players, visible_by_player):
            if not any(sample.get(metric) is not None for sample in records):
                owner = player_label if comparison_player is not None else "this participant"
                st.info(f"{heading} is unavailable for {owner}.")
                chart_series.append([])
            elif not any(sample.get(metric) is not None for sample in visible):
                owner = f" for {player_label}" if comparison_player is not None else ""
                st.info(f"No measurements{owner} in this interval. Widen the game time range to see more.")
                chart_series.append([])
            else:
                chart_series.append(visible)
        if not any(chart_series):
            continue
        st.caption(f"{description} Markers show observations. Gaps indicate missing measurements.")
        figure = build_progression_chart(
            chart_series[0], metric, progression_players[0][0],
            comparison_snapshots=chart_series[1] if comparison_player is not None else None,
            comparison_label=secondary_label if comparison_player is not None else None,
        )
        st.plotly_chart(figure, key=f"{metric}_{match['match_id']}_{player['slot']}")

    # Purchase history uses the chart's time range and the stats table's player order.
    st.subheader("Purchase history")
    st.caption("Purchase events are listed separately from the active inventory at match end.")
    if comparison_player is None:
        _render_purchase_history(purchases, start, end)
    else:
        for column, label, history in zip(st.columns(2), comparison_labels, (purchases, comparison_purchases)):
            with column:
                st.text(label)
                _render_purchase_history(history, start, end)
