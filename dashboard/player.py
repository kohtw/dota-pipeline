"""Render an individual participant's view from records supplied by the app.

The application owns data loading and navigation. This module owns presentation
of the selected participant, keeping those responsibilities separate.
"""

from collections import Counter
from math import ceil, floor, fsum

import streamlit as st

from charts import build_damage_chart, build_progression_chart
from components import MISSING_VALUE, format_duration, format_number, format_percentage
from inventory import render_inventory
from team_advantage import render_team_advantage


TEAM_LABELS = {2: "Radiant", 3: "Dire"}


def _format_game_time(seconds: int) -> str:
    """Label game-relative progression time, including samples before the horn."""
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
        "Full-match recorded damage to heroes, including self-damage such as health sacrificed "
        "by abilities. Creep and tower damage are excluded."
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
                "Attacker (dealt damage)": label,
                "Target (received damage)": target,
                "Damage type": "Self-damage" if target_slot == slot else "Damage to another hero",
                "Source": "Auto attack" if source == "dota_unknown" else source.replace("_", " ").title(),
                "Damage": format_number(row["total_damage"]),
                "Recorded hits": format_number(row["hits"]),
                "Share of attacker's team damage": format_percentage(share),
            })

        # One player's source shares all use the same team total as their denominator.
        summaries.append({
            "Attacker (dealt damage)": label,
            "Total damage to heroes (includes self)": format_number(sum(target_totals.values())),
            "Share of own team's hero damage": format_percentage(None if None in shares else fsum(shares)),
        })
        chart_series.append((label, target_totals))

    if chart_series:
        st.table(summaries, hide_index=True)
        st.caption(
            "Team share = this player's recorded hero damage ÷ their own team's recorded hero damage."
        )
        st.markdown("**Who did each player damage?**")
        st.caption(
            "Rows are targets receiving damage; colours identify the selected attackers. "
            "Each bar shows damage dealt by that attacker to that target. Other heroes appear because "
            "they received damage. A bar on the attacker's own row is self-damage; "
            "an absent bar means no damage recorded for that pair."
        )
        st.plotly_chart(build_damage_chart(chart_series), key="hero_damage_by_target")
        st.markdown("**Attack and ability breakdown**")
        st.caption("Each row identifies who dealt damage, who received it, and the recorded attack or ability.")
        st.table(source_rows, hide_index=True)


def render_player_page(
    match: dict,
    player: dict,
    snapshots: list[dict] | None = None,
    *,
    comparison_player: dict | None = None,
    comparison_snapshots: list[dict] | None = None,
    damage_by_slot: dict[int, list[dict]] | None = None,
    roster: list[dict] | None = None,
    team_advantage: list[dict] | None = None,
    inventory_by_slot: dict[int, list[dict] | None] | None = None,
) -> None:
    """Display the player page from match records supplied by the app.

    The optional comparison inputs keep the second player's data separate. A
    comparison player can be present even if either progression series is missing.
    """
    # Hero, team and slot identify a participant within the match without account-name data.
    st.title(player.get("hero_label") or player.get("hero") or "Player analysis")

    # Team and match context distinguish the participant without assuming a
    # gameplay role or requiring a Steam account ID.
    team = TEAM_LABELS.get(player.get("team"), "Unavailable")
    match_id = match.get("match_id")
    match_label = str(match_id) if match_id is not None else MISSING_VALUE
    st.caption(f"{team} · Match {match_label}")

    # Keep the full bounds for the team review control, independent of the selected range.
    observations = snapshots or []
    comparison_observations = comparison_snapshots or []
    timed_records = list(observations)
    if comparison_player is not None:
        timed_records.extend(comparison_observations)
    timestamps = [record["game_time_s"] for record in timed_records]
    start = floor(min(0, min(timestamps, default=0)))
    end = ceil(max(0, match.get("duration_s") or 0, max(timestamps, default=0)))
    progression_bounds = (start, end) if timestamps and end > start else None

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
        groups = (
            ("Combat", combat_stats),
            ("Farming", [("Last hits", "last_hits"), ("Denies", "denies")]),
            ("Economy", [("Net worth", "net_worth"), ("Gold", "gold")]),
        )
        for column, (title, metrics) in zip(st.columns(3), groups):
            with column.container(border=True, height="stretch"):
                st.markdown(f"### {title}")
                for index, (label, field) in enumerate(metrics):
                    if index:
                        st.divider()
                    st.metric(label, format_number(player.get(field)))
        with st.container(border=True):
            st.metric("Kill participation", _kill_participation_detail(player))

    st.subheader("Final inventory")
    st.caption("Last recorded loadout. Not recorded means no item record is available for that slot.")
    inventories = inventory_by_slot or {}
    if comparison_player is not None:
        for column, label, participant in zip(st.columns(2), comparison_labels, participants):
            with column:
                render_inventory(label, inventories.get(participant["slot"]))
    else:
        render_inventory(f"{player.get('hero_label') or player.get('hero') or 'Selected player'} · {team}",
                         inventories.get(player["slot"]))

    # The damage mart contains full-match totals with no event timestamps to filter.
    if damage_by_slot is not None:
        if comparison_player is not None:
            selected_players = [(player["slot"], primary_label), (comparison_player["slot"], secondary_label)]
        else:
            hero = player.get("hero_label") or player.get("hero") or "Selected player"
            selected_players = [(player["slot"], f"{hero} · {team}")]
        _render_hero_damage(selected_players, damage_by_slot, roster or [])

    # Keep whole-second slider bounds while including fractional observations.
    if timestamps and end > start:
        start, end = st.slider(
            "Progression time range (seconds)",
            min_value=start,
            max_value=end,
            value=(start, end),
            key=f"progression_range_{match_id}_{player['slot']}",
        )
        st.caption("Negative times are pregame before the opening horn; 0:00 marks the start of game time.")
        st.caption(
            f"Showing {_format_game_time(start)} to {_format_game_time(end)} for net worth and XP. "
            "Final statistics and inventory stay at match end; damage and kill participation use full-match totals."
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
        st.caption(f"{description} Hover for recorded values. Gaps indicate missing measurements.")
        figure = build_progression_chart(
            chart_series[0], metric, progression_players[0][0],
            comparison_snapshots=chart_series[1] if comparison_player is not None else None,
            comparison_label=secondary_label if comparison_player is not None else None,
        )
        st.plotly_chart(figure, key=f"{metric}_{match['match_id']}_{player['slot']}")

    st.divider()
    render_team_advantage(
        team_advantage, match_id,
        progression_key=f"progression_range_{match_id}_{player['slot']}" if progression_bounds else None,
        progression_bounds=progression_bounds,
    )
