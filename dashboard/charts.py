"""Build Plotly figures from supplied observations without loading or displaying data."""

from collections import Counter
from html import escape
from math import ceil

import plotly.graph_objects as go


def _clock_label(seconds: int | float) -> str:
    # Round the display label to milliseconds while the chart keeps the original timestamp.
    whole_seconds, milliseconds = divmod(round(abs(seconds) * 1000), 1000)
    minutes, remainder = divmod(whole_seconds, 60)
    sign = "−" if seconds < 0 else ""
    label = f"{sign}{minutes}:{remainder:02d}"
    if milliseconds:
        label += f".{milliseconds:03d}".rstrip("0")
    return label


def build_progression_chart(
    snapshots: list[dict],
    metric: str,
    player_label: str,
    *,
    comparison_snapshots: list[dict] | None = None,
    comparison_label: str | None = None,
) -> go.Figure:
    """Plot recorded progression in game-time order without filling missing values."""
    # Use the same metric label on the axis and in the hover details.
    metric_labels = {"net_worth": "Net worth (gold)", "total_xp": "Total XP"}
    if metric not in metric_labels:
        raise ValueError(f"Unsupported progression metric: {metric}")

    # Assign styles by role so a player keeps the same style even when they
    # are the only one with observations for this metric.
    series = [(snapshots, player_label, "#38bdf8", "solid")]
    if comparison_snapshots is not None:
        series.append((comparison_snapshots, comparison_label or "Comparison player", "#f59e0b", "dash"))
    figure = go.Figure()
    tick_times = set()
    for records, label, color, dash in series:
        if not any(sample.get(metric) is not None for sample in records):
            continue

        # Plot each series at its own timestamps and keep null values as gaps.
        # Sorting a copy leaves the loader's records in their original order.
        ordered = sorted(records, key=lambda sample: sample["game_time_s"])
        times = [sample["game_time_s"] for sample in ordered]
        values = [sample.get(metric) for sample in ordered]
        # Dense replay snapshots need a clean line, rather than overlapping dots.
        # Keep isolated measurements visible even when both neighbours are missing.
        marker_sizes = [
            4 if len(ordered) <= 60 or (
                value is not None
                and (index == 0 or values[index - 1] is None)
                and (index == len(values) - 1 or values[index + 1] is None)
            ) else 0
            for index, value in enumerate(values)
        ]
        tick_times.update(times)
        figure.add_scatter(
            x=times, y=values,
            customdata=[_clock_label(time) for time in times], name=escape(label),
            mode="lines+markers", connectgaps=False,
            line={"color": color, "dash": dash, "shape": "linear", "width": 2},
            marker={"size": marker_sizes, "color": color, "line": {"width": 0}},
            hovertemplate=(escape(label) + "<br>Game time: %{customdata}"
                           + "<br>" + metric_labels[metric] + ": %{y:,.0f}<extra></extra>"),
        )

    # Use regular clock intervals instead of ticks tied to snapshot density.
    axis_times = sorted(tick_times)
    ticks = []
    if axis_times:
        span = axis_times[-1] - axis_times[0]
        step = next((step for step in (1, 5, 10, 15, 30, 60, 120, 300, 600, 1200)
                     if span / step <= 7), 1800)
        ticks = list(range(ceil(axis_times[0] / step) * step, int(axis_times[-1]) + 1, step))
        if not ticks:
            ticks = [axis_times[0]]
    figure.update_layout(
        xaxis={"title": "Game time", "tickmode": "array",
               "tickvals": ticks, "ticktext": [_clock_label(time) for time in ticks],
               "showgrid": False, "zeroline": False,
               "showspikes": True, "spikemode": "across", "spikesnap": "cursor",
               "spikethickness": 1, "spikecolor": "rgba(148,163,184,0.4)"},
        yaxis={"title": metric_labels[metric], "tickformat": ",.0f", "rangemode": "tozero",
               "gridcolor": "rgba(148,163,184,0.16)", "zeroline": False, "nticks": 5},
        margin={"l": 20, "r": 20, "t": 35, "b": 30},
        height=320, showlegend=comparison_snapshots is not None,
        hovermode="x unified", hoverlabel={"namelength": -1},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def build_damage_chart(series: list[tuple[str, dict[str, int]]]) -> go.Figure:
    """Compare hero damage by target on a common damage-points axis."""
    # Use one target order for both players, ranked by their combined damage.
    totals = Counter()
    for _, damage in series:
        totals.update(damage)
    targets = sorted(totals, key=lambda target: (-totals[target], target))
    target_labels = [escape(target) for target in targets]

    figure = go.Figure()
    for (label, damage), color in zip(series, ("#38bdf8", "#f59e0b")):
        figure.add_bar(
            name=escape(label), orientation="h", marker_color=color,
            y=target_labels, x=[damage.get(target) for target in targets],
            text=[(f"{damage[target]:,}" + (" (self)" if target == label else ""))
                  if target in damage else "" for target in targets],
            texttemplate="%{text}", textposition="outside", cliponaxis=False,
            customdata=["Self-damage" if target == label else "Damage to target" for target in targets],
            hovertemplate=("Attacker: " + escape(label)
                           + "<br>Target: %{y}<br>%{customdata}: %{x:,.0f}<extra></extra>"),
        )
    figure.update_layout(
        barmode="group", showlegend=True,
        legend={"title": {"text": "Attacker (dealt the damage)"}, "orientation": "h",
                "yanchor": "bottom", "y": 1.05, "x": 0},
        xaxis={"title": "Damage dealt (points)", "rangemode": "tozero", "tickformat": ",.0f",
               "showgrid": True},
        yaxis={"title": "Target receiving damage", "categoryorder": "array",
               "categoryarray": target_labels, "autorange": "reversed", "automargin": True},
        margin={"l": 20, "r": 80, "t": 80, "b": 60},
        height=max(360, 180 + 64 * len(targets)),
    )
    return figure


RADIANT_COLOR, DIRE_COLOR, EVEN_COLOR = "#22c55e", "#ef4444", "#94a3b8"
# A teamfight (3+ deaths) is highlighted when Radiant's lead moved at least this much across it.
SIGNIFICANT_SWING = 2000


def key_teamfights(fights: list[dict] | None) -> list[dict]:
    """Teamfights whose net-worth swing reached SIGNIFICANT_SWING, in time order."""
    return [fight for fight in fights or []
            if fight["kind"] == "teamfight" and fight["advantage_swing"] is not None
            and abs(fight["advantage_swing"]) >= SIGNIFICANT_SWING]


def fight_result(fight: dict) -> str:
    winner = {2: "Radiant won", 3: "Dire won"}.get(fight["winning_team"], "Even trade")
    # Deaths are written as kills per side: Radiant kills are Dire deaths.
    return f"{winner} {fight['dire_deaths']}–{fight['radiant_deaths']}"


def _fight_hover(fight: dict, deaths: list[dict], labels: dict[int, str]) -> str:
    fallen = [death for death in deaths if death["fight_number"] == fight["fight_number"]]
    names = {2: [], 3: []}
    for death in fallen:
        names[death["victim_team"]].append(escape(labels.get(death["victim_slot"], f"Slot {death['victim_slot']}")))
    swing = fight["advantage_swing"]
    lines = [
        f"<b>⚔ Teamfight {_clock_label(round(fight['start_s']))}–{_clock_label(round(fight['end_s']))}</b>",
        f"{fight_result(fight)} (kills Radiant–Dire)",
        f"Net worth swing: {swing:+,} gold toward {'Radiant' if swing > 0 else 'Dire'}",
        f"Hero damage: Radiant {fight['radiant_hero_damage']:,} · Dire {fight['dire_hero_damage']:,}",
        f"Heroes involved: {fight['participants']}",
    ]
    if names[2]:
        lines.append("Radiant lost: " + ", ".join(names[2]))
    if names[3]:
        lines.append("Dire lost: " + ", ".join(names[3]))
    return "<br>".join(lines)


def build_team_advantage_chart(records: list[dict], *, change: bool = False, fights: list[dict] | None = None,
                               deaths: list[dict] | None = None, labels: dict[int, str] | None = None) -> go.Figure:
    """Show the fixed Radiant-minus-Dire convention without bridging missing observations.

    The advantage is drawn as bars, green while Radiant leads and red while Dire leads; a missing
    observation is a gap with no bar. With fights, key teamfights (see key_teamfights) are shaded
    and marked ⚔ above the bars, with the fight's result and stats on hover.
    The two-minute change stays a line.
    """
    metric = "net_worth_advantage_change_120s" if change else "net_worth_advantage"
    times = [row["observation_time_s"] for row in records]
    values = [row[metric] for row in records]
    customdata = [[_clock_label(row["observation_time_s"]),
                   "—" if row["radiant_net_worth"] is None else f"{row['radiant_net_worth']:,}",
                   "—" if row["dire_net_worth"] is None else f"{row['dire_net_worth']:,}",
                   f"{row['radiant_players_observed']}/5 Radiant; {row['dire_players_observed']}/5 Dire"]
                  for row in records]
    hovertemplate = ("Game time: %{customdata[0]}<br>"
                     + ("Change over preceding 2 minutes" if change else "Radiant − Dire")
                     + ": %{y:+,.0f} gold<br>Radiant: %{customdata[1]}<br>Dire: %{customdata[2]}"
                     + "<br>Coverage: %{customdata[3]}<extra></extra>")
    if change:
        figure = go.Figure(go.Scatter(
            x=times, y=values, mode="lines+markers", connectgaps=False,
            line={"color": "#a78bfa", "width": 2}, marker={"size": 4},
            customdata=customdata, hovertemplate=hovertemplate,
        ))
    else:
        colors = [EVEN_COLOR if value is None or value == 0 else RADIANT_COLOR if value > 0 else DIRE_COLOR
                  for value in values]
        figure = go.Figure(go.Bar(
            x=times, y=values, marker={"color": colors, "line": {"width": 0}},
            customdata=customdata, hovertemplate=hovertemplate, name="Radiant − Dire",
        ))
        figure.update_layout(bargap=0)
        _add_key_teamfights(figure, values, key_teamfights(fights), deaths or [], labels or {})
    ticks = times[::max(1, (len(times) + 5) // 6)]
    if times and times[-1] not in ticks:
        ticks.append(times[-1])
    figure.update_layout(
        xaxis={"title": "Game time", "tickmode": "array", "tickvals": ticks,
               "ticktext": [_clock_label(time) for time in ticks]},
        yaxis={"title": "2-minute change (gold)" if change else "Radiant − Dire net worth (gold)",
               "tickformat": "+,.0f", "rangemode": "tozero", "zeroline": True, "zerolinewidth": 2},
        showlegend=False, height=320, margin={"l": 20, "r": 20, "t": 20, "b": 30},
    )
    return figure


def _add_key_teamfights(figure: go.Figure, values: list, fights: list[dict], deaths: list[dict],
                        labels: dict[int, str]) -> None:
    """Shade each key teamfight's window and put a ⚔ marker above the bars, coloured by the winner."""
    if not fights:
        return
    known = [value for value in values if value is not None]
    low, high = min(known + [0]), max(known + [0])
    span = (high - low) or 1000
    marker_y = high + 0.18 * span
    for fight in fights:
        fill = {2: "rgba(34,197,94,0.16)", 3: "rgba(239,68,68,0.16)"}.get(fight["winning_team"], "rgba(148,163,184,0.16)")
        figure.add_vrect(x0=fight["start_s"] - 10, x1=fight["end_s"] + 3, fillcolor=fill, line_width=0, layer="below")
    figure.add_scatter(
        x=[(fight["start_s"] + fight["end_s"]) / 2 for fight in fights], y=[marker_y] * len(fights),
        mode="markers+text", text=["⚔"] * len(fights), textposition="middle center", textfont={"size": 13},
        marker={"size": 24, "symbol": "circle", "line": {"width": 2, "color": "#0f172a"},
                "color": [{2: RADIANT_COLOR, 3: DIRE_COLOR}.get(fight["winning_team"], EVEN_COLOR) for fight in fights]},
        hovertext=[_fight_hover(fight, deaths, labels) for fight in fights], hoverinfo="text",
        name="Key teamfights", cliponaxis=False,
    )
    for fight in fights:
        swing = fight["advantage_swing"]
        figure.add_annotation(x=(fight["start_s"] + fight["end_s"]) / 2, y=marker_y, yshift=20, showarrow=False,
                              text=f"{swing / 1000:+.1f}k", font={"size": 11,
                                                                   "color": RADIANT_COLOR if swing > 0 else DIRE_COLOR})
    figure.update_yaxes(range=[low - 0.05 * span, high + 0.32 * span])
