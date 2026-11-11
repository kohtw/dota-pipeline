"""Build Plotly figures from supplied observations without loading or displaying data."""

from collections import Counter
from html import escape

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
        tick_times.update(times)
        figure.add_scatter(
            x=times, y=[sample.get(metric) for sample in ordered],
            customdata=[_clock_label(time) for time in times], name=escape(label),
            mode="lines+markers", connectgaps=False,
            line={"color": color, "dash": dash, "shape": "linear", "width": 2},
            marker={"size": 6},
            hovertemplate=(escape(label) + "<br>Game time: %{customdata}"
                           + "<br>" + metric_labels[metric] + ": %{y:,.0f}<extra></extra>"),
        )

    # Space out the clock labels and always label the last plotted observation.
    axis_times = sorted(tick_times)
    stride = max(1, (len(axis_times) + 5) // 6)
    ticks = axis_times[::stride]
    if axis_times and ticks[-1] != axis_times[-1]:
        ticks.append(axis_times[-1])
    figure.update_layout(
        xaxis={"title": "Game time", "tickmode": "array",
               "tickvals": ticks, "ticktext": [_clock_label(time) for time in ticks]},
        yaxis={"title": metric_labels[metric], "tickformat": ",.0f", "rangemode": "tozero"},
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        height=340, showlegend=comparison_snapshots is not None,
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
            hovertemplate="%{y}<br>Damage: %{x:,.0f}<extra>%{fullData.name}</extra>",
        )
    figure.update_layout(
        barmode="group", showlegend=len(series) > 1,
        xaxis={"title": "Damage points", "rangemode": "tozero"},
        yaxis={"categoryorder": "array", "categoryarray": target_labels, "autorange": "reversed"},
        margin={"l": 20, "r": 20, "t": 20, "b": 40},
        height=max(230, 90 + 42 * len(targets)),
    )
    return figure
