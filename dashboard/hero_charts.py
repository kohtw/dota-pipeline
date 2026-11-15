"""Plotly figures for the Hero statistics page: the hero meta map, win rate by game length and
best/worst pairings (opponents or allies)."""

from html import escape

import plotly.graph_objects as go

BASE_COLOR = "#64748b"
HIGHLIGHT_COLOR = "#f59e0b"
UP_COLOR, DOWN_COLOR = "#22c55e", "#ef4444"
GRID_COLOR = "rgba(148,163,184,0.16)"


def _style(figure: go.Figure, *, height: int) -> go.Figure:
    figure.update_layout(height=height, margin={"l": 20, "r": 20, "t": 30, "b": 30}, showlegend=False,
                         paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                         hoverlabel={"namelength": -1})
    return figure


def build_meta_chart(heroes: list[dict], selected_id: int | None) -> go.Figure:
    """Every hero by pick rate and win rate, bubble size by picks; the selected hero is labelled.

    customdata[0] is the hero ID, so a clicked point can select that hero.
    """
    rows = [hero for hero in heroes if hero["pick_rate"] is not None and hero["win_rate"] is not None]
    largest = max((hero["picks"] for hero in rows), default=1)
    figure = go.Figure()
    for highlighted in (False, True):
        group = [hero for hero in rows if (hero["hero_id"] == selected_id) == highlighted]
        if not group:
            continue
        figure.add_scatter(
            x=[100 * hero["pick_rate"] for hero in group], y=[100 * hero["win_rate"] for hero in group],
            mode="markers+text" if highlighted else "markers",
            text=[escape(hero["hero_label"]) for hero in group] if highlighted else None,
            textposition="top center", textfont={"color": HIGHLIGHT_COLOR, "size": 13},
            marker={"size": [8 + 22 * (hero["picks"] / largest) ** 0.5 for hero in group],
                    "color": HIGHLIGHT_COLOR if highlighted else BASE_COLOR,
                    "opacity": 1 if highlighted else 0.65,
                    "line": {"width": 2 if highlighted else 0.5, "color": "#0f172a"}},
            customdata=[[hero["hero_id"], escape(hero["hero_label"]), hero["picks"], hero["wins"]] for hero in group],
            hovertemplate="<b>%{customdata[1]}</b><br>Win rate %{y:.1f}% · pick rate %{x:.1f}%"
                          "<br>%{customdata[3]:,} wins in %{customdata[2]:,} picks<extra></extra>",
        )
    figure.add_hline(y=50, line={"color": "rgba(148,163,184,0.6)", "dash": "dot", "width": 1})
    figure.update_layout(
        xaxis={"title": "Pick rate (% of matches)", "ticksuffix": "%", "gridcolor": GRID_COLOR, "zeroline": False},
        yaxis={"title": "Win rate", "ticksuffix": "%", "gridcolor": GRID_COLOR, "zeroline": False},
        clickmode="event+select",
    )
    return _style(figure, height=420)


def build_duration_chart(rows: list[dict], overall_win_rate: float | None) -> go.Figure:
    """Win rate in each match-length bucket, against the hero's overall win rate and a coin flip."""
    colors = [UP_COLOR if overall_win_rate is None or row["win_rate"] >= overall_win_rate else DOWN_COLOR
              for row in rows]
    figure = go.Figure(go.Bar(
        x=[row["duration_bucket"] for row in rows], y=[100 * row["win_rate"] for row in rows],
        marker={"color": colors}, text=[f"n={row['matches']:,}" for row in rows], textposition="outside",
        customdata=[[row["wins"], row["matches"]] for row in rows],
        hovertemplate="%{x}: %{y:.1f}% win rate<br>%{customdata[0]:,} wins in %{customdata[1]:,} matches<extra></extra>",
    ))
    figure.add_hline(y=50, line={"color": "rgba(148,163,184,0.6)", "dash": "dot", "width": 1})
    if overall_win_rate is not None:
        figure.add_hline(y=100 * overall_win_rate, line={"color": HIGHLIGHT_COLOR, "dash": "dash", "width": 1.5},
                         annotation_text=f"Overall {100 * overall_win_rate:.1f}%", annotation_position="top left",
                         annotation_font_color=HIGHLIGHT_COLOR)
    values = [100 * row["win_rate"] for row in rows] + [50]
    low, high = min(values), max(values)
    figure.update_layout(xaxis={"title": "Match length"},
                         yaxis={"title": "Win rate", "ticksuffix": "%", "gridcolor": GRID_COLOR,
                                "range": [max(0, low - 8), min(100, high + 8)]})
    return _style(figure, height=320)


def best_and_worst(rows: list[dict], *, minimum_matches: int, count: int) -> list[dict]:
    """The `count` highest and lowest win-rate differences among pairs with enough matches,
    worst first, without repeating a pair when there are few."""
    eligible = sorted((row for row in rows if row["matches"] >= minimum_matches),
                      key=lambda row: row["win_rate_difference_pp"])
    if len(eligible) <= 2 * count:
        return eligible
    return eligible[:count] + eligible[-count:]


def build_pair_chart(rows: list[dict], label_key: str) -> go.Figure:
    """Horizontal bars of win-rate difference in percentage points: green above the hero's
    overall win rate, red below, with each pair's sample size."""
    figure = go.Figure(go.Bar(
        y=[escape(row[label_key]) for row in rows], x=[row["win_rate_difference_pp"] for row in rows],
        orientation="h",
        marker={"color": [UP_COLOR if row["win_rate_difference_pp"] >= 0 else DOWN_COLOR for row in rows]},
        text=[f"{100 * row['win_rate']:.0f}% · n={row['matches']:,}" for row in rows], textposition="auto",
        customdata=[[100 * row["win_rate"], row["wins"], row["matches"]] for row in rows],
        hovertemplate="%{y}: %{x:+.1f} pp<br>%{customdata[0]:.1f}% win rate, "
                      "%{customdata[1]:,} wins in %{customdata[2]:,} matches<extra></extra>",
    ))
    figure.add_vline(x=0, line={"color": "rgba(148,163,184,0.6)", "width": 1})
    figure.update_layout(xaxis={"title": "Win rate vs the hero's overall (pp)", "ticksuffix": " pp",
                                "gridcolor": GRID_COLOR, "zeroline": False},
                         yaxis={"automargin": True})
    return _style(figure, height=max(260, 60 + 26 * len(rows)))
