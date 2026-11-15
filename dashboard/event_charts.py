"""Build Plotly figures from event-level replay records: maps, the map replay, event timelines,
damage by minute, item builds and the replay-wide insight charts.

Coordinates are Dota world units: Radiant's fountain is bottom-left, Dire's top-right. Maps are drawn
over the in-game minimap image in assets/.
"""

from base64 import b64encode
from collections import defaultdict
from functools import cache
from html import escape
from pathlib import Path

import plotly.graph_objects as go

from charts import _clock_label


TEAM_LABELS = {2: "Radiant", 3: "Dire"}
TEAM_COLORS = {2: "#22c55e", 3: "#ef4444"}
TEAM_FILLS = {2: "rgba(34,197,94,0.13)", 3: "rgba(239,68,68,0.13)", None: "rgba(148,163,184,0.13)"}
# In-game player colours by slot, so heroes look the way players know them.
SLOT_COLORS = ("#3375FF", "#66FFBF", "#BF00BF", "#F3F00B", "#FF6B00",
               "#FE86C2", "#A1B447", "#65D9F7", "#008321", "#A46900")
KILLER_KIND_LABELS = {
    "building": "Tower or building", "lane creep": "Lane creeps", "neutral creep": "Neutral creeps",
    "roshan": "Roshan", "fountain": "Fountain", "summon": "Summon or ward", "unknown": "Unknown", "hero": "Hero",
}
TELEPORT = 3000  # world units in five seconds that no hero walks: a teleport, blink chain or respawn
GRID_COLOR = "rgba(148,163,184,0.16)"


def clock(seconds: float) -> str:
    """Whole-second game clock, with a minus sign before the horn."""
    return _clock_label(round(seconds))


def _time_axis(times: list[float]) -> dict:
    """Clock-labelled ticks on a seconds axis."""
    if not times:
        return {"title": "Game time"}
    low, high = min(times), max(times)
    step = next((step for step in (60, 120, 300, 600) if (high - low) / step <= 10), 900)
    first = int(low // step) * step
    ticks = list(range(first, int(high) + step, step))
    return {"title": "Game time", "tickmode": "array", "tickvals": ticks,
            "ticktext": [clock(tick) for tick in ticks], "showgrid": True, "gridcolor": GRID_COLOR,
            "zeroline": False}


def _style(figure: go.Figure, *, height: int, legend: bool = True) -> go.Figure:
    figure.update_layout(
        height=height, margin={"l": 20, "r": 20, "t": 40, "b": 30}, showlegend=legend,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        hoverlabel={"namelength": -1}, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


# Map views

# The 7.41 minimap (Valve game art, via Liquipedia), placed in world units. The image's edges were fitted
# to the parsed replays: the pre-horn spawn at each fountain and the three lanes' busiest laning spots.
MAP_IMAGE = Path(__file__).parent / "assets" / "minimap_7.41.jpg"
MAP_IMAGE_EXTENT = (-9255, 10508, -9797, 9966)  # world x at the left and right edges, y at the bottom and top
MAP_VIEW = ((-8100, 9350), (-8650, 8800))  # the playable area inside the image's dark frame


@cache
def _map_source() -> str | None:
    try:
        return "data:image/jpeg;base64," + b64encode(MAP_IMAGE.read_bytes()).decode()
    except OSError:
        return None


def map_figure(*, height: int = 620, zoom: tuple[float, float, float] | None = None) -> go.Figure:
    """An empty map: the in-game minimap under world-unit axes.

    zoom = (center_x, center_y, half_width) frames part of the map instead of all of it.
    """
    figure = go.Figure()
    source = _map_source()
    if source:
        left, right, bottom, top = MAP_IMAGE_EXTENT
        figure.add_layout_image(source=source, xref="x", yref="y", x=left, y=top, sizex=right - left,
                                sizey=top - bottom, sizing="stretch", layer="below", opacity=1)
    if zoom:
        center_x, center_y, half = zoom
        x_range, y_range = [center_x - half, center_x + half], [center_y - half, center_y + half]
    else:
        x_range, y_range = MAP_VIEW
    axis = {"visible": False, "fixedrange": False, "showgrid": False, "zeroline": False}
    figure.update_layout(
        xaxis={**axis, "range": list(x_range), "constrain": "domain"},
        yaxis={**axis, "range": list(y_range), "scaleanchor": "x", "scaleratio": 1, "constrain": "domain"},
    )
    return _style(figure, height=height)


def _killer_label(death: dict, labels: dict[int, str]) -> str:
    credited = death["credited_killer_slot"]
    if credited is not None:
        return labels.get(credited, f"Slot {credited}")
    return KILLER_KIND_LABELS.get(death["killer_kind"], "Unknown")


def _assist_label(death: dict, labels: dict[int, str]) -> str:
    return ", ".join(labels.get(slot, f"Slot {slot}") for slot in death["assists"]) or "none"


def add_death_markers(figure: go.Figure, deaths: list[dict], labels: dict[int, str], *, size: int = 11) -> None:
    """One marker per death at the corpse, coloured by the victim's team."""
    for team in (2, 3):
        rows = [death for death in deaths if death["victim_team"] == team and death["x"] is not None]
        if not rows:
            continue
        figure.add_scatter(
            x=[row["x"] for row in rows], y=[row["y"] for row in rows], mode="markers",
            name=f"{TEAM_LABELS[team]} hero died",
            marker={"symbol": "x", "size": size, "color": TEAM_COLORS[team],
                    "line": {"width": 1, "color": "rgba(15,23,42,0.6)"}},
            customdata=[[clock(row["game_time_s"]), labels.get(row["victim_slot"], "?"),
                         _killer_label(row, labels), _assist_label(row, labels)] for row in rows],
            hovertemplate=("%{customdata[0]} · %{customdata[1]} died<br>Killed by %{customdata[2]}"
                           "<br>Assists: %{customdata[3]}<extra></extra>"),
        )


def build_replay_map(positions: list[dict], deaths: list[dict],
                     labels: dict[int, str], *, step: int = 10) -> go.Figure:
    """An animated map of all ten heroes. Play it, or drag the slider to any moment.

    Dead heroes stay at their corpse, drawn faded with a cross. Deaths of the last 20 seconds are
    marked where they happened.
    """
    figure = map_figure(height=680)
    by_time = defaultdict(dict)
    for row in positions:
        if row["time_s"] % step == 0:
            by_time[row["time_s"]][row["player_slot"]] = row
    times = sorted(by_time)
    if not times:
        return figure

    def hero_trace(time: int) -> go.Scatter:
        rows = [by_time[time][slot] for slot in sorted(by_time[time])]
        return go.Scatter(
            x=[row["x"] for row in rows], y=[row["y"] for row in rows], ids=[str(row["player_slot"]) for row in rows],
            mode="markers+text", text=[labels.get(row["player_slot"], "") for row in rows],
            textposition="top center", textfont={"size": 11},
            marker={"size": [15 if row["is_alive"] else 11 for row in rows],
                    "color": [SLOT_COLORS[row["player_slot"]] for row in rows],
                    "opacity": [1 if row["is_alive"] else 0.35 for row in rows],
                    "symbol": ["circle" if row["is_alive"] else "x" for row in rows],
                    "line": {"width": 2, "color": [TEAM_COLORS[2] if row["player_slot"] < 5 else TEAM_COLORS[3]
                                                   for row in rows]}},
            customdata=[["alive" if row["is_alive"] else "dead"] for row in rows],
            hovertemplate="%{text} (%{customdata[0]})<extra></extra>", name="Heroes", showlegend=False,
        )

    def death_trace(time: int) -> go.Scatter:
        rows = [death for death in deaths if death["x"] is not None and time - 20 < death["game_time_s"] <= time]
        return go.Scatter(
            x=[row["x"] for row in rows], y=[row["y"] for row in rows], mode="markers",
            marker={"symbol": "x-thin", "size": 26, "line": {"width": 3, "color": [TEAM_COLORS[row["victim_team"]]
                                                                                  for row in rows]}},
            customdata=[[clock(row["game_time_s"]), labels.get(row["victim_slot"], "?"), _killer_label(row, labels)]
                        for row in rows],
            hovertemplate="%{customdata[0]} · %{customdata[1]} killed by %{customdata[2]}<extra></extra>",
            name="Recent deaths", showlegend=False,
        )

    start = next((time for time in times if time >= 0), times[0])
    figure.add_trace(hero_trace(start))
    figure.add_trace(death_trace(start))
    hero_index = len(figure.data) - 2
    figure.frames = [go.Frame(name=str(time), data=[hero_trace(time), death_trace(time)],
                              traces=[hero_index, hero_index + 1]) for time in times]
    frame_args = {"frame": {"duration": 250, "redraw": False}, "mode": "immediate",
                  "transition": {"duration": 200, "easing": "linear"}}
    figure.update_layout(
        updatemenus=[{"type": "buttons", "direction": "left", "x": 0, "y": -0.02, "xanchor": "left", "yanchor": "top",
                      "pad": {"r": 10, "t": 10}, "showactive": False,
                      "buttons": [{"label": "▶ Play", "method": "animate", "args": [None, {**frame_args, "fromcurrent": True}]},
                                  {"label": "❚❚ Pause", "method": "animate",
                                   "args": [[None], {"frame": {"duration": 0, "redraw": False}, "mode": "immediate"}]}]}],
        sliders=[{"active": times.index(start), "x": 0.16, "len": 0.84, "y": -0.02, "yanchor": "top",
                  "currentvalue": {"prefix": "Game time ", "font": {"size": 13}},
                  "pad": {"t": 10}, "transition": {"duration": 0},
                  "steps": [{"label": clock(time), "method": "animate",
                             "args": [[str(time)], {"frame": {"duration": 0, "redraw": False}, "mode": "immediate",
                                                    "transition": {"duration": 0}}]} for time in times]}],
        margin={"l": 10, "r": 10, "t": 30, "b": 90}, showlegend=False,
    )
    return figure


def build_fight_map(fight: dict, positions: list[dict], deaths: list[dict],
                    labels: dict[int, str], slots: set[int]) -> go.Figure:
    """Each fighter's path through the fight window (five-second steps) and where heroes fell."""
    start, end = fight["start_s"] - 10, fight["end_s"] + 3
    paths = defaultdict(list)
    for row in positions:
        if row["player_slot"] in slots and start <= row["time_s"] <= end:
            paths[row["player_slot"]].append(row)
    # Frame the deaths; paths that leave the frame (a teleport, a respawn) are cut off at its edge.
    spots = [(death["x"], death["y"]) for death in deaths if death["x"] is not None]
    if spots:
        xs, ys = [p[0] for p in spots], [p[1] for p in spots]
        half = max(2200.0, (max(xs) - min(xs)) / 2 + 1500, (max(ys) - min(ys)) / 2 + 1500)
        zoom = ((max(xs) + min(xs)) / 2, (max(ys) + min(ys)) / 2, half)
    else:
        zoom = None
    figure = map_figure(height=560, zoom=zoom)
    for slot in sorted(paths):
        rows = sorted(paths[slot], key=lambda row: row["time_s"])
        x, y, times, symbols = [], [], [], []
        for index, row in enumerate(rows):
            previous = rows[index - 1] if index else None
            if previous and abs(row["x"] - previous["x"]) + abs(row["y"] - previous["y"]) > TELEPORT:
                x.append(None)  # break the line rather than draw a teleport or respawn as a walk
                y.append(None)
                times.append("")
                symbols.append("circle")
            x.append(row["x"])
            y.append(row["y"])
            times.append(clock(row["time_s"]))
            symbols.append("circle" if row["is_alive"] else "x")
        figure.add_scatter(
            x=x, y=y, mode="lines+markers", name=labels.get(slot, f"Slot {slot}"), connectgaps=False,
            line={"color": SLOT_COLORS[slot], "width": 2},
            marker={"size": [5] * (len(x) - 1) + [12], "color": SLOT_COLORS[slot], "symbol": symbols,
                    "line": {"width": 1, "color": TEAM_COLORS[2] if slot < 5 else TEAM_COLORS[3]}},
            customdata=times, hovertemplate=escape(labels.get(slot, "")) + " at %{customdata}<extra></extra>",
        )
    add_death_markers(figure, deaths, labels, size=16)
    figure.update_layout(legend={"orientation": "v", "x": 1.02, "y": 1, "yanchor": "top"})
    return figure


# Timelines

def _row_label(slot: int, labels: dict[int, str]) -> str:
    return f"{labels.get(slot, f'Slot {slot}')}"


def build_event_timeline(slots: list[int], labels: dict[int, str], deaths: list[dict], fights: list[dict],
                         purchases: list[dict] | None = None, *, show_assists: bool = True) -> go.Figure:
    """Every kill, death and assist on one row per hero, with fights shaded by the side that won them."""
    order = [slot for slot in slots if slot < 5] + [slot for slot in slots if slot >= 5]
    rows = [_row_label(slot, labels) for slot in order]
    figure = go.Figure()
    for fight in fights:
        if fight["kind"] == "pick-off":
            continue
        figure.add_vrect(x0=fight["start_s"] - 3, x1=fight["end_s"] + 3, line_width=0, layer="below",
                         fillcolor=TEAM_FILLS[fight["winning_team"]])
    kills = [death for death in deaths if death["credited_killer_slot"] in labels]
    figure.add_scatter(
        x=[death["game_time_s"] for death in kills],
        y=[_row_label(death["credited_killer_slot"], labels) for death in kills], mode="markers", name="Kill",
        marker={"symbol": "triangle-up", "size": 11, "color": [SLOT_COLORS[d["credited_killer_slot"]] for d in kills],
                "line": {"width": 1, "color": "rgba(15,23,42,0.7)"}},
        customdata=[[clock(d["game_time_s"]), labels.get(d["victim_slot"], "?"), _assist_label(d, labels)] for d in kills],
        hovertemplate="%{customdata[0]} · %{y} killed %{customdata[1]}<br>Assists: %{customdata[2]}<extra></extra>",
    )
    if show_assists:
        assists = [(death, slot) for death in deaths for slot in death["assists"] if slot in labels]
        figure.add_scatter(
            x=[death["game_time_s"] for death, _ in assists], y=[_row_label(slot, labels) for _, slot in assists],
            mode="markers", name="Assist", marker={"symbol": "circle-open", "size": 7, "color": "rgba(148,163,184,0.9)"},
            customdata=[[clock(d["game_time_s"]), labels.get(d["victim_slot"], "?")] for d, _ in assists],
            hovertemplate="%{customdata[0]} · %{y} assisted on %{customdata[1]}<extra></extra>",
        )
    victims = [death for death in deaths if death["victim_slot"] in labels]
    figure.add_scatter(
        x=[death["game_time_s"] for death in victims], y=[_row_label(d["victim_slot"], labels) for d in victims],
        mode="markers", name="Death", marker={"symbol": "x", "size": 10, "color": "#94a3b8",
                                              "line": {"width": 1, "color": "#475569"}},
        customdata=[[clock(d["game_time_s"]), _killer_label(d, labels),
                     "—" if d["dead_s"] is None else f"{d['dead_s']:.0f} s"] for d in victims],
        hovertemplate="%{customdata[0]} · %{y} died to %{customdata[1]}<br>Dead for %{customdata[2]}<extra></extra>",
    )
    if purchases:
        items = [row for row in purchases if row["is_recipe"] and row["completed_item_display_name"]
                 and row["player_slot"] in labels]
        figure.add_scatter(
            x=[row["game_time_s"] for row in items], y=[_row_label(row["player_slot"], labels) for row in items],
            mode="markers", name="Item completed", marker={"symbol": "diamond", "size": 8, "color": "#facc15",
                                                          "line": {"width": 1, "color": "#a16207"}},
            customdata=[[clock(row["game_time_s"]), row["completed_item_display_name"]] for row in items],
            hovertemplate="%{customdata[0]} · %{y} completed %{customdata[1]}<extra></extra>",
        )
    times = [death["game_time_s"] for death in deaths] + [0]
    figure.update_layout(
        xaxis=_time_axis(times),
        yaxis={"categoryorder": "array", "categoryarray": rows, "autorange": "reversed", "title": None,
               "showgrid": True, "gridcolor": GRID_COLOR},
    )
    figure.add_hline(y=4.5, line={"color": "rgba(148,163,184,0.45)", "dash": "dot", "width": 1})
    return _style(figure, height=110 + 38 * len(rows))


def build_advantage_story(records: list[dict], deaths: list[dict], fights: list[dict]) -> go.Figure:
    """Radiant's net-worth lead with every kill marked on it and teamfights shaded by winner."""
    figure = go.Figure()
    for fight in fights:
        if fight["kind"] == "teamfight":
            figure.add_vrect(x0=fight["start_s"] - 3, x1=fight["end_s"] + 3, line_width=0, layer="below",
                             fillcolor=TEAM_FILLS[fight["winning_team"]])
    times = [row["observation_time_s"] for row in records]
    figure.add_scatter(
        x=times, y=[row["net_worth_advantage"] for row in records], mode="lines", connectgaps=False,
        line={"color": "#a78bfa", "width": 2}, name="Radiant − Dire net worth", fill="tozeroy",
        fillcolor="rgba(167,139,250,0.12)",
        customdata=[clock(time) for time in times],
        hovertemplate="%{customdata} · Radiant − Dire: %{y:+,.0f} gold<extra></extra>",
    )
    by_time = {row["observation_time_s"]: row["net_worth_advantage"] for row in records}
    for team, symbol in ((2, "triangle-up"), (3, "triangle-down")):
        kills = [death for death in deaths if death["killer_team"] == team]
        figure.add_scatter(
            x=[death["game_time_s"] for death in kills],
            y=[by_time.get(int(death["game_time_s"] // 10) * 10) or 0 for death in kills],
            mode="markers", name=f"{TEAM_LABELS[team]} kill",
            marker={"symbol": symbol, "size": 9, "color": TEAM_COLORS[team], "line": {"width": 1, "color": "#0f172a"}},
            customdata=[clock(death["game_time_s"]) for death in kills],
            hovertemplate=f"%{{customdata}} · {TEAM_LABELS[team]} kill<extra></extra>",
        )
    figure.update_layout(xaxis=_time_axis(times),
                         yaxis={"title": "Radiant − Dire (gold)", "tickformat": "+,.0f", "zeroline": True,
                                "zerolinewidth": 2, "gridcolor": GRID_COLOR})
    return _style(figure, height=360)


# Damage

def build_team_damage_chart(timeline: list[dict]) -> go.Figure:
    """Hero damage each team dealt per minute: fights show up as spikes."""
    figure = go.Figure()
    for team in (2, 3):
        per_minute = defaultdict(int)
        for row in timeline:
            if row["team"] == team:
                per_minute[row["minute"]] += row["damage_to_heroes"]
        minutes = sorted(per_minute)
        figure.add_bar(x=[minute * 60 + 30 for minute in minutes], y=[per_minute[m] for m in minutes],
                       name=f"{TEAM_LABELS[team]} damage to heroes", marker_color=TEAM_COLORS[team],
                       customdata=[f"{clock(m * 60)}–{clock(m * 60 + 60)}" for m in minutes],
                       hovertemplate="%{customdata}: %{y:,.0f}<extra>" + TEAM_LABELS[team] + "</extra>")
    figure.update_layout(barmode="group", bargap=0.15,
                         xaxis=_time_axis([row["minute"] * 60 for row in timeline]),
                         yaxis={"title": "Damage per minute", "tickformat": ",.0f", "gridcolor": GRID_COLOR})
    return _style(figure, height=340)


def build_player_damage_chart(timeline: list[dict], slots: list[int], labels: dict[int, str]) -> go.Figure:
    """Hero damage dealt (solid) and taken from heroes (dotted) per minute for the chosen players."""
    figure = go.Figure()
    for slot in slots:
        rows = sorted((row for row in timeline if row["player_slot"] == slot), key=lambda row: row["minute"])
        x = [row["minute"] * 60 + 30 for row in rows]
        label = labels.get(slot, f"Slot {slot}")
        for key, dash, suffix in (("damage_to_heroes", "solid", "dealt"), ("damage_from_heroes", "dot", "taken")):
            figure.add_scatter(x=x, y=[row[key] for row in rows], mode="lines", name=f"{label} · {suffix}",
                               line={"color": SLOT_COLORS[slot], "dash": dash, "width": 2, "shape": "spline"},
                               customdata=[clock(row["minute"] * 60) for row in rows],
                               hovertemplate=f"{escape(label)} {suffix}" + " · minute from %{customdata}: %{y:,.0f}<extra></extra>")
    figure.update_layout(xaxis=_time_axis([row["minute"] * 60 for row in timeline]),
                         yaxis={"title": "Hero damage per minute", "tickformat": ",.0f", "gridcolor": GRID_COLOR,
                                "rangemode": "tozero"}, hovermode="x unified")
    return _style(figure, height=360)


def build_damage_breakdown(timeline: list[dict], slots: list[int], labels: dict[int, str]) -> go.Figure:
    """Whole-match damage dealt by each hero, split by what it hit."""
    parts = (("damage_to_heroes", "Enemy heroes", "#f472b6"), ("damage_to_buildings", "Buildings", "#38bdf8"),
             ("damage_to_roshan", "Roshan", "#a78bfa"), ("damage_to_units", "Creeps and units", "#94a3b8"))
    totals = {slot: defaultdict(int) for slot in slots}
    for row in timeline:
        if row["player_slot"] in totals:
            for key, _, _ in parts:
                totals[row["player_slot"]][key] += row[key]
    names = [labels.get(slot, f"Slot {slot}") for slot in slots]
    figure = go.Figure()
    for key, name, color in parts:
        figure.add_bar(y=names, x=[totals[slot][key] for slot in slots], name=name, orientation="h",
                       marker_color=color, hovertemplate="%{y} · " + name + ": %{x:,.0f}<extra></extra>")
    figure.update_layout(barmode="stack", yaxis={"autorange": "reversed", "automargin": True},
                         xaxis={"title": "Damage dealt", "tickformat": ",.0f", "gridcolor": GRID_COLOR})
    return _style(figure, height=110 + 40 * len(slots))


# Items

def build_item_timeline(purchases: list[dict], slots: list[int], labels: dict[int, str], *,
                        consumables: bool = False) -> go.Figure:
    """Each hero's purchases on one row: completed items (recipes) as stars, other items as diamonds."""
    order = [slot for slot in slots if slot < 5] + [slot for slot in slots if slot >= 5]
    rows = [labels.get(slot, f"Slot {slot}") for slot in order]
    figure = go.Figure()
    kinds = (
        ("Item completed", lambda r: r["is_recipe"] and r["completed_item_display_name"], "star", 13),
        ("Item bought", lambda r: not r["is_recipe"] and not r["is_consumable"], "diamond", 8),
    )
    if consumables:
        kinds += (("Consumable", lambda r: r["is_consumable"], "circle", 6),)
    for name, keep, symbol, size in kinds:
        chosen = [row for row in purchases if row["player_slot"] in labels and keep(row)]
        figure.add_scatter(
            x=[row["game_time_s"] for row in chosen], y=[labels[row["player_slot"]] for row in chosen],
            mode="markers", name=name,
            marker={"symbol": symbol, "size": size, "color": [SLOT_COLORS[row["player_slot"]] for row in chosen],
                    "line": {"width": 1, "color": "rgba(15,23,42,0.7)"}},
            customdata=[[clock(row["game_time_s"]),
                         row["completed_item_display_name"] if row["is_recipe"] and row["completed_item_display_name"]
                         else row["item_display_name"],
                         f" ×{row['quantity']}" if row["quantity"] > 1 else ""] for row in chosen],
            hovertemplate="%{customdata[0]} · %{y}: %{customdata[1]}%{customdata[2]}<extra></extra>",
        )
    figure.update_layout(xaxis=_time_axis([row["game_time_s"] for row in purchases] + [0]),
                         yaxis={"categoryorder": "array", "categoryarray": rows, "autorange": "reversed",
                                "showgrid": True, "gridcolor": GRID_COLOR})
    figure.add_hline(y=4.5, line={"color": "rgba(148,163,184,0.45)", "dash": "dot", "width": 1})
    return _style(figure, height=110 + 38 * len(rows))


# Laning

def build_lane_chart(lanes: list[dict], labels: dict[int, str]) -> go.Figure:
    """Net worth at 10:00 of every hero, grouped by lane, Radiant against Dire."""
    figure = go.Figure()
    lane_names = {"top": "Top", "mid": "Mid", "bot": "Bot", "jungle": "Jungle", "roaming": "Roaming"}
    for team in (2, 3):
        rows = [row for row in lanes if row["team"] == team and row["lane"] and row["net_worth_10"] is not None]
        figure.add_bar(
            x=[lane_names[row["lane"]] for row in rows], y=[row["net_worth_10"] for row in rows],
            name=TEAM_LABELS[team], marker_color=[SLOT_COLORS[row["player_slot"]] for row in rows],
            marker_line={"color": TEAM_COLORS[team], "width": 2},
            text=[labels.get(row["player_slot"], "") for row in rows], textposition="inside",
            offsetgroup=str(team), customdata=[[row["last_hits_10"], row["denies_10"]] for row in rows],
            hovertemplate="%{text}: %{y:,.0f} net worth · %{customdata[0]} LH / %{customdata[1]} DN<extra></extra>",
        )
    figure.update_layout(barmode="group", xaxis={"categoryorder": "array", "categoryarray": list(lane_names.values())},
                         yaxis={"title": "Net worth at 10:00", "tickformat": ",.0f", "gridcolor": GRID_COLOR})
    return _style(figure, height=380, legend=False)


# Replay insights

def build_deaths_by_minute(rows: list[dict]) -> go.Figure:
    """Deaths per match in each game minute, and how long heroes stay dead as the game goes on."""
    rows = [row for row in rows if row["matches_reaching"] >= 5 and row["minute"] >= 0]
    figure = go.Figure()
    figure.add_bar(x=[row["minute"] for row in rows],
                   y=[row["deaths"] / row["matches_reaching"] for row in rows], name="Deaths per match",
                   marker_color="#f472b6", customdata=[row["matches_reaching"] for row in rows],
                   hovertemplate="Minute %{x}: %{y:.2f} deaths per match (%{customdata} matches)<extra></extra>")
    figure.add_scatter(x=[row["minute"] for row in rows], y=[row["avg_dead_s"] for row in rows], yaxis="y2",
                       mode="lines", name="Average time dead (s)", line={"color": "#38bdf8", "width": 2},
                       hovertemplate="Minute %{x}: dead for %{y:.0f} s on average<extra></extra>")
    figure.update_layout(xaxis={"title": "Game minute", "gridcolor": GRID_COLOR},
                         yaxis={"title": "Deaths per match", "gridcolor": GRID_COLOR},
                         yaxis2={"title": "Seconds dead", "overlaying": "y", "side": "right", "showgrid": False,
                                 "rangemode": "tozero", "tickmode": "auto", "nticks": 5, "tickformat": ".0f"})
    return _style(figure, height=360)


def build_killer_kinds(rows: list[dict]) -> go.Figure:
    total = sum(row["deaths"] for row in rows) or 1
    rows = sorted(rows, key=lambda row: row["deaths"])
    figure = go.Figure(go.Bar(
        y=[KILLER_KIND_LABELS.get(row["killer_kind"], row["killer_kind"]) for row in rows],
        x=[row["deaths"] for row in rows], orientation="h", marker_color="#a78bfa",
        text=[f"{row['deaths'] / total:.1%}" for row in rows], textposition="outside", cliponaxis=False,
        hovertemplate="%{y}: %{x:,} deaths<extra></extra>",
    ))
    figure.update_layout(xaxis={"title": "Deaths (last hit)", "type": "log", "gridcolor": GRID_COLOR},
                         yaxis={"automargin": True})
    return _style(figure, height=320, legend=False)


LEAD_BINS = ((0, 2000, "Under 2k"), (2000, 5000, "2k–5k"), (5000, 10000, "5k–10k"), (10000, None, "10k+"))


def lead_win_rates(rows: list[dict]) -> list[dict]:
    """How often the team ahead in net worth went on to win, by lead size and game time."""
    out = []
    for time in (600, 1200, 1800):
        for low, high, label in LEAD_BINS:
            chosen = [row for row in rows if row["observation_time_s"] == time and row["net_worth_advantage"]
                      and low <= abs(row["net_worth_advantage"]) and (high is None or abs(row["net_worth_advantage"]) < high)]
            wins = sum((row["net_worth_advantage"] > 0) == row["radiant_won"] for row in chosen)
            out.append({"time_s": time, "bin": label, "matches": len(chosen),
                        "win_rate": wins / len(chosen) if chosen else None})
    return out


def build_lead_chart(rates: list[dict]) -> go.Figure:
    figure = go.Figure()
    for time, color in ((600, "#38bdf8"), (1200, "#a78bfa"), (1800, "#f472b6")):
        rows = [row for row in rates if row["time_s"] == time]
        figure.add_bar(x=[row["bin"] for row in rows], y=[row["win_rate"] for row in rows],
                       name=f"Lead at {clock(time)}", marker_color=color,
                       text=[f"n={row['matches']}" for row in rows], textposition="outside", cliponaxis=False,
                       hovertemplate="%{x} lead at " + clock(time) + ": team ahead won %{y:.0%} (%{text})<extra></extra>")
    figure.add_hline(y=0.5, line={"dash": "dot", "color": "rgba(148,163,184,0.7)"})
    figure.update_layout(barmode="group", xaxis={"title": "Net-worth lead"},
                         yaxis={"title": "Team ahead won", "tickformat": ".0%", "range": [0, 1.12], "gridcolor": GRID_COLOR})
    return _style(figure, height=360)


def build_item_timing_chart(rows: list[dict], *, minimum_rate: float) -> go.Figure:
    """When a hero usually finishes each item it often buys (median first purchase), earliest first."""
    rows = sorted((row for row in rows if row["purchase_rate"] >= minimum_rate),
                  key=lambda row: row["median_first_purchase_s"])
    figure = go.Figure(go.Bar(
        y=[row["item_display_name"] for row in rows], x=[row["median_first_purchase_s"] for row in rows],
        orientation="h", marker={"color": [row["purchase_rate"] for row in rows], "colorscale": "Viridis",
                                 "cmin": 0, "cmax": 1, "colorbar": {"title": "Bought in", "tickformat": ".0%"}},
        customdata=[[clock(row["median_first_purchase_s"]), row["purchase_rate"], row["matches_bought"],
                     row["hero_matches"]] for row in rows],
        hovertemplate="%{y}: median %{customdata[0]} · bought in %{customdata[1]:.0%} "
                      "(%{customdata[2]} of %{customdata[3]} matches)<extra></extra>",
    ))
    ticks = list(range(0, int(max((row["median_first_purchase_s"] for row in rows), default=0)) + 300, 300))
    figure.update_layout(xaxis={"title": "Median first purchase", "tickvals": ticks,
                                "ticktext": [clock(tick) for tick in ticks], "gridcolor": GRID_COLOR},
                         yaxis={"autorange": "reversed", "automargin": True})
    return _style(figure, height=max(320, 90 + 24 * len(rows)), legend=False)
