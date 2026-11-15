"""Event-level sections of the match page: story, timeline, map, fights, laning, items and damage.

Each section loads only the marts it draws, through the events_db module (looked up at call time,
so tests can replace loaders). A failed load shows a warning in that section and leaves the rest
of the page working.
"""

from collections import Counter, defaultdict
from html import escape

import streamlit as st

import dashboard_db
import event_charts as ec
import events_db
from components import format_duration
from postgres_connection import DashboardDatabaseError


MATCH_SECTIONS = {
    "overview": "Overview", "timeline": "Timeline", "map": "Map", "fights": "Fights",
    "laning": "Laning", "items": "Items", "damage": "Damage",
}
PLAYER_SECTIONS = {"summary": "Summary", "story": "Kills & lane", "map": "Map", "items": "Items & damage"}
PHASE_LABELS = {"laning": "Laning (0–10 min)", "midgame": "Midgame (10–25 min)", "lategame": "Late game (25+ min)"}
LANE_LABELS = {"top": "Top", "mid": "Mid", "bot": "Bot", "jungle": "Jungle", "roaming": "Roaming"}
ROLE_LABELS = {"safe": "safe lane", "off": "off lane", "mid": "mid", "jungle": "jungle", "roaming": "roaming"}
MAP_CONFIG = {"scrollZoom": True, "displaylogo": False}


def hero_labels(players: list[dict]) -> dict[int, str]:
    """Slot to hero name, falling back to the slot for an unknown hero."""
    return {player["slot"]: player.get("hero_label") or player.get("hero") or f"Slot {player['slot']}"
            for player in players}


def _store_section(key: str, param: str) -> None:
    value = st.session_state.get(key)
    if value is not None:
        st.query_params[param] = value


def section_switcher(sections: dict[str, str], param: str, key: str) -> str:
    """A row of section buttons whose choice lives in the URL, so links reopen the same section."""
    requested = st.query_params.get(param)
    current = requested if requested in sections else next(iter(sections))
    st.session_state[key] = current
    st.segmented_control(
        "Section", options=list(sections), format_func=sections.__getitem__, key=key,
        on_change=_store_section, args=(key, param), label_visibility="collapsed",
    )
    return current


def _load(what: str, loader, *args):
    """Run an optional loader; on failure warn and return None so the section can carry on."""
    try:
        return loader(*args)
    except DashboardDatabaseError:
        st.warning(f"{what} could not be loaded. Run the pipeline's dbt build if the event marts are new.")
        return None


def _cards(items: list[tuple[str, str, str]]) -> None:
    """Small labelled cards: (label, value, detail)."""
    cards = "".join(
        f'<div class="dota-card"><div class="label">{escape(label)}</div>'
        f'<div class="value">{value}</div><div class="detail">{escape(detail)}</div></div>'
        for label, value, detail in items
    )
    st.html(
        """
        <style>
        .dota-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: .75rem; }
        .dota-card { border: 1px solid rgba(128,128,128,.25); border-radius: .6rem; padding: .7rem .85rem; }
        .dota-card .label { font-size: .8rem; opacity: .7; }
        .dota-card .value { font-size: 1.35rem; font-weight: 600; margin: .15rem 0; font-variant-numeric: tabular-nums; }
        .dota-card .detail { font-size: .8rem; opacity: .75; }
        .dota-card .radiant { color: #22c55e; } .dota-card .dire { color: #ef4444; }
        </style>
        """ + f'<div class="dota-cards">{cards}</div>'
    )


def _team_span(radiant, dire) -> str:
    return f'<span class="radiant">{escape(str(radiant))}</span> – <span class="dire">{escape(str(dire))}</span>'


# Overview

def render_match_story(config, match_id: int, players: list[dict], team_advantage: list[dict] | None,
                       *, deaths: list[dict] | None = None, fights: list[dict] | None = None) -> None:
    """Key moments of the match in one row of cards, from the deaths and fights marts.

    The caller may pass deaths and fights it already loaded; otherwise they are loaded here.
    """
    if deaths is None or fights is None:
        try:
            deaths = events_db.load_match_deaths(config, match_id)
            fights = events_db.load_match_fights(config, match_id)
        except DashboardDatabaseError:
            st.caption("Match story unavailable: the event marts have not been built yet.")
            return
    if not deaths:
        return
    labels = hero_labels(players)
    kills = Counter(death["killer_team"] for death in deaths if death["killer_team"] is not None)
    first = next((death for death in deaths if death["is_first_blood"]), deaths[0])
    teamfights = [fight for fight in fights if fight["kind"] == "teamfight"]
    won = Counter(fight["winning_team"] for fight in teamfights)
    cards = [
        ("Kills", _team_span(kills[2], kills[3]), "Radiant – Dire, credited kills"),
        ("First blood", escape(format_duration(max(0, first["game_time_s"]))),
         f"{ec._killer_label(first, labels)} killed {labels.get(first['victim_slot'], '?')}"),
        ("Teamfights won", _team_span(won[2], won[3]),
         f"{len(teamfights)} fights of 3+ deaths" + (f", {won[None]} even" if won[None] else "")),
    ]
    complete = [row for row in team_advantage or [] if row["net_worth_advantage"] is not None]
    if complete:
        best = max(complete, key=lambda row: row["net_worth_advantage"])
        worst = min(complete, key=lambda row: row["net_worth_advantage"])
        cards.append(("Biggest leads",
                      _team_span(f"+{max(0, best['net_worth_advantage']):,}", f"+{max(0, -worst['net_worth_advantage']):,}"),
                      f"Radiant at {format_duration(best['observation_time_s'])}, "
                      f"Dire at {format_duration(worst['observation_time_s'])}"))
    if fights:
        bloodiest = max(fights, key=lambda fight: (fight["deaths"], -fight["start_s"]))
        cards.append(("Bloodiest fight", escape(f"{bloodiest['deaths']} deaths"),
                      f"at {format_duration(max(0, bloodiest['start_s']))}: Radiant lost {bloodiest['radiant_deaths']}, "
                      f"Dire lost {bloodiest['dire_deaths']}"))
    dead = Counter()
    for death in deaths:
        if death["dead_s"] is not None:
            dead[death["victim_slot"]] += death["dead_s"]
    if dead:
        slot, seconds = dead.most_common(1)[0]
        cards.append(("Most time dead", escape(format_duration(seconds)), labels.get(slot, f"Slot {slot}")))
    st.subheader("Match story")
    _cards(cards)


# Timeline

def _kill_feed(deaths: list[dict], labels: dict[int, str]) -> list[dict]:
    feed = []
    for death in deaths:
        credited = death["credited_killer_slot"]
        direct = (labels.get(death["killer_slot"], "?") if death["killer_slot"] is not None
                  else ec.KILLER_KIND_LABELS.get(death["killer_kind"], "Unknown"))
        feed.append({
            "Time": ec.clock(death["game_time_s"]),
            "Fight": death["fight_number"],
            "Killer": ec._killer_label(death, labels),
            "Victim": labels.get(death["victim_slot"], "?"),
            "Victim team": ec.TEAM_LABELS[death["victim_team"]],
            "Assists": ec._assist_label(death, labels),
            "Last hit": "" if death["killer_slot"] == credited else direct,
            "Dead for": None if death["dead_s"] is None else round(death["dead_s"]),
            "Note": "" if death["link_status"] == "exact" else "credit link uncertain",
        })
    return feed


def render_timeline(config, match_id: int, players: list[dict]) -> None:
    labels = hero_labels(players)
    deaths = _load("Deaths", events_db.load_match_deaths, config, match_id)
    fights = _load("Fights", events_db.load_match_fights, config, match_id)
    if deaths is None or fights is None:
        return
    if not deaths:
        st.info("No deaths are recorded for this match yet.")
        return
    try:
        advantage = dashboard_db.load_team_advantage(config, match_id)
    except DashboardDatabaseError:
        advantage = None
    if advantage:
        st.subheader("Net worth and kills")
        st.caption("Radiant's net-worth lead with every credited kill on it: ▲ Radiant kill, ▼ Dire kill. "
                   "Shaded bands are teamfights (3+ deaths), green when Radiant lost fewer heroes, red when Dire did.")
        st.plotly_chart(ec.build_advantage_story(advantage, deaths, fights), key=f"advantage_story_{match_id}")

    st.subheader("Event timeline")
    left, right = st.columns(2)
    show_assists = left.toggle("Show assists", value=True, key=f"timeline_assists_{match_id}")
    show_items = right.toggle("Show completed items", value=False, key=f"timeline_items_{match_id}",
                              help="Items finished by buying their recipe. Items with no recipe are on the Items section.")
    purchases = _load("Purchases", events_db.load_purchases, config, match_id) if show_items else None
    st.caption("One row per hero: ▲ kill (credited), ○ assist, ✕ death. Shading marks fights of two or more deaths, "
               "coloured by the side that lost fewer heroes. Hover for details.")
    st.plotly_chart(ec.build_event_timeline(sorted(labels), labels, deaths, fights, purchases,
                                            show_assists=show_assists), key=f"event_timeline_{match_id}")

    st.subheader("Kill feed")
    st.caption("Killer is the player credited on the scoreboard. Last hit shows who actually landed it when that "
               "differs (a tower, creep or summon). Fight numbers group deaths less than 15 seconds apart.")
    st.dataframe(_kill_feed(deaths, labels), hide_index=True, width="stretch", key=f"kill_feed_{match_id}",
                 column_config={"Dead for": st.column_config.NumberColumn(format="%d s")})


# Map

def render_map(config, match_id: int, players: list[dict]) -> None:
    labels = hero_labels(players)
    mode = st.segmented_control("Map view", options=["replay", "deaths"], default="replay",
                                format_func={"replay": "Map replay", "deaths": "Where heroes died"}.__getitem__,
                                key=f"map_mode_{match_id}")
    mode = mode or "replay"
    if mode == "replay":
        positions = _load("Hero positions", events_db.load_match_positions, config, match_id)
        deaths = _load("Deaths", events_db.load_match_deaths, config, match_id) or []
        if positions is None:
            return
        if not positions:
            st.info("No hero positions are recorded for this match.")
            return
        st.caption("Press Play or drag the slider. Each dot is a hero in its player colour, ringed green for Radiant "
                   "and red for Dire; a faded cross is a dead hero at its corpse. Big crosses mark deaths of the "
                   "last 20 seconds. "
                   "Scroll to zoom.")
        st.plotly_chart(ec.build_replay_map(positions, deaths, labels), key=f"replay_map_{match_id}",
                        config=MAP_CONFIG)
        return

    deaths = _load("Deaths", events_db.load_match_deaths, config, match_id)
    if deaths is None:
        return
    end = max([int(death["game_time_s"] // 60) + 1 for death in deaths] + [1])
    start_min, end_min = st.slider("Game minutes", min_value=0, max_value=end, value=(0, end),
                                   key=f"death_map_range_{match_id}")
    shown = [death for death in deaths if start_min * 60 <= death["game_time_s"] <= end_min * 60]
    figure = ec.map_figure()
    ec.add_death_markers(figure, shown, labels, size=13)
    st.caption(f"{len(shown)} deaths between {start_min}:00 and {end_min}:00, at the corpse, coloured by the "
               "victim's team. Hover a cross for the killer and assists.")
    st.plotly_chart(figure, key=f"death_map_{match_id}", config=MAP_CONFIG)


def _phase(seconds: float) -> str:
    return "laning" if seconds < 600 else "midgame" if seconds < 1500 else "lategame"


def _add_kill_markers(figure, kills: list[dict], labels: dict[int, str]) -> None:
    rows = [death for death in kills if death["x"] is not None]
    if not rows:
        return
    figure.add_scatter(
        x=[row["x"] for row in rows], y=[row["y"] for row in rows], mode="markers", name="Kill",
        marker={"symbol": "star", "size": 15, "color": "#facc15", "line": {"width": 1, "color": "#713f12"}},
        customdata=[[ec.clock(row["game_time_s"]), labels.get(row["victim_slot"], "?")] for row in rows],
        hovertemplate="%{customdata[0]} · killed %{customdata[1]}<extra></extra>",
    )


# Fights

def _fight_label(fight: dict) -> str:
    winner = {2: "Radiant won", 3: "Dire won"}.get(fight["winning_team"], "Even trade")
    return (f"#{fight['fight_number']} · {ec.clock(fight['start_s'])} · {fight['kind']} · "
            f"{fight['radiant_deaths']}–{fight['dire_deaths']} deaths · {winner}")


def render_fights(config, match_id: int, players: list[dict]) -> None:
    labels = hero_labels(players)
    fights = _load("Fights", events_db.load_match_fights, config, match_id)
    if fights is None:
        return
    if not fights:
        st.info("No deaths, so no fights, are recorded for this match yet.")
        return
    teamfights = [fight for fight in fights if fight["kind"] == "teamfight"]
    won = Counter(fight["winning_team"] for fight in teamfights)
    _cards([
        ("Teamfights", escape(str(len(teamfights))), "3+ deaths, under 15 s apart"),
        ("Teamfights won", _team_span(won[2], won[3]), "Radiant – Dire, by fewer heroes lost"),
        ("Skirmishes", escape(str(sum(fight["kind"] == "skirmish" for fight in fights))), "2 deaths"),
        ("Pick-offs", escape(str(sum(fight["kind"] == "pick-off" for fight in fights))), "a single death"),
    ])
    include_picks = st.toggle("Include pick-offs", value=False, key=f"fights_picks_{match_id}")
    shown = [fight for fight in fights if include_picks or fight["kind"] != "pick-off"]
    if not shown:
        st.info("No fights with two or more deaths. Include pick-offs to see single kills.")
        return
    st.caption("Damage is hero damage to enemy heroes from 10 s before the first death to 3 s after the last. "
               "Net-worth swing is the change in Radiant's lead across that window (it includes any farming then).")
    st.dataframe([{
        "Fight": fight["fight_number"], "Start": ec.clock(fight["start_s"]), "Length (s)": round(fight["duration_s"]),
        "Kind": fight["kind"], "Radiant deaths": fight["radiant_deaths"], "Dire deaths": fight["dire_deaths"],
        "Winner": ec.TEAM_LABELS.get(fight["winning_team"], "Even"),
        "Radiant damage": fight["radiant_hero_damage"], "Dire damage": fight["dire_hero_damage"],
        "Heroes involved": fight["participants"], "Net-worth swing": fight["advantage_swing"],
    } for fight in shown], hide_index=True, width="stretch", key=f"fights_table_{match_id}",
        column_config={"Radiant damage": st.column_config.NumberColumn(format="localized"),
                       "Dire damage": st.column_config.NumberColumn(format="localized"),
                       "Net-worth swing": st.column_config.NumberColumn(format="%+d")})

    default = max(range(len(shown)), key=lambda index: (shown[index]["deaths"], -index))
    chosen = st.selectbox("Inspect a fight", range(len(shown)), index=default, key=f"fight_pick_{match_id}",
                          format_func=lambda index: _fight_label(shown[index]))
    fight = shown[chosen]
    participants = _load("Fight participants", events_db.load_fight_players, config, match_id) or []
    participants = [row for row in participants if row["fight_number"] == fight["fight_number"]]
    deaths = [death for death in (_load("Deaths", events_db.load_match_deaths, config, match_id) or [])
              if death["fight_number"] == fight["fight_number"]]
    positions = _load("Hero positions", events_db.load_match_positions, config, match_id) or []
    map_column, table_column = st.columns([3, 2])
    with map_column:
        st.caption("Paths of the heroes involved in five-second steps (the big dot is where each ended) and ✕ "
                   "where heroes died.")
        st.plotly_chart(ec.build_fight_map(fight, positions, deaths, labels,
                                           {row["player_slot"] for row in participants}),
                        key=f"fight_map_{match_id}", config=MAP_CONFIG)
    with table_column:
        st.markdown(f"**{_fight_label(fight)}**")
        st.dataframe([{
            "Hero": labels.get(row["player_slot"], "?"), "Team": ec.TEAM_LABELS[row["team"]],
            "K": row["kills"], "D": row["deaths"], "A": row["assists"],
            "Damage dealt": row["damage_dealt"], "Damage taken": row["damage_taken"],
        } for row in sorted(participants, key=lambda row: (row["team"], -row["damage_dealt"]))],
            hide_index=True, width="stretch", key=f"fight_players_{match_id}",
            column_config={"Damage dealt": st.column_config.NumberColumn(format="localized"),
                           "Damage taken": st.column_config.NumberColumn(format="localized")})
        for death in deaths:
            st.text(f"{ec.clock(death['game_time_s'])}  {ec._killer_label(death, labels)} killed "
                    f"{labels.get(death['victim_slot'], '?')}")


# Laning

def _lane_table(rows: list[dict], labels: dict[int, str]) -> str:
    def cell(value):
        return "—" if value is None else f"{value:,}"
    body = "".join(
        f'<tr class="{"radiant" if row["team"] == 2 else "dire"}"><td>{escape(labels.get(row["player_slot"], "?"))}</td>'
        f'<td>{cell(row["net_worth_10"])}</td><td>{cell(row["last_hits_10"])}/{cell(row["denies_10"])}</td>'
        f'<td>{cell(row["level_10"])}</td><td>{cell(row["kills_10"])}/{cell(row["deaths_10"])}</td></tr>'
        for row in rows
    )
    return ('<table class="dota-lane"><thead><tr><th>Hero</th><th>NW</th><th>LH/DN</th><th>Lvl</th><th>K/D</th>'
            f"</tr></thead><tbody>{body}</tbody></table>")


def render_laning(config, match_id: int, players: list[dict]) -> None:
    labels = hero_labels(players)
    lanes = _load("Lanes", events_db.load_player_lanes, config, match_id)
    if lanes is None:
        return
    if not any(row["lane"] for row in lanes):
        st.info("No laning-phase positions are recorded for this match.")
        return
    st.caption("Lanes come from where each hero stood while alive from 1:00 to 10:00. Stats are each hero's "
               "values at 10:00. The lane result compares the two sides' total net worth in that lane.")
    st.html("""
        <style>
        .dota-lane { width: 100%; border-collapse: collapse; font-size: .85rem; font-variant-numeric: tabular-nums; }
        .dota-lane th, .dota-lane td { padding: .3rem .4rem; border-bottom: 1px solid rgba(128,128,128,.2); text-align: right; }
        .dota-lane th:first-child, .dota-lane td:first-child { text-align: left; }
        .dota-lane tr.radiant td:first-child { border-left: 3px solid #22c55e; padding-left: .5rem; }
        .dota-lane tr.dire td:first-child { border-left: 3px solid #ef4444; padding-left: .5rem; }
        </style>""")
    for column, lane in zip(st.columns(3), ("top", "mid", "bot")):
        rows = sorted((row for row in lanes if row["lane"] == lane), key=lambda row: (row["team"], -(row["net_worth_10"] or 0)))
        with column.container(border=True):
            st.markdown(f"**{LANE_LABELS[lane]} lane**")
            if not rows:
                st.caption("Nobody laned here.")
                continue
            totals = {team: sum(row["net_worth_10"] or 0 for row in rows if row["team"] == team) for team in (2, 3)}
            measured = all(row["net_worth_10"] is not None for row in rows)
            if measured and {row["team"] for row in rows} == {2, 3}:
                lead = totals[2] - totals[3]
                verdict = ("Even lane" if abs(lead) < 300 else
                           f"{'Radiant' if lead > 0 else 'Dire'} won by {abs(lead):,} net worth")
                st.caption(verdict)
            st.html(_lane_table(rows, labels))
    others = [row for row in lanes if row["lane"] in ("jungle", "roaming")]
    if others:
        st.caption("Also: " + ", ".join(f"{labels.get(row['player_slot'], '?')} ({LANE_LABELS[row['lane']].lower()})"
                                         for row in others))
    st.markdown("**Net worth at 10:00 by lane**")
    st.caption("Left bar Radiant (green outline), right bar Dire (red outline); each block is one hero.")
    st.plotly_chart(ec.build_lane_chart(lanes, labels), key=f"lane_chart_{match_id}")


# Items

WARD_ITEMS = {"item_ward_observer": "Observer", "item_ward_sentry": "Sentry", "item_ward_dispenser": "Observer and Sentry"}


def _build_rows(purchases: list[dict], slot: int, consumables: bool) -> list[dict]:
    return [{
        "Time": ec.clock(row["game_time_s"]),
        "Item": (row["completed_item_display_name"] if row["is_recipe"] and row["completed_item_display_name"]
                 else row["item_display_name"]),
        "Kind": ("Completed (recipe)" if row["is_recipe"] and row["completed_item_display_name"]
                 else "Consumable" if row["is_consumable"] else "Item"),
        "Quantity": row["quantity"],
    } for row in purchases if row["player_slot"] == slot and (consumables or not row["is_consumable"])]


def render_items(config, match_id: int, players: list[dict]) -> None:
    labels = hero_labels(players)
    purchases = _load("Purchases", events_db.load_purchases, config, match_id)
    if purchases is None:
        return
    if not purchases:
        st.info("No purchases are recorded for this match.")
        return
    consumables = st.toggle("Show consumables (wards, regeneration, scrolls)", value=False, key=f"items_consumables_{match_id}")
    st.caption("★ an item finished by buying its recipe, ◆ any other item bought. Hover for names and times.")
    st.plotly_chart(ec.build_item_timeline(purchases, sorted(labels), labels, consumables=consumables),
                    key=f"item_timeline_{match_id}")

    left, right = st.columns([3, 2])
    with left:
        slot = st.selectbox("Build order", sorted(labels), key=f"build_order_{match_id}",
                            format_func=lambda option: f"{labels[option]} · {ec.TEAM_LABELS[2 if option < 5 else 3]}")
        st.dataframe(_build_rows(purchases, slot, consumables), hide_index=True, width="stretch",
                     key=f"build_order_table_{match_id}")
    with right:
        st.markdown("**Vision bought**")
        wards = defaultdict(Counter)
        for row in purchases:
            if row["item_name"] in WARD_ITEMS:
                wards[row["player_slot"]][WARD_ITEMS[row["item_name"]]] += row["quantity"]
        if not wards:
            st.caption("No wards bought.")
        else:
            st.dataframe([{"Hero": labels.get(slot, "?"), "Team": ec.TEAM_LABELS[2 if slot < 5 else 3],
                           **{kind: wards[slot][kind] for kind in WARD_ITEMS.values()}}
                          for slot in sorted(wards)], hide_index=True, width="stretch", key=f"wards_{match_id}")
            st.caption("Purchases, not wards placed. A dispenser holds both kinds.")


# Damage

def render_damage(config, match_id: int, players: list[dict]) -> None:
    labels = hero_labels(players)
    timeline = _load("Damage timeline", events_db.load_damage_timeline, config, match_id)
    if timeline is None:
        return
    if not timeline:
        st.info("No damage is recorded for this match.")
        return
    st.subheader("Hero damage per minute")
    st.caption("Damage each team dealt to enemy heroes in each game minute. Spikes are fights.")
    st.plotly_chart(ec.build_team_damage_chart(timeline), key=f"team_damage_{match_id}")
    st.subheader("What each hero hit")
    st.caption("Whole-match damage dealt, split by target. Creeps and units include neutrals, summons and wards.")
    st.plotly_chart(ec.build_damage_breakdown(timeline, sorted(labels), labels), key=f"damage_breakdown_{match_id}")
    st.subheader("Compare heroes")
    totals = Counter()
    for row in timeline:
        totals[row["player_slot"]] += row["damage_to_heroes"]
    default = [slot for slot, _ in totals.most_common(2)]
    chosen = st.multiselect("Heroes", sorted(labels), default=default, key=f"damage_heroes_{match_id}",
                            format_func=lambda option: labels[option], max_selections=4)
    if chosen:
        st.caption("Solid: hero damage dealt per minute. Dotted: damage taken from enemy heroes.")
        st.plotly_chart(ec.build_player_damage_chart(timeline, chosen, labels), key=f"player_damage_{match_id}")


SECTION_RENDERERS = {
    "timeline": render_timeline, "map": render_map, "fights": render_fights, "laning": render_laning,
    "items": render_items, "damage": render_damage,
}


def render_match_section(section: str, config, match_id: int, players: list[dict]) -> None:
    SECTION_RENDERERS[section](config, match_id, players)


# Player sections

def render_player_section(section: str, config, match: dict, player: dict, players: list[dict],
                          comparison: dict | None) -> None:
    """The event sections of one player's page; Summary is drawn by player.render_player_page."""
    labels = hero_labels(players)
    match_id = match["match_id"]
    slots = [player["slot"]] + ([comparison["slot"]] if comparison else [])
    st.title(labels[player["slot"]])
    st.caption(f"{ec.TEAM_LABELS.get(player['team'], '—')} · Match {match_id}"
               + (f" · compared with {labels[comparison['slot']]}" if comparison else ""))
    if section == "story":
        _render_player_story(config, match_id, slots, labels, players)
    elif section == "map":
        _render_player_map(config, match_id, slots, labels)
    else:
        _render_player_items(config, match_id, slots, labels)


def _render_player_story(config, match_id: int, slots: list[int], labels: dict[int, str], players: list[dict]) -> None:
    deaths = _load("Deaths", events_db.load_match_deaths, config, match_id)
    lanes = _load("Lanes", events_db.load_player_lanes, config, match_id)
    fight_rows = _load("Fight participants", events_db.load_fight_players, config, match_id) or []
    if lanes:
        st.subheader("Laning")
        by_slot = {row["player_slot"]: row for row in lanes}
        for slot in slots:
            row = by_slot.get(slot)
            if not row or not row["lane"]:
                st.caption(f"{labels[slot]}: no laning-phase positions recorded.")
                continue
            rivals = [other for other in lanes if other["lane"] == row["lane"] and other["team"] != row["team"]]
            mates = [other for other in lanes if other["lane"] == row["lane"] and other["team"] == row["team"]
                     and other["player_slot"] != slot]
            role = ROLE_LABELS.get(row["lane_role"], row["lane_role"])
            st.markdown(f"**{escape(labels[slot])}** played **{LANE_LABELS[row['lane']].lower()}** ({role}, "
                        f"{row['lane_share']:.0%} of laning time there)"
                        + (f" with {', '.join(labels.get(m['player_slot'], '?') for m in mates)}" if mates else "")
                        + (f" against {', '.join(labels.get(r['player_slot'], '?') for r in rivals)}." if rivals else "."))
            table = [row] + mates + rivals
            st.dataframe([{
                "Hero": labels.get(r["player_slot"], "?"), "Team": ec.TEAM_LABELS[r["team"]],
                "Net worth @10": r["net_worth_10"], "Last hits @10": r["last_hits_10"], "Denies @10": r["denies_10"],
                "Level @10": r["level_10"], "Kills @10": r["kills_10"], "Deaths @10": r["deaths_10"],
            } for r in table], hide_index=True, width="stretch", key=f"lane_compare_{match_id}_{slot}")
    if deaths is None:
        return
    st.subheader("Kills, deaths and assists")
    involved = [death for death in deaths if death["victim_slot"] in slots or death["credited_killer_slot"] in slots
                or any(slot in death["assists"] for slot in slots)]
    if not involved:
        st.info("No kills, deaths or assists recorded.")
    else:
        fights = _load("Fights", events_db.load_match_fights, config, match_id) or []
        st.plotly_chart(ec.build_event_timeline(slots, {slot: labels[slot] for slot in slots}, involved, fights),
                        key=f"player_timeline_{match_id}_{'_'.join(map(str, slots))}")
    for slot in slots:
        dead = sum(death["dead_s"] or 0 for death in deaths if death["victim_slot"] == slot)
        fights_in = [row for row in fight_rows if row["player_slot"] == slot]
        teamfight_damage = sum(row["damage_dealt"] for row in fights_in)
        _cards([
            ("Time dead", escape(format_duration(dead)), labels[slot]),
            ("Fights joined", escape(str(len(fights_in))), "dealt, took damage or scored in a fight"),
            ("Damage in fights", escape(f"{teamfight_damage:,}"), "to enemy heroes, inside fight windows"),
            ("First blood", escape("yes" if any(d["is_first_blood"] and slot in (d["credited_killer_slot"], d["victim_slot"])
                                                 for d in deaths) else "no"), "killed or died for first blood"),
        ])
    st.dataframe(_kill_feed(involved, labels), hide_index=True, width="stretch",
                 key=f"player_feed_{match_id}_{'_'.join(map(str, slots))}",
                 column_config={"Dead for": st.column_config.NumberColumn(format="%d s")})


def _render_player_map(config, match_id: int, slots: list[int], labels: dict[int, str]) -> None:
    deaths = _load("Deaths", events_db.load_match_deaths, config, match_id)
    if deaths is None:
        return
    phases = st.pills("Phase", options=list(PHASE_LABELS), selection_mode="multi", default=list(PHASE_LABELS),
                      format_func=PHASE_LABELS.__getitem__, key=f"player_phase_{match_id}") or list(PHASE_LABELS)
    figure = ec.map_figure()
    for slot in slots:
        ec.add_death_markers(figure, [d for d in deaths if d["victim_slot"] == slot and _phase(d["game_time_s"]) in phases],
                             labels, size=13)
        _add_kill_markers(figure, [d for d in deaths if d["credited_killer_slot"] == slot
                                   and _phase(d["game_time_s"]) in phases], labels)
    names = " and ".join(labels[slot] for slot in slots)
    st.caption(f"★ heroes {names} killed and ✕ where they died, coloured by the victim's team.")
    st.plotly_chart(figure, key=f"player_map_{match_id}", config=MAP_CONFIG)


def _render_player_items(config, match_id: int, slots: list[int], labels: dict[int, str]) -> None:
    purchases = _load("Purchases", events_db.load_purchases, config, match_id)
    timeline = _load("Damage timeline", events_db.load_damage_timeline, config, match_id)
    if purchases:
        st.subheader("Items bought")
        consumables = st.toggle("Show consumables", value=False, key=f"player_consumables_{match_id}")
        chosen = {slot: labels[slot] for slot in slots}
        st.plotly_chart(ec.build_item_timeline(purchases, slots, chosen, consumables=consumables),
                        key=f"player_items_{match_id}")
        for column, slot in zip(st.columns(len(slots)), slots):
            with column:
                st.markdown(f"**{escape(labels[slot])}**")
                st.dataframe(_build_rows(purchases, slot, consumables), hide_index=True, width="stretch",
                             key=f"player_build_{match_id}_{slot}")
    if timeline:
        st.subheader("Damage per minute")
        st.caption("Solid: hero damage dealt per minute. Dotted: damage taken from enemy heroes.")
        st.plotly_chart(ec.build_player_damage_chart(timeline, slots, labels), key=f"player_damage_minute_{match_id}")
        st.plotly_chart(ec.build_damage_breakdown(timeline, slots, labels), key=f"player_damage_split_{match_id}")
