"""Load hero summaries and match records from the Postgres dashboard marts."""

from math import isfinite

from config import WarehouseConfig
from postgres_connection import DashboardDatabaseError, open_postgres_database


HERO_PERFORMANCE_METRICS = (
    ("avg_kills", "kills_observations"),
    ("avg_deaths", "deaths_observations"),
    ("avg_assists", "assists_observations"),
    ("avg_gpm", "gpm_observations"),
    ("avg_xpm", "xpm_observations"),
    ("avg_hero_damage_per_min", "hero_damage_observations"),
    ("avg_hero_healing_per_min", "hero_healing_observations"),
    ("avg_tower_damage_per_min", "tower_damage_observations"),
)
HERO_PERFORMANCE_COLUMNS = (
    "hero_id", "appearances", "wins", "losses", "win_rate", "avg_match_duration_s",
    *(column for pair in HERO_PERFORMANCE_METRICS for column in pair),
)

HERO_MATCHUP_COLUMNS = (
    "hero_id", "opponent_hero_id", "opponent_name", "matches", "wins", "losses",
    "win_rate", "overall_matches", "overall_win_rate", "win_rate_difference_pp",
)

TEAM_ADVANTAGE_COLUMNS = (
    "observation_time_s", "radiant_net_worth", "dire_net_worth", "net_worth_advantage",
    "radiant_players_observed", "dire_players_observed", "max_snapshot_age_s",
    "net_worth_advantage_change_120s",
)


def load_team_advantage(config: WarehouseConfig, match_id: int) -> list[dict]:
    """Read one replay's complete grid, preserving missing coverage as chart gaps."""
    if type(match_id) is not int or not 0 < match_id < 2**63:
        raise DashboardDatabaseError("Choose a valid match to load team advantage.")
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT " + ", ".join(TEAM_ADVANTAGE_COLUMNS)
            + " FROM marts.team_advantage_timeline WHERE match_id = %s ORDER BY observation_time_s",
            (match_id,),
        )
        rows = cursor.fetchall()
    records, by_time = [], {}
    for row in rows:
        record = dict(zip(TEAM_ADVANTAGE_COLUMNS, row, strict=True))
        time = record["observation_time_s"]
        if type(time) is not int or time != len(records) * 10:
            raise DashboardDatabaseError("The team advantage mart contains an invalid observation grid.")
        for side in ("radiant", "dire"):
            count, total = record[side + "_players_observed"], record[side + "_net_worth"]
            if (type(count) is not int or not 0 <= count <= 5
                    or (total is None) != (count < 5)
                    or (total is not None and (type(total) is not int or total < 0))):
                raise DashboardDatabaseError("The team advantage mart contains invalid team coverage.")
        radiant, dire = record["radiant_net_worth"], record["dire_net_worth"]
        advantage = None if radiant is None or dire is None else radiant - dire
        previous = by_time.get(time - 120)
        expected_change = None if advantage is None or previous is None else advantage - previous
        age = record["max_snapshot_age_s"]
        if (record["net_worth_advantage"] != advantage
                or record["net_worth_advantage_change_120s"] != expected_change
                or (age is not None and (not isfinite(age) or age < 0))
                or (age is None and (record["radiant_players_observed"] or record["dire_players_observed"]))):
            raise DashboardDatabaseError("The team advantage mart contains inconsistent advantage values.")
        by_time[time] = advantage
        records.append(record)
    return records


def load_hero_matchups(config: WarehouseConfig, hero_id: int) -> list[dict]:
    """Read the selected hero's observed opponents, ordered by sample size."""
    if type(hero_id) is not int or not 0 < hero_id < 2**31:
        raise DashboardDatabaseError("Choose a valid hero to load opponent results.")
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT " + ", ".join(HERO_MATCHUP_COLUMNS)
            + " FROM marts.hero_matchup_summary WHERE hero_id = %s ORDER BY matches DESC, opponent_hero_id",
            (hero_id,),
        )
        rows = cursor.fetchall()
    matchups, opponents, contexts = [], set(), set()
    for row in rows:
        record = dict(zip(HERO_MATCHUP_COLUMNS, row, strict=True))
        opponent, matches, wins, losses, overall = (
            record[key] for key in ("opponent_hero_id", "matches", "wins", "losses", "overall_matches")
        )
        rate, baseline, difference = (record[key] for key in ("win_rate", "overall_win_rate", "win_rate_difference_pp"))
        if (record["hero_id"] != hero_id
                or any(type(value) is not int for value in (opponent, matches, wins, losses, overall))
                or not 0 < opponent < 2**31 or opponent == hero_id
                or not 0 < matches <= overall or not 0 <= wins <= matches or losses != matches - wins
                or any(value is None or not isfinite(value) for value in (rate, baseline, difference))
                or not 0 <= rate <= 1 or not 0 <= baseline <= 1
                or abs(rate - wins / matches) > 0.000001
                or abs(difference - 100 * (rate - baseline)) > 0.000001):
            raise DashboardDatabaseError("The hero matchup mart contains invalid counts or rates.")
        if opponent in opponents:
            raise DashboardDatabaseError("The hero matchup mart contains duplicate opponents.")
        opponents.add(opponent)
        contexts.add((overall, baseline))
        name = record.pop("opponent_name")
        record["opponent_label"] = name.strip() if isinstance(name, str) and name.strip() else f"Hero {opponent}"
        matchups.append(record)
    if len(contexts) > 1:
        raise DashboardDatabaseError("The hero matchup mart contains inconsistent overall context.")
    return matchups


def load_hero_performance(config: WarehouseConfig, hero_id: int) -> dict | None:
    """Read one selected hero's profile; missing rows are not fabricated as zeros."""
    if type(hero_id) is not int or not 0 < hero_id < 2**31:
        raise DashboardDatabaseError("Choose a valid hero to load its performance profile.")
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT " + ", ".join(HERO_PERFORMANCE_COLUMNS)
            + " FROM marts.hero_performance_summary WHERE hero_id = %s",
            (hero_id,),
        )
        rows = cursor.fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise DashboardDatabaseError("The hero performance mart contains duplicate hero IDs.")
    profile = dict(zip(HERO_PERFORMANCE_COLUMNS, rows[0], strict=True))
    appearances, wins, losses = (profile[key] for key in ("appearances", "wins", "losses"))
    rate, duration = profile["win_rate"], profile["avg_match_duration_s"]
    if (profile["hero_id"] != hero_id
            or any(type(value) is not int for value in (appearances, wins, losses))
            or appearances <= 0 or not 0 <= wins <= appearances or losses != appearances - wins
            or rate is None or not isfinite(rate) or not 0 <= rate <= 1
            or abs(rate - wins / appearances) > 0.000001
            or duration is None or not isfinite(duration) or duration < 900):
        raise DashboardDatabaseError("The hero performance mart contains invalid outcomes or duration.")
    for metric, count_column in HERO_PERFORMANCE_METRICS:
        average, count = profile[metric], profile[count_column]
        if (type(count) is not int or not 0 <= count <= appearances
                or (count == 0) != (average is None)
                or (average is not None and (not isfinite(average) or average < 0))):
            raise DashboardDatabaseError("The hero performance mart contains invalid metrics or observation counts.")
    return profile


def load_hero_statistics(config: WarehouseConfig) -> list[dict]:
    """Read one row per observed hero, retaining the mart's dataset-wide denominator."""
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT hero_id, localized_name, picks, wins, ranked_matches, pick_rate, win_rate
            FROM marts.hero_pick_win_rate
            ORDER BY picks DESC, hero_id
            """
        )
        rows = cursor.fetchall()

    heroes = []
    hero_ids = set()
    match_counts = set()
    for hero_id, name, picks, wins, total, pick_rate, win_rate in rows:
        # Counts must be usable before deriving losses or comparing sample sizes.
        valid_counts = all(type(value) is int for value in (hero_id, picks, wins, total))
        if not valid_counts or hero_id <= 0 or not 0 <= wins <= picks <= total or picks == 0:
            raise DashboardDatabaseError("The hero statistics mart contains invalid hero IDs or counts.")
        if hero_id in hero_ids:
            raise DashboardDatabaseError("The hero statistics mart contains duplicate hero IDs.")
        hero_ids.add(hero_id)
        match_counts.add(total)
        # Missing names keep their identity; an unobserved hero is never fabricated as a zero-rate row.
        label = name.strip() if isinstance(name, str) and name.strip() else f"Hero {hero_id}"
        heroes.append({
            "hero_id": hero_id, "hero_label": label,
            "picks": picks, "wins": wins, "losses": picks - wins, "ranked_matches": total,
            "pick_rate": pick_rate if pick_rate is not None and isfinite(pick_rate) and 0 <= pick_rate <= 1 else None,
            "win_rate": win_rate if win_rate is not None and isfinite(win_rate) and 0 <= win_rate <= 1 else None,
        })

    # Every row repeats the same match count. Summing it would multiply the dataset size by its heroes.
    if len(match_counts) > 1:
        raise DashboardDatabaseError("The hero statistics mart contains inconsistent match totals.")
    return heroes


def load_default_match_id(config: WarehouseConfig) -> int | None:
    """Select a deterministic default from the available scoreboard matches."""
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT min(match_id) FROM marts.match_scoreboard")
        return cursor.fetchone()[0]


def load_available_matches(config: WarehouseConfig) -> list[int]:
    """Return a small, deterministic set of match IDs already present in the scoreboard."""
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        # The scoreboard has one row per player; DISTINCT prevents repeated example buttons.
        cursor.execute(
            """
            SELECT DISTINCT match_id
            FROM marts.match_scoreboard
            ORDER BY match_id
            LIMIT 5
            """
        )
        return [row[0] for row in cursor.fetchall()]


def load_scoreboard(config: WarehouseConfig, match_id: int) -> tuple[dict, list[dict]]:
    """Return the match header and player rows from the same mart query."""
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        # Check that the progression mart is queryable without fetching any rows.
        cursor.execute("SELECT 1 FROM marts.player_timeseries LIMIT 0")
        # The left join keeps every scoreboard player, with NULL KP when no row matches.
        cursor.execute(
            """
            SELECT s.match_id, s.player_slot AS slot, s.hero_name AS hero, s.hero_localized_name AS hero_label, s.team,
                   s.kills, s.deaths, s.assists, s.last_hits, s.denies, s.net_worth,
                   s.unspent_gold AS gold, s.final_item_labels AS final_items, s.duration_s, s.won,
                   kp.kill_participation, CAST(kp.team_kills AS BIGINT) AS team_kills,
                   kp.kill_involvements
            FROM marts.match_scoreboard AS s
            LEFT JOIN marts.kill_participation AS kp
              ON kp.match_id = s.match_id AND kp.player_slot = s.player_slot
            WHERE s.match_id = %s
            ORDER BY s.player_slot
            """,
            (match_id,),
        )
        # Postgres SUM(bigint) returns numeric; team kill counts are exposed above as integers.
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

    players = []
    durations = set()
    winning_teams = set()
    losing_teams = set()
    for row in rows:
        player = dict(zip(columns, row))
        durations.add(player.pop("duration_s"))
        won = player.pop("won")
        if won is True:
            winning_teams.add(player["team"])
        elif won is False:
            losing_teams.add(player["team"])
        label = player["hero_label"]
        player["hero_label"] = (label.strip() if isinstance(label, str) and label.strip() else
                                player["hero"].removeprefix("npc_dota_hero_").replace("_", " ").title())
        final_items = player.pop("final_items")
        player["main_inventory"] = None
        if final_items is not None:
            player["main_inventory"] = [
                item.removeprefix("item_").replace("_", " ").title() if item.startswith("item_") else item
                for item in final_items if item
            ]
        players.append(player)

    # Match facts come from the duration and outcome values shared across the player rows.
    duration = durations.pop() if len(durations) == 1 else None
    winner = None
    if len(winning_teams) == 1 and winning_teams.isdisjoint(losing_teams):
        winner = winning_teams.pop()
    match = {
        "match_id": match_id,
        "duration_s": duration if duration is not None and isfinite(duration) and duration >= 0 else None,
        "winner_team": winner,
    }
    return match, players


def load_player_inventory(
    config: WarehouseConfig, match_id: int, player_slot: int, comparison_slot: int | None = None,
) -> dict[int, list[dict] | None]:
    """Load occupied final slots; absent rows mean unknown coverage, not an empty inventory."""
    inventory = {player_slot: None}
    if comparison_slot is not None:
        inventory[comparison_slot] = None
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT player_slot, inventory_slot, location, item_name, item_display_name
            FROM marts.match_inventory
            WHERE match_id = %s AND player_slot IN (%s, %s)
            ORDER BY player_slot, inventory_slot
            """,
            (match_id, player_slot, comparison_slot),
        )
        rows = cursor.fetchall()

    locations = ("main",) * 6 + ("backpack",) * 3 + ("stash",) * 6 + ("teleport", "neutral")
    seen = set()
    for slot, index, location, name, label in rows:
        if (type(slot) is not int or slot not in inventory
                or type(index) is not int or not 0 <= index <= 16
                or location != locations[index]
                or not isinstance(name, str) or not name.strip()
                or not isinstance(label, str) or not label.strip()):
            raise DashboardDatabaseError("The inventory mart contains invalid inventory records.")
        if (slot, index) in seen:
            raise DashboardDatabaseError("The inventory mart contains duplicate inventory slots.")
        seen.add((slot, index))
        if inventory[slot] is None:
            inventory[slot] = []
        inventory[slot].append({
            "inventory_slot": index,
            "location": location,
            "item_name": name,
            "item_display_name": label.strip(),
        })
    return inventory


def load_player_snapshots(
    config: WarehouseConfig, match_id: int, player_slot: int, comparison_slot: int | None,
) -> dict[int, list[dict]]:
    """Load ordered progression records for the selected slots in one match."""
    # With no comparison, the second SQL parameter is NULL and only the selected slot matches.
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT player_slot, game_time_s, net_worth, total_earned_xp
            FROM marts.player_timeseries
            WHERE match_id = %s AND player_slot IN (%s, %s)
            ORDER BY player_slot, game_time_s, tick
            """,
            (match_id, player_slot, comparison_slot),
        )
        rows = cursor.fetchall()

    snapshots = {}
    for slot, time, net_worth, total_xp in rows:
        snapshots.setdefault(slot, []).append({
            "game_time_s": time,
            "net_worth": net_worth,
            "total_xp": total_xp,
        })
    return snapshots


def load_player_damage(
    config: WarehouseConfig, match_id: int, player_slot: int, comparison_slot: int | None,
) -> dict[int, list[dict]]:
    """Load full-match damage rows for the selected attackers."""
    with open_postgres_database(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT attacker_slot, target_slot, target_team, inflictor_name,
                   total_damage, hits, share_of_team_damage
            FROM marts.damage_by_pair
            WHERE match_id = %s AND attacker_slot IN (%s, %s)
            ORDER BY attacker_slot, total_damage DESC, target_slot, inflictor_name
            """,
            (match_id, player_slot, comparison_slot),
        )
        rows = cursor.fetchall()

    damage = {}
    for attacker, target, team, source, total, hits, share in rows:
        # An undefined team share can be non-finite when the damage denominator is zero.
        damage.setdefault(attacker, []).append({
            "target_slot": target,
            "target_team": team,
            "source": source,
            "total_damage": total,
            "hits": hits,
            "share_of_team_damage": share if share is not None and isfinite(share) else None,
        })
    return damage
