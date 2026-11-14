"""Load hero summaries and match records from the Postgres dashboard marts."""

from math import isfinite

from config import WarehouseConfig
from postgres_connection import DashboardDatabaseError, open_postgres_database


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
            SELECT s.match_id, s.player_slot AS slot, s.hero_name AS hero, s.team,
                   s.kills, s.deaths, s.assists, s.last_hits, s.denies, s.net_worth,
                   s.unspent_gold AS gold, s.final_items, s.duration_s, s.won,
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
        player["hero_label"] = player["hero"].removeprefix("npc_dota_hero_").replace("_", " ").title()
        final_items = player.pop("final_items")
        player["main_inventory"] = None
        if final_items is not None:
            player["main_inventory"] = [
                item.removeprefix("item_").replace("_", " ").title()
                for item in final_items.split(",") if item
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
