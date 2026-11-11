"""Load dashboard records from the local DuckDB marts."""

from contextlib import contextmanager
from math import isfinite
from pathlib import Path


class DashboardDatabaseError(Exception):
    """The configured dashboard database is unavailable or incomplete."""


@contextmanager
def _open_dashboard_database(database_path: str):
    """Open the database read-only and translate errors for the UI."""
    path = Path(database_path)
    if not path.is_file():
        raise DashboardDatabaseError(
            f"Database file not found at {path}. Build the fixture database or correct DUCKDB_PATH."
        )

    try:
        import duckdb
    except ImportError as error:
        raise DashboardDatabaseError(
            "Install DuckDB in the Python environment running Streamlit with `python -m pip install duckdb`."
        ) from error

    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            yield connection
    except duckdb.CatalogException as error:
        raise DashboardDatabaseError(
            "The database is missing dashboard marts. Run `dbt build --project-dir dbt --profiles-dir dbt` "
            "with DUCKDB_PATH pointing to this file."
        ) from error
    except duckdb.Error as error:
        raise DashboardDatabaseError(
            "Could not read the DuckDB database. Check DUCKDB_PATH and make sure dbt has finished building it."
        ) from error


def load_default_match_id(database_path: str) -> int | None:
    """Select a deterministic default from the available scoreboard matches."""
    with _open_dashboard_database(database_path) as connection:
        return connection.execute("SELECT min(match_id) FROM marts.match_scoreboard").fetchone()[0]


def load_scoreboard(database_path: str, match_id: int) -> tuple[dict, list[dict]]:
    """Return the match header and player rows from the same mart query."""
    with _open_dashboard_database(database_path) as connection:
        # Check that the progression mart is queryable without fetching any rows.
        connection.execute("SELECT 1 FROM marts.player_timeseries LIMIT 0")
        # The left join keeps every scoreboard player, with NULL KP when no row matches.
        result = connection.execute(
            """
            SELECT s.match_id, s.player_slot AS slot, s.hero_name AS hero, s.team,
                   s.kills, s.deaths, s.assists, s.last_hits, s.denies, s.net_worth,
                   s.unspent_gold AS gold, s.final_items, s.duration_s, s.won,
                   kp.kill_participation, kp.team_kills,
                   kp.kills_from_events + kp.assists_from_events AS kill_involvements
            FROM marts.match_scoreboard AS s
            LEFT JOIN marts.kill_participation AS kp
              ON kp.match_id = s.match_id AND kp.player_slot = s.player_slot
            WHERE s.match_id = ?
            ORDER BY s.player_slot
            """,
            [match_id],
        )
        columns = [column[0] for column in result.description]
        rows = result.fetchall()

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
        player["active_items"] = None
        if final_items is not None:
            player["active_items"] = [
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
    database_path: str, match_id: int, player_slot: int, comparison_slot: int | None,
) -> dict[int, list[dict]]:
    """Load ordered progression records for the selected slots in one match."""
    # With no comparison, the second SQL parameter is NULL and only the selected slot matches.
    with _open_dashboard_database(database_path) as connection:
        rows = connection.execute(
            """
            SELECT player_slot, game_time_s, net_worth, total_earned_xp
            FROM marts.player_timeseries
            WHERE match_id = ? AND player_slot IN (?, ?)
            ORDER BY player_slot, game_time_s, tick
            """,
            [match_id, player_slot, comparison_slot],
        ).fetchall()

    snapshots = {}
    for slot, time, net_worth, total_xp in rows:
        snapshots.setdefault(slot, []).append({
            "game_time_s": time,
            "net_worth": net_worth,
            "total_xp": total_xp,
        })
    return snapshots


def load_player_damage(
    database_path: str, match_id: int, player_slot: int, comparison_slot: int | None,
) -> dict[int, list[dict]]:
    """Load full-match damage rows for the selected attackers."""
    with _open_dashboard_database(database_path) as connection:
        rows = connection.execute(
            """
            SELECT attacker_slot, target_slot, target_team, inflictor_name,
                   total_damage, hits, share_of_team_damage
            FROM marts.damage_by_pair
            WHERE match_id = ? AND attacker_slot IN (?, ?)
            ORDER BY attacker_slot, total_damage DESC, target_slot, inflictor_name
            """,
            [match_id, player_slot, comparison_slot],
        ).fetchall()

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
