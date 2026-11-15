"""Reference data with history: heroes, items, patches and game modes. They change between patches
(a hero renamed, an item reworked), so the refresh_constants DAG keeps every version it observes.

A snapshot is one fetch of every source, saved raw to Garage under
constants/fetched_at={YYYYMMDDTHHMMSSZ}/{resource}.json.gz, gzipped like steam_raw/ (herodata is
one JSON object keyed by hero id, about 4 MB a day before gzip). Loading a snapshot parses it with
dota.constants and appends to stg.constant_* only the rows that differ from their key's latest
stored version at the snapshot's time, so each table holds one row per observed change and loading
the same snapshot again inserts nothing. dbt derives the validity windows (dim_hero_versions, ...)
from these rows on top of the seed baseline. See docs/constants-lane.md.

A key missing from a later feed is neither deleted nor flagged: its last version stays current,
because old replays still use it. Loading goes through a caller-supplied DB connection."""

import gzip
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from dota.constants import parse_game_modes, parse_heroes, parse_items, parse_patches
from dota.warehouse import _execute_values

PREFIX = "constants/"
RESOURCES = ("herolist", "herodata", "itemlist", "patchnoteslist", "game_mode")
_STAMP = "%Y%m%dT%H%M%SZ"  # ISO 8601 basic format: no colons in the Garage key
_SNAPSHOT = re.compile(r"^constants/fetched_at=(\d{8}T\d{6}Z)/$")


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[str, ...]  # the seed's columns in seed order; the first is the key

    @property
    def key(self) -> str:
        return self.columns[0]


TABLES = {
    "heroes": Table("stg.constant_heroes", ("hero_id", "npc_name", "localized_name", "primary_attr", "roles")),
    "items": Table("stg.constant_items", ("item_id", "name", "localized_name", "is_recipe", "neutral_tier")),
    "patches": Table("stg.constant_patches", ("patch", "released_at")),
    "game_modes": Table("stg.constant_game_modes", ("game_mode_id", "name", "label")),
}
TIMESTAMP_COLUMNS = {"released_at"}


def fetch_raw() -> dict:
    """Every source's raw response, keyed by RESOURCES. About 130 datafeed calls, paced."""
    from dota import datafeed, opendota

    return {**datafeed.fetch(), "game_mode": opendota.game_modes()}


def parse_snapshot(raw: dict) -> dict[str, list[dict]]:
    """Seed-shaped rows per table (keys of TABLES). Raises when a source changed shape, came back
    empty, or repeats a key."""
    role_levels = {int(k): v["result"]["data"]["heroes"][0]["role_levels"] for k, v in raw["herodata"].items()}
    rows = {
        "heroes": parse_heroes(raw["herolist"], role_levels),
        "items": parse_items(raw["itemlist"]),
        "patches": parse_patches(raw["patchnoteslist"]),
        "game_modes": parse_game_modes(raw["game_mode"]),
    }
    for name, table_rows in rows.items():
        if not table_rows:
            raise ValueError(f"{name}: the feed returned no rows")
        keys = [r[TABLES[name].key] for r in table_rows]
        if len(set(keys)) != len(keys):
            raise ValueError(f"{name}: the feed repeats a {TABLES[name].key}")
    return rows


def snapshot_prefix(fetched_at: datetime) -> str:
    return f"{PREFIX}fetched_at={fetched_at.astimezone(timezone.utc).strftime(_STAMP)}/"


def snapshot_time(prefix: str) -> datetime:
    """The fetch time in a snapshot prefix, as naive UTC (the column is TIMESTAMP, and an aware
    value would be shifted by the session time zone)."""
    m = _SNAPSHOT.match(prefix)
    if not m:
        raise ValueError(f"not a constants snapshot prefix: {prefix!r}")
    return datetime.strptime(m.group(1), _STAMP)


def save_snapshot(raw: dict, fetched_at: datetime, put: Callable[[str, bytes], None]) -> str:
    """Write each raw resource to the snapshot's prefix and return the prefix. Parses first, so a
    feed that no longer parses raises before anything is written."""
    parse_snapshot(raw)
    prefix = snapshot_prefix(fetched_at)
    for resource in RESOURCES:
        put(f"{prefix}{resource}.json.gz", gzip.compress(json.dumps(raw[resource]).encode()))
    return prefix


def read_snapshot(prefix: str, read: Callable[[str], bytes]) -> dict:
    return {resource: json.loads(gzip.decompress(read(f"{prefix}{resource}.json.gz"))) for resource in RESOURCES}


def normalize(row: dict, table: Table) -> tuple:
    """A parsed row as the tuple the database returns for it."""
    values = []
    for column in table.columns:
        value = row[column]
        if value == "":
            # the seed CSVs load a blank as NULL; storing the same keeps history equal to the baseline
            value = None
        if column in TIMESTAMP_COLUMNS and isinstance(value, str):
            value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        values.append(value)
    return tuple(values)


def latest_versions(history: list[tuple]) -> dict:
    """Key -> latest values, from stored rows of (table columns..., observed_at)."""
    latest = {}
    for *values, _ in sorted(history, key=lambda r: r[-1]):
        latest[values[0]] = tuple(values)
    return latest


def changed_rows(latest: dict, rows: list[dict], table: Table) -> list[tuple]:
    """The rows (normalized) whose key is new or whose values differ from its latest version."""
    return [values for values in (normalize(r, table) for r in rows) if latest.get(values[0]) != values]


def insert_sql(table: Table) -> str:
    return f"INSERT INTO {table.name} ({', '.join(table.columns)}, observed_at, source_snapshot) VALUES %s"


def load_snapshot(rows: dict[str, list[dict]], conn, *, observed_at: datetime, source_snapshot: str,
                  ddl: str, execute_values=_execute_values) -> dict[str, int]:
    """Appends the changed rows of one parsed snapshot in one transaction: run `ddl` (creates the
    tables), then per table compare against each key's latest version observed at or before
    `observed_at` and insert what differs. Comparing as of the snapshot's time keeps a rerun of the
    same or an older snapshot from adding rows. Returns rows inserted per table."""
    cur = conn.cursor()
    try:
        cur.execute(ddl)
        counts = {}
        for name, table in TABLES.items():
            cur.execute(f"SELECT {', '.join(table.columns)}, observed_at FROM {table.name} "
                        "WHERE observed_at <= %s", (observed_at,))
            new = changed_rows(latest_versions(cur.fetchall()), rows[name], table)
            if new:
                execute_values(cur, insert_sql(table), [v + (observed_at, source_snapshot) for v in new])
            counts[name] = len(new)
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def load_saved_snapshot(prefix: str, read: Callable[[str], bytes], conn, *, ddl: str,
                        execute_values=_execute_values) -> dict[str, int]:
    """Read a snapshot back from Garage, parse it and load it (see load_snapshot)."""
    rows = parse_snapshot(read_snapshot(prefix, read))
    return load_snapshot(rows, conn, observed_at=snapshot_time(prefix), source_snapshot=prefix,
                         ddl=ddl, execute_values=execute_values)
