"""Reference data history: snapshot round trip through a fake Garage, and the append-only diff load
on a DuckDB stand-in for the warehouse, using the real DDL. No network, no Docker."""

import copy
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dota import constants_load

duckdb = pytest.importorskip("duckdb")

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
DDL = (ROOT / "warehouse" / "init" / "06_constants_tables.sql").read_text()
T1 = datetime(2026, 11, 1, 3, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 11, 2, 3, 0, 0, tzinfo=timezone.utc)


def load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def raw_snapshot() -> dict:
    """The fixtures in the shape fetch_raw returns: herodata is one response per hero."""
    levels = load("datafeed_role_levels.json")
    return {
        "herolist": load("datafeed_herolist.json"),
        "herodata": {k: {"result": {"data": {"heroes": [{"id": int(k), "role_levels": v}]}}} for k, v in levels.items()},
        "itemlist": load("datafeed_itemlist.json"),
        "patchnoteslist": load("datafeed_patchnoteslist.json"),
        "game_mode": load("constants_game_mode.json"),
    }


def rename_hero(raw: dict, hero_id: int, name: str) -> dict:
    raw = copy.deepcopy(raw)
    hero = next(h for h in raw["herolist"]["result"]["data"]["heroes"] if h["id"] == hero_id)
    hero["name_loc"] = name
    return raw


def drop_item(raw: dict, item_id: int) -> dict:
    raw = copy.deepcopy(raw)
    items = raw["itemlist"]["result"]["data"]["itemabilities"]
    items[:] = [i for i in items if i["id"] != item_id]
    return raw


class FakeGarage:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: bytes = b"") -> None:
        self.objects[key] = body

    def read(self, key: str) -> bytes:
        return self.objects[key]


class DuckConn:
    """The slice of a psycopg2 connection the loader uses, over one DuckDB connection."""

    def __init__(self):
        self.con = duckdb.connect()
        self.in_tx = False

    def cursor(self):
        if not self.in_tx:
            self.con.execute("BEGIN TRANSACTION")
            self.in_tx = True
        return self

    def execute(self, sql, params=None):
        sql = sql.replace("%s", "?")
        return self.con.execute(sql) if params is None else self.con.execute(sql, params)

    def fetchall(self):
        return self.con.fetchall()

    def commit(self):
        self.con.commit()
        self.in_tx = False

    def rollback(self):
        self.con.rollback()
        self.in_tx = False

    def close(self):
        pass

    def rows(self, sql):
        return self.con.execute(sql).fetchall()


def duck_execute_values(cur, sql, rows):
    cur.con.executemany(sql.replace("VALUES %s", "VALUES (" + ", ".join("?" * len(rows[0])) + ")"), rows)


def load_raw(raw: dict, conn: DuckConn, at: datetime) -> dict:
    garage = FakeGarage()
    prefix = constants_load.save_snapshot(raw, at, garage.put)
    return constants_load.load_saved_snapshot(prefix, garage.read, conn, ddl=DDL,
                                              execute_values=duck_execute_values)


# snapshots


def test_snapshot_prefix_is_utc_basic_iso_and_round_trips():
    naive_utc = datetime(2026, 11, 1, 3)
    prefix = constants_load.snapshot_prefix(T1)
    assert prefix == "constants/fetched_at=20261101T030000Z/"
    assert constants_load.snapshot_time(prefix) == naive_utc


def test_snapshot_time_rejects_other_keys():
    with pytest.raises(ValueError, match="not a constants snapshot"):
        constants_load.snapshot_time("steam_raw/seq_1.json.gz")


def test_save_snapshot_writes_one_json_per_resource_and_reads_back_equal():
    garage, raw = FakeGarage(), raw_snapshot()
    prefix = constants_load.save_snapshot(raw, T1, garage.put)
    assert sorted(garage.objects) == sorted(f"{prefix}{r}.json.gz" for r in
                                            ("herolist", "herodata", "itemlist", "patchnoteslist", "game_mode"))
    assert constants_load.read_snapshot(prefix, garage.read) == raw


def test_save_snapshot_writes_nothing_when_a_feed_does_not_parse():
    garage, raw = FakeGarage(), raw_snapshot()
    raw["herodata"]["14"]["result"]["data"]["heroes"][0]["role_levels"].append(1)
    with pytest.raises(ValueError, match="hero 14"):
        constants_load.save_snapshot(raw, T1, garage.put)
    assert garage.objects == {}


def test_parse_snapshot_rejects_an_empty_feed():
    raw = raw_snapshot()
    raw["itemlist"]["result"]["data"]["itemabilities"] = []
    with pytest.raises(ValueError, match="items: the feed returned no rows"):
        constants_load.parse_snapshot(raw)


def test_parse_snapshot_rejects_a_repeated_key():
    raw = raw_snapshot()
    patches = raw["patchnoteslist"]["patches"]
    patches.append(dict(patches[0]))
    with pytest.raises(ValueError, match="repeats a patch"):
        constants_load.parse_snapshot(raw)


# diff


def test_normalize_stores_blanks_as_null_and_release_times_as_timestamps():
    items, patches = constants_load.TABLES["items"], constants_load.TABLES["patches"]
    assert constants_load.normalize({"item_id": 246, "name": "item_recipe_x", "localized_name": "",
                                     "is_recipe": True, "neutral_tier": None}, items) == (
        246, "item_recipe_x", None, True, None)
    assert constants_load.normalize({"patch": "7.41f", "released_at": "2026-09-15 07:00:00"}, patches) == (
        "7.41f", datetime(2026, 9, 15, 7))


HEROES = constants_load.TABLES["heroes"]
OD = {"hero_id": 76, "npc_name": "npc_dota_hero_obsidian_destroyer", "localized_name": "Outworld Destroyer",
      "primary_attr": "int", "roles": "Carry|Nuker|Disabler"}


def test_changed_rows_new_key_is_inserted():
    assert constants_load.changed_rows({}, [OD], HEROES) == [tuple(OD.values())]


def test_changed_rows_renamed_hero_is_a_new_version():
    latest = {76: tuple({**OD, "localized_name": "Outworld Devourer"}.values())}
    assert constants_load.changed_rows(latest, [OD], HEROES) == [tuple(OD.values())]


def test_changed_rows_unchanged_key_is_not_inserted():
    assert constants_load.changed_rows({76: tuple(OD.values())}, [OD], HEROES) == []


def test_changed_rows_ignores_a_key_the_feed_no_longer_lists():
    assert constants_load.changed_rows({76: tuple(OD.values()), 999: (999, "x", "y", "str", None)}, [OD], HEROES) == []


def test_latest_versions_takes_the_newest_row_per_key():
    old = (*{**OD, "localized_name": "Outworld Devourer"}.values(), datetime(2026, 1, 1))
    new = (*OD.values(), datetime(2026, 6, 1))
    assert constants_load.latest_versions([new, old]) == {76: tuple(OD.values())}


# load


def test_first_load_inserts_every_row_stamped_with_the_snapshot():
    conn = DuckConn()
    counts = load_raw(raw_snapshot(), conn, T1)
    assert counts == {"heroes": 6, "items": 4, "patches": 3, "game_modes": len(load("constants_game_mode.json"))}
    assert conn.rows("SELECT DISTINCT observed_at, source_snapshot FROM stg.constant_heroes") == [
        (datetime(2026, 11, 1, 3), "constants/fetched_at=20261101T030000Z/")]
    assert conn.rows("SELECT localized_name, is_recipe, neutral_tier FROM stg.constant_items WHERE item_id = 1") == [
        ("Blink Dagger", False, None)]


def test_rerunning_the_same_snapshot_inserts_nothing():
    conn = DuckConn()
    load_raw(raw_snapshot(), conn, T1)
    assert set(load_raw(raw_snapshot(), conn, T1).values()) == {0}
    assert conn.rows("SELECT count(*) FROM stg.constant_heroes") == [(6,)]


def test_a_later_snapshot_adds_only_the_changed_hero():
    conn = DuckConn()
    load_raw(rename_hero(raw_snapshot(), 76, "Outworld Devourer"), conn, T1)
    counts = load_raw(raw_snapshot(), conn, T2)
    assert counts == {"heroes": 1, "items": 0, "patches": 0, "game_modes": 0}
    assert conn.rows("SELECT localized_name, observed_at FROM stg.constant_heroes WHERE hero_id = 76 "
                     "ORDER BY observed_at") == [("Outworld Devourer", datetime(2026, 11, 1, 3)),
                                                 ("Outworld Destroyer", datetime(2026, 11, 2, 3))]


def test_an_item_missing_from_a_later_feed_keeps_its_last_version():
    conn = DuckConn()
    load_raw(raw_snapshot(), conn, T1)
    assert set(load_raw(drop_item(raw_snapshot(), 1), conn, T2).values()) == {0}
    assert conn.rows("SELECT count(*) FROM stg.constant_items WHERE item_id = 1") == [(1,)]


def test_rerunning_an_older_snapshot_after_a_newer_one_inserts_nothing():
    conn = DuckConn()
    load_raw(rename_hero(raw_snapshot(), 76, "Outworld Devourer"), conn, T1)
    load_raw(raw_snapshot(), conn, T2)
    assert set(load_raw(rename_hero(raw_snapshot(), 76, "Outworld Devourer"), conn, T1).values()) == {0}
    assert conn.rows("SELECT count(*) FROM stg.constant_heroes WHERE hero_id = 76") == [(2,)]


def test_a_failed_insert_rolls_back_the_whole_snapshot():
    conn = DuckConn()
    garage = FakeGarage()
    prefix = constants_load.save_snapshot(raw_snapshot(), T1, garage.put)

    def boom(cur, sql, rows):
        if "constant_patches" in sql:
            raise RuntimeError("db down")
        duck_execute_values(cur, sql, rows)

    with pytest.raises(RuntimeError, match="db down"):
        constants_load.load_saved_snapshot(prefix, garage.read, conn, ddl=DDL, execute_values=boom)
    conn.con.execute(DDL)
    assert conn.rows("SELECT count(*) FROM stg.constant_heroes") == [(0,)]


def test_insert_columns_match_the_ddl():
    con = duckdb.connect()
    con.execute(DDL)
    for table in constants_load.TABLES.values():
        schema, name = table.name.split(".")
        ddl_cols = [r[0] for r in con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE schema_name = ? AND table_name = ? "
            "ORDER BY column_index", [schema, name]).fetchall()]
        assert ddl_cols == [*table.columns, "observed_at", "source_snapshot"]


def test_dev_database_has_the_empty_history_tables(tmp_path):
    path = tmp_path / "dev.duckdb"
    subprocess.run([sys.executable, str(ROOT / "warehouse/make_dev_db.py"), str(path)], check=True)
    with duckdb.connect(str(path)) as con:
        for table in constants_load.TABLES.values():
            assert con.execute(f"SELECT count(*) FROM {table.name}").fetchone() == (0,)
