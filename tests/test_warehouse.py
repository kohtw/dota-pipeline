import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dota import warehouse

MATCH = 9079737909


def parquet_bytes(cols: dict) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    return buf.getvalue()


class FakeCursor:
    def __init__(self):
        self.executed: list[tuple[str, tuple]] = []
        self.closed = False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_clean_prefix_is_the_partition_path():
    assert warehouse.clean_prefix("positions", MATCH) == f"clean/positions/match_id={MATCH}/"


def test_delete_and_insert_sql():
    assert warehouse.delete_sql("matches") == "DELETE FROM clean.matches WHERE match_id = %s"
    assert warehouse.insert_sql("players", ["match_id", "player_slot", "team"]) == \
        "INSERT INTO clean.players (match_id, player_slot, team) VALUES %s"


def test_rows_from_parquet_injects_match_id_first_and_keeps_nulls():
    table = pa.table({"player_slot": [0, 5], "hero_name": ["axe", None]})
    columns, rows = warehouse.rows_from_parquet(table, MATCH)
    assert columns == ["match_id", "player_slot", "hero_name"]
    assert rows == [(MATCH, 0, "axe"), (MATCH, 5, None)]


def test_rows_from_parquet_drops_any_match_id_in_the_file_path_wins():
    # a file that happens to carry match_id (wrong value) must not override the partition
    table = pa.table({"match_id": [111, 222], "player_slot": [0, 1]})
    columns, rows = warehouse.rows_from_parquet(table, MATCH)
    assert columns == ["match_id", "player_slot"]
    assert rows == [(MATCH, 0), (MATCH, 1)]


def test_rows_from_parquet_empty_table_has_columns_but_no_rows():
    table = pa.table({"player_slot": pa.array([], type=pa.int32())})
    columns, rows = warehouse.rows_from_parquet(table, MATCH)
    assert columns == ["match_id", "player_slot"]
    assert rows == []


def test_load_match_deletes_every_table_inserts_present_ones_and_commits():
    present = {"matches": parquet_bytes({"game_mode": [22], "winner_team": [2]}),
               "players": parquet_bytes({"player_slot": [0, 1], "team": [2, 2]})}
    store = {warehouse.clean_prefix(t, MATCH) + "part-0.parquet": b for t, b in present.items()}

    def list_keys(prefix):
        return [k for k in store if k.startswith(prefix)]

    inserts = []
    conn = FakeConn()
    counts = warehouse.load_match(MATCH, store.__getitem__, list_keys, conn,
                                  execute_values=lambda cur, sql, rows: inserts.append((sql, rows)))

    deletes = [p for (s, p) in conn.cur.executed if s.startswith("DELETE")]
    assert len(deletes) == len(warehouse.CLEAN_TABLES)      # every table cleared
    assert all(p == (MATCH,) for p in deletes)
    assert {sql.split()[2] for sql, _ in inserts} == {"clean.matches", "clean.players"}  # only present ones
    assert counts["players"] == 2 and counts["matches"] == 1 and counts["positions"] == 0
    assert conn.commits == 1 and conn.rollbacks == 0 and conn.cur.closed


def test_load_match_rolls_back_and_reraises_on_a_failed_insert():
    store = {warehouse.clean_prefix("matches", MATCH) + "part-0.parquet":
             parquet_bytes({"game_mode": [22]})}

    def boom(cur, sql, rows):
        raise RuntimeError("insert failed")

    conn = FakeConn()
    with pytest.raises(RuntimeError, match="insert failed"):
        warehouse.load_match(MATCH, store.__getitem__, lambda p: [k for k in store if k.startswith(p)],
                             conn, execute_values=boom)
    assert conn.rollbacks == 1 and conn.commits == 0 and conn.cur.closed


def test_connect_requires_a_password(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="WAREHOUSE_PASSWORD"):
        warehouse.connect()
