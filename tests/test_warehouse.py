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


def test_loader_preserves_credit_separately_from_last_hitter_and_side_labels():
    present = {
        "deaths": {"tick": [80], "game_time_s": [1.0], "victim_slot": [5],
                   "killer_slot": pa.array([None], type=pa.int32()), "killer_name": ["tower"],
                   "credited_killer_slot": [0], "link_status": ["participant_assumed"]},
        "death_assists": {"tick": [80], "victim_slot": [5], "assist_slot": [6], "same_side": [True]},
    }
    objects = {warehouse.clean_prefix(name, MATCH) + "part-0.parquet": parquet_bytes(columns)
               for name, columns in present.items()}
    inserts = []
    conn = FakeConn()
    counts = warehouse.load_match(MATCH, objects.__getitem__,
        lambda p: [k for k in objects if k.startswith(p)], conn,
        execute_values=lambda cur, sql, rows: inserts.append((sql, rows)))
    assert counts["deaths"] == counts["death_assists"] == 1
    assert "credited_killer_slot, link_status" in inserts[0][0]
    assert inserts[0][1] == [(MATCH, 80, 1.0, 5, None, "tower", 0, "participant_assumed")]
    assert inserts[1][1] == [(MATCH, 80, 5, 6, True)]
    assert conn.commits == 1


def test_loader_rejects_legacy_death_files_and_rolls_back():
    objects = {warehouse.clean_prefix("deaths", MATCH) + "part-0.parquet": parquet_bytes({
        "tick": [80], "game_time_s": [1.0], "victim_slot": [5], "killer_slot": [0], "killer_name": ["hero"]})}
    conn = FakeConn()
    with pytest.raises(warehouse.clean_contract.CleanContractError, match="credited_killer_slot"):
        warehouse.load_match(MATCH, objects.__getitem__, lambda p: [k for k in objects if k.startswith(p)],
                             conn, execute_values=lambda *args: pytest.fail("Legacy credit must not insert"))
    assert conn.rollbacks == 1 and conn.commits == 0


def test_connect_requires_a_password(monkeypatch):
    monkeypatch.delenv("WAREHOUSE_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="WAREHOUSE_PASSWORD"):
        warehouse.connect()


def test_loaded_marker_key_lives_under_the_loaded_prefix():
    key = warehouse.loaded_marker_key(MATCH)
    assert key.startswith(f"clean/_loaded/match_id={MATCH}/")
    assert warehouse.unloaded([f"clean/_complete/match_id={MATCH}/_SUCCESS"], [key]) == []


def test_unloaded_is_complete_minus_loaded_newest_first():
    complete = [f"clean/_complete/match_id={i}/part-0.parquet" for i in (5, 9, 7)]
    complete.append("clean/_complete/match_id=9/part-1.parquet")  # duplicate id collapses
    loaded = [warehouse.loaded_marker_key(7)]
    assert warehouse.unloaded(complete, loaded) == [9, 5]


def test_unloaded_ignores_unrelated_keys_and_loaded_without_complete():
    complete = ["clean/_complete/match_id=3/x", "clean/matches/match_id=4/part.parquet", "junk"]
    loaded = ["clean/_loaded/match_id=8/_loaded", "clean/_complete/match_id=3x/y"]
    assert warehouse.unloaded(complete, loaded) == [3]
    assert warehouse.unloaded([], []) == []


def write_init(tmp_path, files: dict[str, str]):
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    return tmp_path


def test_init_sql_files_are_in_name_order_and_only_sql(tmp_path):
    write_init(tmp_path, {"02_b.sql": "B", "01_a.sql": "A", "10_c.sql": "C", "notes.md": "x", "03.sql.bak": "y"})
    assert [p.name for p in warehouse.init_sql_files(tmp_path)] == ["01_a.sql", "02_b.sql", "10_c.sql"]


def test_init_sql_files_raises_naming_a_missing_directory(tmp_path):
    missing = tmp_path / "nope"
    with pytest.raises(FileNotFoundError, match="nope"):
        warehouse.init_sql_files(missing)


def test_apply_init_sql_runs_every_file_in_order_in_one_transaction(tmp_path):
    write_init(tmp_path, {"02_b.sql": "SELECT 2;", "01_a.sql": "SELECT 1;", "readme.txt": "no"})
    conn = FakeConn()
    assert warehouse.apply_init_sql(tmp_path, conn) == ["01_a.sql", "02_b.sql"]
    assert [sql for sql, _ in conn.cur.executed] == ["SELECT 1;", "SELECT 2;"]
    assert conn.commits == 1 and conn.rollbacks == 0 and conn.cur.closed


def test_apply_init_sql_rolls_back_and_reraises_on_a_failed_statement(tmp_path):
    write_init(tmp_path, {"01_a.sql": "SELECT 1;", "02_b.sql": "BOOM"})
    conn = FakeConn()

    def execute(sql, params=None):
        if sql == "BOOM":
            raise RuntimeError("bad sql")
        conn.cur.executed.append((sql, params))

    conn.cur.execute = execute
    with pytest.raises(RuntimeError, match="bad sql"):
        warehouse.apply_init_sql(tmp_path, conn)
    assert conn.rollbacks == 1 and conn.commits == 0 and conn.cur.closed


def test_apply_init_sql_with_a_missing_directory_touches_nothing(tmp_path):
    conn = FakeConn()
    with pytest.raises(FileNotFoundError):
        warehouse.apply_init_sql(tmp_path / "nope", conn)
    assert conn.cur.executed == [] and conn.commits == 0
