"""Loads the clean Parquet tables into Postgres schema `clean`, the step between the Spark
clean job and dbt (see docs/clean-contract.md). The load copies Parquet columns unchanged,
after checking the counter-credit fields. It never infers credited killers or default
labels from legacy data. Loading is idempotent per match: delete that match_id and insert
its rows in one transaction, so re-running a match replaces it and never doubles it.

Spark writes `clean/{table}/match_id={id}/part-*.parquet`, partitioned by match_id, so the
part files do NOT carry a match_id column: it lives in the path. This module injects it back
onto every row (clean.* has match_id NOT NULL).

psycopg2 and pyarrow are imported inside functions so DAG files can import this at parse time."""

import io
import os
import re
from pathlib import Path
from typing import Callable

from dota import clean_contract

# Loaded in this order (no foreign keys, so order is free). Must match warehouse/init and
# docs/clean-contract.md. A table with no part files for a match is cleared, not an error:
# whether every table must exist is the Spark clean job's contract, not the loader's.
CLEAN_TABLES = (
    "matches", "players", "player_stats", "positions", "damage_events",
    "deaths", "death_assists", "purchases", "final_inventory",
)
SCHEMA = "clean"
KEY = "match_id"
# Same env vars as dbt's prod target (dbt/profiles.yml), so one config feeds both.
ENV = {"host": "WAREHOUSE_HOST", "port": "WAREHOUSE_PORT", "user": "WAREHOUSE_USER",
       "password": "WAREHOUSE_PASSWORD", "dbname": "WAREHOUSE_DB"}
DEFAULTS = {"host": "localhost", "port": "5432", "user": "warehouse", "dbname": "warehouse"}


def clean_prefix(table: str, match_id: int) -> str:
    return f"clean/{table}/match_id={match_id}/"


CLEAN_COMPLETE = "clean/_complete"  # written by the clean job after all tables
LOADED = "clean/_loaded"  # written by the load DAG after a match's DB load commits


def loaded_marker_key(match_id: int) -> str:
    """Object key of the loaded marker (an empty object, so the prefix lists non-empty)."""
    return f"{LOADED}/match_id={match_id}/_loaded"


def _ids(keys: list[str], prefix: str) -> set[int]:
    return {int(m.group(1)) for k in keys if (m := re.match(rf"{re.escape(prefix)}/match_id=(\d+)/", k))}


def unloaded(clean_complete_keys: list[str], loaded_keys: list[str]) -> list[int]:
    """Match ids with a clean _complete marker but no _loaded marker, newest first. A load that
    died before writing its marker counts as unloaded and runs again (load_match is idempotent)."""
    return sorted(_ids(clean_complete_keys, CLEAN_COMPLETE) - _ids(loaded_keys, LOADED), reverse=True)


def delete_sql(table: str) -> str:
    return f"DELETE FROM {SCHEMA}.{table} WHERE {KEY} = %s"


def insert_sql(table: str, columns: list[str]) -> str:
    # columns are fixed clean-layer identifiers (no dots, unlike raw), so no quoting needed.
    return f"INSERT INTO {SCHEMA}.{table} ({', '.join(columns)}) VALUES %s"


def rows_from_parquet(table, match_id: int) -> tuple[list[str], list[tuple]]:
    """Turns one pyarrow Table into (columns, rows) ready to insert, with match_id injected as
    the first column of every row. Any match_id already in the file is dropped: the path is the
    source of truth. Returns column names in insert order and a list of row tuples."""
    data_cols = [c for c in table.column_names if c != KEY]
    columns = [KEY] + data_cols
    records = table.select(data_cols).to_pylist()  # Python natives; nulls become None
    rows = [(match_id, *[r[c] for c in data_cols]) for r in records]
    return columns, rows


def _read_table(keys: list[str], read: Callable[[str], bytes]):
    """All part files of one partition as one pyarrow Table."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    return pa.concat_tables([pq.read_table(io.BytesIO(read(k))) for k in keys])


def _execute_values(cur, sql: str, rows: list[tuple]) -> None:
    from psycopg2.extras import execute_values

    execute_values(cur, sql, rows)


def connect():
    """A psycopg2 connection to the warehouse, from the WAREHOUSE_* env vars."""
    params = {k: os.environ.get(var, DEFAULTS.get(k)) for k, var in ENV.items()}
    if params["password"] is None:
        raise RuntimeError(f"{ENV['password']} is not set")  # fail before importing the driver
    import psycopg2

    return psycopg2.connect(**params)


def load_match(match_id: int, read: Callable[[str], bytes],
               list_keys: Callable[[str], list[str]], conn, *, execute_values=_execute_values) -> dict[str, int]:
    """Loads one match's clean tables into Postgres in a single transaction. For each table:
    delete the match's rows, then insert whatever the Spark clean job wrote (nothing if it
    wrote no part file). All tables commit together or roll back together. Returns rows per
    table. `read`/`list_keys` are the storage functions; `conn` is a live DB connection."""
    cur = conn.cursor()
    try:
        counts: dict[str, int] = {}
        for table in CLEAN_TABLES:
            cur.execute(delete_sql(table), (match_id,))
            parts = [k for k in list_keys(clean_prefix(table, match_id)) if k.endswith(".parquet")]
            if not parts:
                counts[table] = 0  # cleared above; Spark wrote nothing for this table
                continue
            parquet = _read_table(parts, read)
            if table in {"matches", "deaths", "death_assists"}:
                # Never turn legacy last hitters into scoreboard credit or supply
                # invented default labels. Copy validated new fields unchanged.
                clean_contract.check_table(table, parquet, match_id)
            columns, rows = rows_from_parquet(parquet, match_id)
            if rows:
                execute_values(cur, insert_sql(table, columns), rows)
            counts[table] = len(rows)
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def init_sql_files(init_dir: Path) -> list[Path]:
    """The `*.sql` files in the warehouse init directory, in name order. Anything else is ignored."""
    if not init_dir.is_dir():
        raise FileNotFoundError(
            f"{init_dir} not found: mount ./warehouse/init at /opt/airflow/warehouse/init (docker-compose.yml)")
    return sorted(init_dir.glob("*.sql"), key=lambda p: p.name)


def apply_init_sql(init_dir: Path, conn) -> list[str]:
    """Runs every init SQL file in name order in one transaction, so the schemas and tables exist on
    a warehouse volume that predates them. Every file must be safe to repeat (IF NOT EXISTS).
    Returns the file names applied; on any error nothing is committed."""
    files = init_sql_files(init_dir)
    cur = conn.cursor()
    try:
        for path in files:
            cur.execute(path.read_text())
        conn.commit()
        return [p.name for p in files]
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
