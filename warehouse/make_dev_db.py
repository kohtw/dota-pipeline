"""Build a throwaway local DuckDB with the clean tables and the fixture match.
Usage: python warehouse/make_dev_db.py PATH.duckdb
Used for local dbt tests without Docker. Prod uses the same SQL files on Postgres.
"""
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).parent


def build(path: str) -> None:
    """Recreate the DuckDB file at `path` from warehouse/init/*.sql plus the fixture."""
    Path(path).unlink(missing_ok=True)  # always start clean so reruns are identical
    con = duckdb.connect(path)
    for f in sorted((HERE / "init").glob("*.sql")):
        con.execute(f.read_text())
    con.execute((HERE / "fixtures" / "sample_match.sql").read_text())
    con.close()


if __name__ == "__main__":
    build(sys.argv[1])
