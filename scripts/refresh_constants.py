"""
Refresh the reference data baseline: the dbt seeds baseline_heroes, baseline_items, baseline_patches
and baseline_game_modes. Heroes, items and patches come from Valve's dota2.com datafeed (no key
needed); game modes from OpenDota, because the datafeed has no list of them.

The warehouse itself is kept current, with history, by the refresh_constants DAG. The baseline is
the version every key has before that DAG first observed it, so the dims also work where the DAG
has never run (the dev DuckDB, a fresh warehouse). The dims treat the baseline as valid from the
beginning of time, so refreshing it rewrites the start of history: refresh it for new keys (a new
hero) rather than after every patch.

Run from the repo root, then review the diff and commit the CSVs:
  python3 scripts/refresh_constants.py
It uses the DAG's fetch and parse code. Nothing is written unless every request succeeds and parses.
"""

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dags"))

from dota.constants_load import fetch_raw, parse_snapshot  # noqa: E402

SEEDS = REPO_ROOT / "dbt" / "seeds"


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write rows with a header, in the order given, with \\n line endings."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    seeds = {f"baseline_{name}": rows for name, rows in parse_snapshot(fetch_raw()).items()}
    for name, rows in seeds.items():
        write_csv(SEEDS / f"{name}.csv", rows)
    print(", ".join(f"{name}: {len(rows)} rows" for name, rows in seeds.items()))


if __name__ == "__main__":
    main()
