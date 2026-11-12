"""
Refresh the static reference seeds (dim_heroes, dim_game_modes) from OpenDota's constants.

Run from the repo root when a patch adds heroes or game modes, then commit the CSVs:
  python3 scripts/refresh_constants.py
"""

import csv
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dags"))

from dota.constants import parse_game_modes, parse_heroes  # noqa: E402

API = "https://api.opendota.com/api/constants"
SEEDS = REPO_ROOT / "dbt" / "seeds"


def fetch(resource: str) -> dict:
    """GET one constants resource as JSON."""
    r = requests.get(f"{API}/{resource}", timeout=30)
    r.raise_for_status()
    return r.json()


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write rows with a header, in the order given, with \\n line endings."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    heroes = parse_heroes(fetch("heroes"))
    game_modes = parse_game_modes(fetch("game_mode"))
    write_csv(SEEDS / "dim_heroes.csv", heroes)
    write_csv(SEEDS / "dim_game_modes.csv", game_modes)
    print(f"dim_heroes: {len(heroes)} rows, dim_game_modes: {len(game_modes)} rows")


if __name__ == "__main__":
    main()
