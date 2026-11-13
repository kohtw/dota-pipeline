"""
Refresh the static reference seeds (dim_heroes, dim_items, dim_patches, dim_game_modes).
Heroes, items and patches come from Valve's dota2.com datafeed (no key needed); game modes
from OpenDota, because the datafeed has no list of them.

Run from the repo root after a patch, then review the diff and commit the CSVs:
  python3 scripts/refresh_constants.py
Nothing is written unless every request succeeds.
"""

import csv
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dags"))

from dota.constants import parse_game_modes, parse_heroes, parse_items, parse_patches  # noqa: E402

DATAFEED = "https://www.dota2.com/datafeed"
OPENDOTA = "https://api.opendota.com/api/constants"
SEEDS = REPO_ROOT / "dbt" / "seeds"
# herodata is one request per hero; the datafeed is a public website backend, so go slowly
PAUSE_S = 0.5


def get(url: str, **params) -> dict:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def datafeed(resource: str, **params) -> dict:
    return get(f"{DATAFEED}/{resource}", language="english", **params)


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write rows with a header, in the order given, with \\n line endings."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    herolist = datafeed("herolist")
    role_levels = {}
    for hero in herolist["result"]["data"]["heroes"]:
        time.sleep(PAUSE_S)
        data = datafeed("herodata", hero_id=hero["id"])
        role_levels[hero["id"]] = data["result"]["data"]["heroes"][0]["role_levels"]

    seeds = {
        "dim_heroes": parse_heroes(herolist, role_levels),
        "dim_items": parse_items(datafeed("itemlist")),
        "dim_patches": parse_patches(datafeed("patchnoteslist")),
        "dim_game_modes": parse_game_modes(get(f"{OPENDOTA}/game_mode")),
    }
    for name, rows in seeds.items():
        write_csv(SEEDS / f"{name}.csv", rows)
    print(", ".join(f"{name}: {len(rows)} rows" for name, rows in seeds.items()))


if __name__ == "__main__":
    main()
