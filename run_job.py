"""
Baseline Dota 2 replay job -- local stand-in for an Airflow DAG.

Each function = one future Airflow task:
  fetch_replay_url -> download_replay -> parse_replay

Setup (once):
  py -m pip install -r requirements.txt
  cd parser && ./gradlew installDist
Run:
  py run_job.py [--match MATCH_ID]

Needs Java 21. If `java` on PATH is older, set JAVA_BIN to a Java 21 executable.
"""

import argparse
import os
import subprocess
from pathlib import Path

import requests
import zstandard

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"      # downloaded replays (cache)
OUTPUT_DIR = ROOT / "output"  # parser results
PARSER_LIB = ROOT / "parser" / "build" / "install" / "parser" / "lib"

DEFAULT_MATCH = 9024732472


def fetch_replay_url(match_id: int) -> str:
    r = requests.get(f"https://api.opendota.com/api/matches/{match_id}", timeout=30)
    r.raise_for_status()
    url = r.json().get("replay_url")
    if not url:
        raise RuntimeError(f"no replay_url for match {match_id} (expired or not parsed yet)")
    return url


def download_replay(match_id: int, url: str) -> Path:
    dem = DATA_DIR / f"{match_id}.dem"
    if dem.exists():
        return dem
    DATA_DIR.mkdir(exist_ok=True)
    part = dem.with_suffix(".part")
    with requests.get(url, stream=True, timeout=60) as r, open(part, "wb") as f:
        r.raise_for_status()
        zstandard.ZstdDecompressor().copy_stream(r.raw, f)  # Valve serves zstd despite the .bz2 name
    part.rename(dem)
    return dem


def parse_replay(match_id: int, dem: Path) -> Path:
    out = OUTPUT_DIR / f"{match_id}.json"
    OUTPUT_DIR.mkdir(exist_ok=True)
    java = os.environ.get("JAVA_BIN", "java")
    subprocess.run([java, "-cp", str(PARSER_LIB / "*"), "ReplayInfo", str(dem), str(out)], check=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", type=int, default=DEFAULT_MATCH)
    match_id = ap.parse_args().match

    url = fetch_replay_url(match_id)
    dem = download_replay(match_id, url)
    out = parse_replay(match_id, dem)
    print(f"done: {out}")


if __name__ == "__main__":
    main()
