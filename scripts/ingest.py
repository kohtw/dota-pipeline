"""
Ingest one match into the raw layer:
  fetch_replay_url -> download_replay -> upload_replay   (raw/replays/{match_id}.dem)

Config via env (defaults work from the host against `docker compose up`):
  REPLAY_DIR     local replay cache        (default: <repo>/data)
  S3_ENDPOINT    S3 API                    (default: http://localhost:3900)
  S3_ACCESS_KEY / S3_SECRET_KEY            (default: dev creds from docker-compose.yml)

Run:
  py -m pip install -r scripts/requirements.txt
  py scripts/ingest.py [--match MATCH_ID]
"""

import argparse
import os
from pathlib import Path

import boto3
import requests
import zstandard
from botocore.config import Config

ROOT = Path(__file__).resolve().parent.parent
REPLAY_DIR = Path(os.environ.get("REPLAY_DIR", ROOT / "data" / "output"))

RAW_BUCKET = "raw"
DEFAULT_MATCH = 9024732472


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:3900"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "GK1a2b3c4d5e6f7a8b9c0d1e2f"),
        aws_secret_access_key=os.environ.get(
            "S3_SECRET_KEY", "9f8e7d6c5b4a39281706f5e4d3c2b1a09f8e7d6c5b4a39281706f5e4d3c2b1a0"),
        region_name="us-east-1",  # must match s3_region in garage.toml
        config=Config(s3={"addressing_style": "path"}),
    )


def fetch_replay_url(match_id: int) -> str:
    r = requests.get(f"https://api.opendota.com/api/matches/{match_id}", timeout=30)
    r.raise_for_status()
    url = r.json().get("replay_url")
    if not url:
        raise RuntimeError(f"no replay_url for match {match_id} (expired or not parsed yet)")
    return url


def download_replay(match_id: int, url: str) -> Path:
    dem = REPLAY_DIR / f"{match_id}.dem"
    if dem.exists():
        return dem
    REPLAY_DIR.mkdir(parents=True, exist_ok=True)
    part = dem.with_suffix(".part")
    with requests.get(url, stream=True, timeout=60) as r, open(part, "wb") as f:
        r.raise_for_status()
        zstandard.ZstdDecompressor().copy_stream(r.raw, f)  # Valve serves zstd despite the .bz2 name
    part.rename(dem)
    return dem


def upload_replay(match_id: int, dem: Path) -> str:
    key = f"replays/{match_id}.dem"
    s3_client().upload_file(str(dem), RAW_BUCKET, key)
    return f"{RAW_BUCKET}/{key}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", type=int, default=DEFAULT_MATCH)
    match_id = ap.parse_args().match

    dem = download_replay(match_id, fetch_replay_url(match_id))
    print(f"done: {upload_replay(match_id, dem)}")


if __name__ == "__main__":
    main()
