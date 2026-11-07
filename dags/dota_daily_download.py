"""
Daily at 00:00 SGT: download the latest 10 ranked All Pick matches' replays straight into
Garage (s3://raw/replays/{match_id}.dem), no local copy.

  get_latest_matches  -> Valve Web API GetMatchHistory (needs STEAM_API_KEY)
  download_replay x10 -> replay_url from OpenDota, replay from Valve, streamed to S3
"""

import os
from datetime import datetime, timedelta, timezone

import boto3
import requests
import zstandard
from airflow.sdk import dag, task
from botocore.config import Config
from botocore.exceptions import ClientError

N_MATCHES = 50
RANKED_LOBBY = 7  # lobby_type 7 = ranked matchmaking
GAME_MODE = 22  # All Pick (ranked/normal); excludes Turbo (23)
RAW_BUCKET = "raw"

STEAM_API = "https://api.steampowered.com/IDOTA2Match_570"
OPENDOTA_API = "https://api.opendota.com/api"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",  # must match s3_region in garage.toml
        config=Config(s3={"addressing_style": "path"}),
    )


@dag(
    schedule="0 16 * * *",  # 16:00 UTC = 00:00 SGT
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # a new run waits for the previous one to finish
)
def dota_daily_download():
    @task
    def get_latest_matches() -> list[int]:
        r = requests.get(
            f"{STEAM_API}/GetMatchHistory/v1/",
            params={"key": os.environ["STEAM_API_KEY"], "min_players": 10, "matches_requested": 100, "game_mode": GAME_MODE}, # recent All Pick matches
            timeout=30,
        )
        r.raise_for_status()
        matches = r.json()["result"]["matches"]
        ids = [m["match_id"] for m in matches if m["lobby_type"] == RANKED_LOBBY][:N_MATCHES]
        if not ids:
            raise RuntimeError("no ranked matches returned")
        return ids

    # OpenDota may not have the salt yet; on miss we ask it to fetch, then retry later
    @task(retries=5, retry_delay=timedelta(minutes=1))
    def download_replay(match_id: int) -> str:
        s3 = s3_client()
        key = f"replays/{match_id}.dem"
        try:
            s3.head_object(Bucket=RAW_BUCKET, Key=key)  # Demo already exists
            return f"{RAW_BUCKET}/{key}"
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                raise

        r = requests.get(f"{OPENDOTA_API}/matches/{match_id}", timeout=30) # Ask OpenDota for information
        r.raise_for_status()
        
        url = r.json().get("replay_url")  # built by OpenDota from cluster + replay_salt
        if not url:
            requests.post(f"{OPENDOTA_API}/request/{match_id}", timeout=30)
            raise RuntimeError(f"no replay_salt for {match_id} yet; requested OpenDota fetch")

        # multipart upload only becomes visible when complete, so no partial objects;
        # concurrent writers of the same match just overwrite with identical content
        with requests.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with zstandard.ZstdDecompressor().stream_reader(resp.raw) as dem:  # Valve serves zstd despite the .bz2 name
                s3.upload_fileobj(dem, RAW_BUCKET, key)
        return f"{RAW_BUCKET}/{key}"

    download_replay.expand(match_id=get_latest_matches())


dota_daily_download()
