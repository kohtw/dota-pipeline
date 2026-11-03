"""
Dota 2 replay DAG: fetch_replay_url -> download_replay -> parse_replay

fetch + download run as Python on the Airflow worker.
parse launches a short-lived dota-parser (JVM) container.
Both share the dota-data volume at /data.
"""

from datetime import datetime
from pathlib import Path

import requests
import zstandard
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag, task
from docker.types import Mount

DATA_DIR = Path("/data")
REPLAY_DIR = DATA_DIR / "replays"  # downloaded replays (cache)
OUTPUT_DIR = DATA_DIR / "output"   # parser results

DEFAULT_MATCH = 9024732472


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"match_id": Param(DEFAULT_MATCH, type="integer")},
)
def dota_replay():

    @task
    def fetch_replay_url(params=None) -> str:
        match_id = params["match_id"]
        r = requests.get(f"https://api.opendota.com/api/matches/{match_id}", timeout=30)
        r.raise_for_status()
        url = r.json().get("replay_url")
        if not url:
            raise RuntimeError(f"no replay_url for match {match_id} (expired or not parsed yet)")
        return url

    @task
    def download_replay(url: str, params=None) -> str:
        dem = REPLAY_DIR / f"{params['match_id']}.dem"
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if dem.exists():
            return str(dem)
        REPLAY_DIR.mkdir(parents=True, exist_ok=True)
        part = dem.with_suffix(".part")
        with requests.get(url, stream=True, timeout=60) as r, open(part, "wb") as f:
            r.raise_for_status()
            zstandard.ZstdDecompressor().copy_stream(r.raw, f)  # Valve serves zstd despite the .bz2 name
        part.rename(dem)
        return str(dem)

    dem = download_replay(fetch_replay_url())

    parse_replay = DockerOperator(
        task_id="parse_replay",
        image="dota-parser",
        command=[
            "{{ ti.xcom_pull(task_ids='download_replay') }}",
            f"{OUTPUT_DIR}/{{{{ params.match_id }}}}.json",
        ],
        docker_url="tcp://docker-proxy:2375",
        mounts=[Mount(source="dota-data", target="/data", type="volume")],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    dem >> parse_replay


dota_replay()
