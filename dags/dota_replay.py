"""
Dummy Dota 2 replay DAG: parse_replay only.

Runs the dota-parser container on a replay already in the dota-data volume
(/data/replays/{match_id}.dem) and writes its outputs to /data/output/.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag
from docker.types import Mount

DEFAULT_MATCH = 9024732472


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"match_id": Param(DEFAULT_MATCH, type="integer")},
)
def dota_replay():
    DockerOperator(
        task_id="parse_replay",
        image="dota-parser",
        command=[
            "/data/replays/{{ params.match_id }}.dem",
            "/data/output/{{ params.match_id }}.json",
        ],
        docker_url="tcp://docker-proxy:2375",
        mounts=[Mount(source="dota-data", target="/data", type="volume")],
        mount_tmp_dir=False,
        auto_remove="success",
    )


dota_replay()
