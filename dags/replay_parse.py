"""
Parse one replay straight from Garage.

Runs the dota-parser container, which reads s3://raw/replays/{match_id}.dem and
uploads match.json, combatlog.csv, positions.csv, names.csv to
s3://raw/parsed/{match_id}/ (see docs/data-contract.md).
"""

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"match_id": Param(type="integer")},  # required; pass at trigger time
)
def replay_parse():
    DockerOperator(
        task_id="parse_replay",
        image="dota-parser",
        command=[
            "s3://raw/replays/{{ params.match_id }}.dem",
            "s3://raw/parsed/{{ params.match_id }}/",
        ],
        docker_url="tcp://docker-proxy:2375",
        network_mode="dota-pipeline_default",  # compose network, so the parser can reach garage:3900
        # not rendered in the UI
        private_environment={k: os.environ.get(k, "") for k in ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")},
        mount_tmp_dir=False,
        auto_remove="success",
    )


replay_parse()
