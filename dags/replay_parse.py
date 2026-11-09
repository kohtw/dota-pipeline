"""
Parse one replay straight from Garage: the dota-parser container (PySpark, local mode) reads
s3a://raw/replays/{match_id}.dem and writes one Parquet dataset per table to
s3a://raw/parsed/{table}/match_id={match_id}/ (docs/data-contract.md).
"""

import os
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag

from dota.config import BUCKET, S3_ENV_VARS

MATCH = "{{ params.match_id }}"


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"match_id": Param(type="integer")},  # required
)
def replay_parse():
    DockerOperator(
        task_id="parse_replay",
        image="dota-parser",
        command=[f"s3a://{BUCKET}/replays/{MATCH}.dem", f"s3a://{BUCKET}/parsed"],
        docker_url="tcp://docker-proxy:2375",
        network_mode="dota-pipeline_default",  # so the parser can reach garage:3900
        private_environment={k: os.environ.get(k, "") for k in S3_ENV_VARS},  # hidden in the UI
        mount_tmp_dir=False,
        auto_remove="success",
    )


replay_parse()
