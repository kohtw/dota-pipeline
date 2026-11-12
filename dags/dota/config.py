"""Plain constants, safe to import at DAG-parse time (no heavy dependencies)."""

import os
from pathlib import Path

BUCKET = "raw"  # Garage raw bucket
S3_ENV_VARS = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")
DOCKER_NETWORK = "dota-pipeline_default"  # compose network, so parser containers can reach garage:3900
# Warehouse DDL (*.sql, repeat-safe). Compose mounts ./warehouse/init here; dbt_build and
# steam_summary_elt both read it.
WAREHOUSE_INIT_DIR = Path(os.environ.get(
    "WAREHOUSE_INIT_DIR", Path(__file__).resolve().parents[2] / "warehouse" / "init"))
