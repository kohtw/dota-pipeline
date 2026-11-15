"""Plain constants, safe to import at DAG-parse time (no heavy dependencies)."""

import os
from datetime import timedelta
from pathlib import Path

BUCKET = "raw"  # Garage raw bucket
S3_ENV_VARS = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")
DOCKER_NETWORK = "dota-pipeline_default"  # compose network, so parser containers can reach garage:3900
# Warehouse DDL (*.sql, repeat-safe). Compose mounts ./warehouse/init here; dbt_build,
# steam_summary_elt and refresh_constants read it.
WAREHOUSE_INIT_DIR = Path(os.environ.get(
    "WAREHOUSE_INIT_DIR", Path(__file__).resolve().parents[2] / "warehouse" / "init"))
# Spark's default 1 GB driver heap plus headroom for the Python driver and the JVM's off-heap memory
JOB_MEM_LIMIT = "2g"


def env_int(name: str, default: int) -> int:
    """A positive integer from the environment, else the default."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    value = int(raw)
    if value < 1:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}")
    return value


# Per-run batch sizes and job caps. The defaults suit a laptop; a bigger machine raises them in
# .env, and the `spark` pool then sets how many parse and clean containers run at once.
PARSE_BATCH = env_int("PARSE_BATCH", 20)
PARALLEL_PARSES = env_int("PARALLEL_PARSES", 2)
CLEAN_BATCH = env_int("CLEAN_BATCH", 20)
PARALLEL_CLEANS = env_int("PARALLEL_CLEANS", 2)
LOAD_BATCH = env_int("LOAD_BATCH", 20)
# task retries for tasks that call Steam or OpenDota, on top of the clients' own retries within a call
API_RETRY = {"retries": 4, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": 2.0,
             "max_retry_delay": timedelta(minutes=30)}
