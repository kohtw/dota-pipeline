"""Failure and retry log for every DAG: one JSON line per event in ALERT_LOG on the shared
dota-data volume, so problems across all runs can be read (or counted for the report) in one
place instead of opening task logs one by one. Standard library only, so DAG files can import it
at parse time."""

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Mapping

ALERT_LOG = Path(os.environ.get("ALERT_LOG", "/data/alerts/failures.jsonl"))
MAX_ERROR_CHARS = 500


def record(context: Mapping[str, Any], event: str) -> dict:
    """Builds one log entry from an Airflow callback context."""
    ti = context["ti"]
    params = context.get("params") or {}
    error = context.get("exception")
    label = getattr(ti, "rendered_map_index", None)  # mapped tasks label themselves with their match id
    match_id = params.get("match_id") or (int(label) if label and str(label).isdigit() else None)
    return {
        "time": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "event": event,
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "map_index": ti.map_index,
        "try_number": ti.try_number,
        "match_id": match_id,
        "error": f"{type(error).__name__}: {error}"[:MAX_ERROR_CHARS] if error else None,
    }


def write(entry: dict, path: Path = ALERT_LOG) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def _log(context: Mapping[str, Any], event: str) -> None:
    # a callback that raises would hide the original failure, so this never raises
    try:
        entry = record(context, event)
        write(entry)
        print(f"alert logged: {entry}")
    except Exception as e:  # noqa: BLE001
        print(f"could not write alert log: {e!r}")


def on_failure(context: Mapping[str, Any]) -> None:
    _log(context, "failed")


def on_retry(context: Mapping[str, Any]) -> None:
    _log(context, "retry")


# Spread into a DAG's default_args: default_args={**ALERT_CALLBACKS, ...}
ALERT_CALLBACKS = {"on_failure_callback": on_failure, "on_retry_callback": on_retry}
