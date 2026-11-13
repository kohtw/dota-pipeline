"""Failure and retry log for every DAG: one JSON line per event in ALERT_LOG on the shared
dota-data volume, so problems across all runs can be read (or counted for the report) in one
place instead of opening task logs one by one. Failures also go to ALERT_WEBHOOK_URL when it is set.
Imports only the standard library and dota.errors, so DAG files can import it at parse time."""

import datetime as dt
import json
import os
import urllib.request
from pathlib import Path
from typing import Any, Mapping

from dota.errors import redact

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
        "error": redact(f"{type(error).__name__}: {error}")[:MAX_ERROR_CHARS] if error else None,
    }


def write(entry: dict, path: Path = ALERT_LOG) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def notify(entry: dict, url: str | None = None, post=urllib.request.urlopen) -> None:
    """Posts a failure to the Discord webhook in ALERT_WEBHOOK_URL; does nothing when it is unset."""
    if url is None:
        url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    try:
        match = f" [match {entry['match_id']}]" if entry.get("match_id") else ""
        content = f"{entry['dag_id']}.{entry['task_id']}{match} failed (try {entry['try_number']})"
        if entry.get("error"):
            content += f": {entry['error']}"
        request = urllib.request.Request(
            url, data=json.dumps({"content": content}).encode(), method="POST",
            # Discord rejects urllib's default User-Agent
            headers={"Content-Type": "application/json", "User-Agent": "dota-pipeline-alerts"})
        with post(request, timeout=5):
            pass
    except Exception as e:  # noqa: BLE001
        # the URL is a secret and exception text can contain it, so print only the type
        print(f"could not post alert webhook: {type(e).__name__}")


def _log(context: Mapping[str, Any], event: str) -> dict | None:
    # a callback that raises would hide the original failure, so this never raises
    try:
        entry = record(context, event)
    except Exception as e:  # noqa: BLE001
        print(f"could not build alert entry: {type(e).__name__}")
        return None
    try:
        write(entry)
        print(f"alert logged: {entry}")
    except Exception as e:  # noqa: BLE001
        print(f"could not write alert log: {e!r}")
    # returned even when the write failed, so the webhook still hears about the failure
    return entry


def on_failure(context: Mapping[str, Any]) -> None:
    entry = _log(context, "failed")
    if entry:
        notify(entry)


def on_retry(context: Mapping[str, Any]) -> None:
    _log(context, "retry")


# Spread into a DAG's default_args: default_args={**ALERT_CALLBACKS, ...}
ALERT_CALLBACKS = {"on_failure_callback": on_failure, "on_retry_callback": on_retry}
