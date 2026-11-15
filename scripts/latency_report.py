"""
Event time vs processing time for the demo sessions (docs/latency-report.md).

  extract  one CSV per session: when each match happened and when each stage finished with it
  chart    event time vs processing time (ideal diagonal, one curve per session) and lag over time
  summary  per session and stage: count, p50/p95/max lag, max skew, throughput, backlog

Reads Garage (S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY), the warehouse (WAREHOUSE_*, host port
5433) and, with --airflow, the Airflow REST API. Nothing is written back to any of them.

  python3 scripts/latency_report.py extract --session burst1x --kind burst --run-id RUN_ID
  python3 scripts/latency_report.py extract --session live --kind live \\
      --since 2026-11-15T09:00:00Z --until 2026-11-15T10:00:00Z --airflow
  python3 scripts/latency_report.py chart --sessions live,burst1x,burst2x
  python3 scripts/latency_report.py summary --sessions live,burst1x,burst2x

Outputs go to --out-dir (default ~/Downloads/dota-latency, outside the repo).
"""

import argparse
import csv
import datetime as dt
import io
import json
import math
import os
import re
import sys
import textwrap
from pathlib import Path

UTC = dt.timezone.utc
DEFAULT_OUT = Path(os.environ.get("LATENCY_OUT", Path.home() / "Downloads" / "dota-latency"))
BUCKET = "raw"
COLD_PREFIX = "cold/"

STAGES = ("released_at", "arrived_at", "parsed_at", "cleaned_at", "loaded_at", "mart_visible_at")
LANE_STAGES = {
    ("burst", "replay"): STAGES,
    ("live", "replay"): STAGES[1:],
    ("live", "steam"): ("loaded_at", "mart_visible_at"),
}
# stage a lane's matches count as arrived at, for throughput in and backlog
ARRIVAL = {("burst", "replay"): "released_at", ("live", "replay"): "arrived_at", ("live", "steam"): "loaded_at"}
REJECTED_STAGES = ("cleaned_at", "loaded_at", "mart_visible_at")  # a rejected match never reaches these
TIME_COLUMNS = ("event_time", "sim_event_time", "origin", "queued_at", *STAGES,
                "parse_started_at", "clean_started_at", "load_started_at", "final_at")
COLUMNS = ("session", "kind", "lane", "match_id", "status", "rejected", "run_id", "speed", "rate_per_min",
           "spacing", "origin", "event_time", "sim_event_time", "queued_at", "released_at", "arrived_at",
           "parse_started_at", "parsed_at", "clean_started_at", "cleaned_at", "load_started_at", "loaded_at",
           "mart_visible_at", "final_stage", "final_at",
           *(f"lag_{s.removesuffix('_at')}_s" for s in STAGES), "lag_final_s")

# Airflow mapped tasks whose map label is the match id
TASKS = {
    "parse_pending": ("parse_and_check.parse_replay", "parse_started_at", "parsed_at"),
    "clean_pending": ("clean_and_check.spark_clean", "clean_started_at", "cleaned_at"),
    "load_pending": ("load_and_retain.load_one", "load_started_at", "loaded_at"),
}
DBT_DAG = "dbt_build"


# ---------- time helpers ----------

def to_utc(value) -> dt.datetime | None:
    """A tz-aware UTC datetime from a datetime (naive = UTC), ISO string or unix seconds."""
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, UTC)
    text = str(value).strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return dt.datetime.fromtimestamp(float(text), UTC)
    return to_utc(dt.datetime.fromisoformat(text.replace("Z", "+00:00")))


def iso(t: dt.datetime | None) -> str:
    return "" if t is None else t.astimezone(UTC).isoformat(timespec="milliseconds")


def seconds(a: dt.datetime | None, b: dt.datetime | None) -> float | None:
    """a - b in seconds, or None if either is missing."""
    return None if a is None or b is None else round((a - b).total_seconds(), 3)


# ---------- Garage: stage markers ----------

REPLAY_KEY = re.compile(r"replays/(\d+)\.dem$")
MARKER_KEY = re.compile(r"(parsed/_complete|clean/_complete|clean/_rejected|clean/_loaded)/match_id=(\d+)/(.+)$")
MARKER_PREFIXES = ("replays/", "parsed/_complete/", "clean/_complete/", "clean/_rejected/", "clean/_loaded/")


def stage_times(objects: list[dict]) -> tuple[dict[int, dict], dict[int, str]]:
    """From S3 listing entries ({Key, LastModified}) under MARKER_PREFIXES: per match id the
    arrived/parsed/cleaned/loaded times and the rejected flag, plus one parsed/_complete Parquet
    key per match to read parsed_at from."""
    times: dict[int, dict] = {}
    parquet: dict[int, str] = {}
    success: set[int] = set()
    for obj in objects:
        key, modified = obj["Key"], to_utc(obj["LastModified"])
        if m := REPLAY_KEY.fullmatch(key):
            times.setdefault(int(m.group(1)), {})["arrived_at"] = modified
            continue
        if not (m := MARKER_KEY.fullmatch(key)):
            continue
        where, match_id, rest = m.group(1), int(m.group(2)), m.group(3)
        t = times.setdefault(match_id, {})
        if where == "parsed/_complete":
            # Spark writes the part files last in its job; the newest one is when the match was done
            t["parsed_at"] = max(filter(None, (t.get("parsed_at"), modified)))
            if rest.endswith(".parquet"):
                parquet.setdefault(match_id, key)
        elif where == "clean/_complete":
            if rest == "_SUCCESS":
                t["cleaned_at"] = modified
                success.add(match_id)
            elif match_id not in success:
                t["cleaned_at"] = max(filter(None, (t.get("cleaned_at"), modified)))
        elif where == "clean/_rejected":
            t["rejected"], t["rejected_at"] = True, modified
        elif where == "clean/_loaded" and rest == "_loaded":
            t["loaded_at"] = modified
    return times, parquet


def parsed_at_from_parquet(body: bytes) -> dt.datetime | None:
    """The parsed_at column of a parsed/_complete part file (spark_jobs/parse_replays.py)."""
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(body))
    if "parsed_at" not in table.column_names:
        return None
    values = [v for v in table.column("parsed_at").to_pylist() if v is not None]
    return to_utc(max(values)) if values else None


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://127.0.0.1:3900"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def list_objects(client, bucket: str, prefix: str) -> list[dict]:
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        out += [{"Key": o["Key"], "LastModified": o.get("LastModified")} for o in page.get("Contents", [])]
    return out


def read_stage_times(client, bucket: str, keep, use_parquet: bool = True) -> dict[int, dict]:
    """stage_times for the matches where keep(match_id, times) is true, with parsed_at read from
    the Parquet column when use_parquet."""
    objects = [o for p in MARKER_PREFIXES for o in list_objects(client, bucket, p)]
    times, parquet = stage_times(objects)
    times = {k: v for k, v in times.items() if keep(k, v)}
    if use_parquet:
        for match_id, key in parquet.items():
            if match_id in times:
                body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
                if parsed := parsed_at_from_parquet(body):
                    times[match_id]["parsed_at"] = parsed
    return times


# ---------- Garage: restore log ----------

def restore_log_prefix(cold_prefix: str, run_id: str) -> str:
    cold_prefix = cold_prefix if cold_prefix.endswith("/") else cold_prefix + "/"
    return f"{cold_prefix}restore_log/{run_id}"


def parse_restore_log(objects: dict[str, bytes]) -> tuple[dict | None, list[dict]]:
    """The plan and per-match rows from {key: body} under restore_log/{run_id}/ (one _plan.json
    and one {match_id}.json each) or, from older runs, a single {run_id}.jsonl."""
    plan, rows = None, []
    for key, body in sorted(objects.items()):
        name = key.rsplit("/", 1)[-1]
        if name == "_plan.json":
            plan = json.loads(body)
        elif name.endswith(".jsonl"):
            rows += [json.loads(line) for line in body.decode().splitlines() if line.strip()]
        elif name.endswith(".json"):
            rows.append(json.loads(body))
    return plan, rows


def read_restore_log(client, bucket: str, cold_prefix: str, run_id: str) -> tuple[dict | None, list[dict]]:
    prefix = restore_log_prefix(cold_prefix, run_id)
    keys = [o["Key"] for o in list_objects(client, bucket, prefix + "/")]
    if not keys:
        keys = [o["Key"] for o in list_objects(client, bucket, prefix + ".jsonl")]
    if not keys:
        raise SystemExit(f"no restore log under s3://{bucket}/{prefix}/")
    return parse_restore_log({k: client.get_object(Bucket=bucket, Key=k)["Body"].read() for k in keys})


T0_KEYS = ("t0", "T0", "start_wall", "started_at", "start")


def burst_origin(plan: dict | None, rows: list[dict]) -> dt.datetime:
    """T0 of a restore run: from the plan, else the earliest scheduled_at - scheduled_offset_s."""
    for key in T0_KEYS:
        if plan and plan.get(key):
            return to_utc(plan[key])
    starts = [to_utc(r["scheduled_at"]) - dt.timedelta(seconds=float(r["scheduled_offset_s"]))
              for r in rows if r.get("scheduled_at") and r.get("scheduled_offset_s") is not None]
    if not starts:
        raise SystemExit("restore log has no T0 in _plan.json and no scheduled_at rows")
    return min(starts)


# ---------- Airflow ----------

def task_times(task_instances: list[dict]) -> dict[int, dict]:
    """Per match id, start/end of the latest successful parse, clean and load mapped task."""
    wanted = {task_id: (start, end) for task_id, start, end in TASKS.values()}
    out: dict[int, dict] = {}
    for ti in task_instances:
        label = str(ti.get("rendered_map_index") or "")
        if ti.get("task_id") not in wanted or ti.get("state") != "success" or not label.isdigit():
            continue
        start_col, end_col = wanted[ti["task_id"]]
        t = out.setdefault(int(label), {})
        end = to_utc(ti.get("end_date"))
        if end and (t.get(end_col) is None or end > t[end_col]):
            t[start_col], t[end_col] = to_utc(ti.get("start_date")), end
    return out


def mart_visible_at(loaded_at: dt.datetime | None, dbt_runs: list[dict]) -> dt.datetime | None:
    """End of the first successful dbt_build run that started after loaded_at."""
    if loaded_at is None:
        return None
    ends = [(to_utc(r["start_date"]), to_utc(r["end_date"])) for r in dbt_runs
            if r.get("state") == "success" and r.get("start_date") and r.get("end_date")]
    after = [(s, e) for s, e in ends if s >= loaded_at]
    return min(after)[1] if after else None


def airflow_session(base_url: str):
    """A requests session with a bearer token. AIRFLOW_API_USER / AIRFLOW_API_PASSWORD are posted
    to /auth/token when set; otherwise a plain GET, which the local stack's all-admins mode allows."""
    import requests

    session = requests.Session()
    user, password = os.environ.get("AIRFLOW_API_USER"), os.environ.get("AIRFLOW_API_PASSWORD")
    if user and password:
        r = session.post(f"{base_url}/auth/token", json={"username": user, "password": password}, timeout=10)
    else:
        r = session.get(f"{base_url}/auth/token", timeout=10)
    if r.ok and "access_token" in r.text:
        session.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    return session


def fetch_all(session, url: str, key: str, params: dict) -> list[dict]:
    items: list[dict] = []
    while True:
        r = session.get(url, params={**params, "limit": 100, "offset": len(items)}, timeout=30)
        r.raise_for_status()
        body = r.json()
        items += body[key]
        if not body[key] or len(items) >= body["total_entries"]:
            return items


def read_airflow(base_url: str, since: dt.datetime) -> tuple[dict[int, dict], list[dict]]:
    """(task_times, dbt_build runs) from the Airflow 3 REST API, for runs started after since."""
    api = f"{base_url.rstrip('/')}/api/v2"
    params = {"start_date_gte": iso(since)}
    with airflow_session(base_url.rstrip("/")) as session:
        tis = [ti for dag in TASKS
               for ti in fetch_all(session, f"{api}/dags/{dag}/dagRuns/~/taskInstances", "task_instances", params)]
        runs = fetch_all(session, f"{api}/dags/{DBT_DAG}/dagRuns", "dag_runs", params)
    return task_times(tis), runs


def maybe_airflow(enabled: bool, base_url: str, since: dt.datetime) -> tuple[dict[int, dict], list[dict]]:
    if not enabled:
        return {}, []
    try:
        return read_airflow(base_url, since)
    except Exception as e:  # best effort: the S3 times are still worth having
        print(f"warning: Airflow API unavailable ({type(e).__name__}: {e}); "
              "start times and mart_visible_at left empty", file=sys.stderr)
        return {}, []


# ---------- warehouse ----------

def warehouse_connect():
    import psycopg2

    if not os.environ.get("WAREHOUSE_PASSWORD"):
        raise SystemExit("WAREHOUSE_PASSWORD is not set")
    return psycopg2.connect(host=os.environ.get("WAREHOUSE_HOST", "127.0.0.1"),
                            port=os.environ.get("WAREHOUSE_PORT", "5433"),
                            user=os.environ.get("WAREHOUSE_USER", "warehouse"),
                            password=os.environ["WAREHOUSE_PASSWORD"],
                            dbname=os.environ.get("WAREHOUSE_DB", "warehouse"))


def query(conn, sql: str, params=()) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        names = [c[0] for c in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


def read_warehouse(conn, match_ids: list[int], since=None, until=None) -> dict:
    """End times for match_ids (clean.matches, else Steam start_time + duration), the download
    queue rows, and, given a window, the Steam rows loaded in it."""
    ids = list(match_ids) or [-1]
    out = {
        "end_times": {r["match_id"]: r["end_time"] for r in query(
            conn, "SELECT match_id, end_time FROM clean.matches WHERE match_id = ANY(%s)", (ids,))
            if r["end_time"] is not None},
        "steam_end_times": {r["match_id"]: r["end_time"] for r in query(
            conn, "SELECT match_id, start_time + duration AS end_time FROM stg.steam_matches "
                  "WHERE match_id = ANY(%s)", (ids,)) if r["end_time"] is not None},
        "queue": {r["match_id"]: r for r in query(
            conn, "SELECT match_id, added_at, done_at FROM ingest.download_queue WHERE match_id = ANY(%s)", (ids,))},
        "steam_rows": [],
    }
    if since is not None:
        out["steam_rows"] = query(
            conn, "SELECT match_id, start_time, duration, loaded_at, source_page FROM stg.steam_matches "
                  "WHERE loaded_at >= %s AND loaded_at <= %s",
            (since.replace(tzinfo=None), until.replace(tzinfo=None)))  # loaded_at is naive UTC
    return out


# ---------- rows ----------

def finish_row(row: dict) -> dict:
    """Fill the final stage and the per-stage lags (stage time - sim_event_time)."""
    for s in STAGES:
        row[f"lag_{s.removesuffix('_at')}_s"] = seconds(row.get(s), row.get("sim_event_time"))
    final = "mart_visible_at" if row.get("mart_visible_at") else "loaded_at" if row.get("loaded_at") else None
    row["final_stage"] = final.removesuffix("_at") if final else None
    row["final_at"] = row.get(final) if final else None
    row["lag_final_s"] = seconds(row["final_at"], row.get("sim_event_time"))
    return row


def drop_stale(row: dict, after: dt.datetime | None) -> dict:
    # a marker older than the match's arrival is left from an earlier run of the same match
    if after is not None:
        for s in ("parsed_at", "cleaned_at", "loaded_at", "mart_visible_at",
                  "parse_started_at", "clean_started_at", "load_started_at"):
            if row.get(s) is not None and row[s] < after:
                row[s] = None
    return row


def burst_rows(session: str, run_id: str, plan: dict | None, log_rows: list[dict], stages: dict[int, dict],
               tasks: dict[int, dict] | None = None, dbt_runs: list[dict] | None = None) -> list[dict]:
    """One row per match in a restore run. Event time is rebased: sim_event_time = T0 +
    scheduled_offset_s, when the schedule replays the match's end, so the ideal is y = x."""
    t0, tasks, dbt_runs = burst_origin(plan, log_rows), tasks or {}, dbt_runs or []
    rows = []
    for r in sorted(log_rows, key=lambda r: float(r.get("scheduled_offset_s") or 0)):
        match_id = int(r["match_id"])
        st, ta = stages.get(match_id, {}), tasks.get(match_id, {})
        released = to_utc(r.get("released_at")) if r.get("status") == "released" else None
        row = {
            "session": session, "kind": "burst", "lane": "replay", "match_id": match_id,
            "status": r.get("status"), "rejected": bool(st.get("rejected")),
            "run_id": r.get("run_id") or run_id,
            **{k: r.get(k, (plan or {}).get(k)) for k in ("speed", "rate_per_min", "spacing")},
            "origin": t0, "event_time": to_utc(r.get("end_time")),
            "sim_event_time": t0 + dt.timedelta(seconds=float(r.get("scheduled_offset_s") or 0)),
            "released_at": released,
            # Garage stamps a multipart object with the time its upload began, but the replay only
            # appears in replays/ when the upload completes, which is released_at
            "arrived_at": max(filter(None, (st.get("arrived_at"), released))) if released else None,
            **{k: ta.get(k) for k in ("parse_started_at", "clean_started_at", "load_started_at")},
            **{k: st.get(k) or ta.get(k) for k in ("parsed_at", "cleaned_at", "loaded_at")},
        }
        drop_stale(row, released or t0)
        row["mart_visible_at"] = mart_visible_at(row["loaded_at"], dbt_runs)
        rows.append(finish_row(row))
    return rows


def live_rows(session: str, since: dt.datetime, until: dt.datetime, stages: dict[int, dict], wh: dict,
              tasks: dict[int, dict] | None = None, dbt_runs: list[dict] | None = None) -> list[dict]:
    """Replay lane: matches whose replay landed in [since, until]; event = real match end.
    Steam lane: Steam rows loaded in the window; event = start_time + duration, processed = loaded_at."""
    tasks, dbt_runs = tasks or {}, dbt_runs or []
    rows = []
    for match_id, st in sorted(stages.items()):
        arrived = st.get("arrived_at")
        if arrived is None or not since <= arrived <= until:
            continue
        ta, queue = tasks.get(match_id, {}), wh.get("queue", {}).get(match_id, {})
        end = wh.get("end_times", {}).get(match_id) or wh.get("steam_end_times", {}).get(match_id)
        row = {
            "session": session, "kind": "live", "lane": "replay", "match_id": match_id,
            "status": "arrived", "rejected": bool(st.get("rejected")), "origin": since,
            "event_time": to_utc(end), "sim_event_time": to_utc(end),
            "queued_at": to_utc(queue.get("added_at")), "arrived_at": arrived,
            **{k: ta.get(k) for k in ("parse_started_at", "clean_started_at", "load_started_at")},
            **{k: st.get(k) or ta.get(k) for k in ("parsed_at", "cleaned_at", "loaded_at")},
        }
        drop_stale(row, arrived)
        row["mart_visible_at"] = mart_visible_at(row["loaded_at"], dbt_runs)
        rows.append(finish_row(row))
    for s in sorted(wh.get("steam_rows", []), key=lambda s: s["match_id"]):
        end = s["start_time"] + s["duration"] if s.get("start_time") is not None and s.get("duration") is not None else None
        loaded = to_utc(s.get("loaded_at"))
        row = {"session": session, "kind": "live", "lane": "steam", "match_id": int(s["match_id"]),
               "status": "loaded", "rejected": False, "origin": since,
               "event_time": to_utc(end), "sim_event_time": to_utc(end), "loaded_at": loaded,
               "mart_visible_at": mart_visible_at(loaded, dbt_runs)}
        rows.append(finish_row(row))
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({c: iso(v) if isinstance(v, dt.datetime) else "" if v is None else v
                        for c, v in ((c, row.get(c)) for c in COLUMNS)})


def read_csv(path: Path) -> list[dict]:
    rows = []
    with open(path, newline="") as f:
        for raw in csv.DictReader(f):
            row: dict = {}
            for c, v in raw.items():
                if v == "":
                    row[c] = None
                elif c in TIME_COLUMNS:
                    row[c] = to_utc(v)
                elif c == "match_id":
                    row[c] = int(v)
                elif c == "rejected":
                    row[c] = v == "True"
                elif c.startswith("lag_") or c in ("speed", "rate_per_min"):
                    row[c] = float(v)
                else:
                    row[c] = v
            rows.append(row)
    return rows


# ---------- maths ----------

def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile; None for no values."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(math.ceil(pct / 100 * len(ordered)) - 1, 0)]


def points(rows: list[dict], stage: str) -> list[tuple[float, float]]:
    """(x, y) on the rebased axes: x = sim_event_time - origin, y = stage time - origin, seconds."""
    out = []
    for r in rows:
        x, y = seconds(r.get("sim_event_time"), r.get("origin")), seconds(r.get(stage), r.get("origin"))
        if x is not None and y is not None:
            out.append((x, y))
    return out


def frontier(pts: list[tuple[float, float]], p: float) -> float | None:
    """Max event time among the events processed by processing time p."""
    done = [x for x, y in pts if y <= p]
    return max(done) if done else None


def skew_curve(pts: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """skew(p) = p - frontier(p), sampled just before and just after each processing instant
    (the frontier is a step function, so this is where the extremes are)."""
    curve, front = [], None
    for y in sorted({y for _, y in pts}):
        if front is not None:
            curve.append((y, round(y - front, 3)))
        front = max([x for x, yy in pts if yy == y] + ([front] if front is not None else []))
        curve.append((y, round(y - front, 3)))
    return curve


def max_skew(pts: list[tuple[float, float]]) -> float | None:
    curve = skew_curve(pts)
    return max(s for _, s in curve) if curve else None


def rate_per_min(times: list[dt.datetime]) -> float | None:
    """Matches a minute between the first and last time: (n - 1) gaps over the span."""
    times = sorted(t for t in times if t is not None)
    span = (times[-1] - times[0]).total_seconds() if len(times) > 1 else 0
    return round((len(times) - 1) * 60 / span, 3) if span > 0 else None


def summarize(rows: list[dict]) -> list[dict]:
    """One row per (session, lane, stage)."""
    out = []
    for session, kind, lane in dict.fromkeys((r["session"], r["kind"], r["lane"]) for r in rows):
        group = [r for r in rows if (r["session"], r["kind"], r["lane"]) == (session, kind, lane)]
        arrival = ARRIVAL[(kind, lane)]
        arrived = [r for r in group if r.get(arrival)]
        in_rate = rate_per_min([r[arrival] for r in arrived])
        planned = next(((r["rate_per_min"] or 0) * (r["speed"] or 0) for r in group
                        if r.get("rate_per_min") and r.get("speed")), None)
        for stage in LANE_STAGES[(kind, lane)]:
            done = [r for r in group if r.get(stage)]
            if stage == "mart_visible_at" and not done:
                continue  # no Airflow data
            lags = [l for r in done if (l := seconds(r[stage], r.get("sim_event_time"))) is not None]
            waiting = [r for r in arrived if not r.get(stage)
                       and not (stage in REJECTED_STAGES and r.get("rejected"))]
            out.append({
                "session": session, "kind": kind, "lane": lane, "stage": stage.removesuffix("_at"),
                "count": len(done), "p50_lag_s": percentile(lags, 50), "p95_lag_s": percentile(lags, 95),
                "max_lag_s": max(lags, default=None), "max_skew_s": max_skew(points(done, stage)),
                "out_per_min": rate_per_min([r[stage] for r in done]), "in_per_min": in_rate,
                "planned_per_min": planned, "backlog": len(waiting),
                "rejected": sum(bool(r.get("rejected")) for r in group),
            })
    return out


def fmt(v, unit: str = "") -> str:
    if v is None:
        return "–"
    if isinstance(v, float):
        if unit == "s" and abs(v) >= 3600:
            return f"{v / 3600:.1f}h"
        if unit == "s" and abs(v) >= 120:
            return f"{v / 60:.1f}m"
        return f"{v:.1f}{unit}"
    return str(v)


def markdown(summary: list[dict]) -> str:
    lines = ["| Session | Lane | Stage | Count | p50 lag | p95 lag | Max lag | Max skew | Out/min | In/min "
             "| Planned/min | Backlog | Rejected |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    lines += [f"| {s['session']} | {s['lane']} | {s['stage']} | {s['count']} | {fmt(s['p50_lag_s'], 's')} "
              f"| {fmt(s['p95_lag_s'], 's')} | {fmt(s['max_lag_s'], 's')} | {fmt(s['max_skew_s'], 's')} "
              f"| {fmt(s['out_per_min'])} | {fmt(s['in_per_min'])} | {fmt(s['planned_per_min'])} "
              f"| {s['backlog']} | {s['rejected']} |" for s in summary]
    return "\n".join(lines)


# ---------- chart ----------

COLORS = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6")
MARKERS = ("o", "s", "^", "D", "v", "P", "X")
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
EARLIER = ("parsed_at", "cleaned_at")
# top-panel axis unit per group: (seconds per unit, unit). Live replays sit about a day after the
# match ends, so seconds there run into six figures; hours read at a glance.
TOP_UNITS = {"burst": (1, "s"), "live": (3600, "h")}
# lag-panel unit per group. A burst's lag is minutes; in seconds the tick labels collapse to an
# offset such as "+3.559e4". Live stays in seconds on a log scale (replays a day, Steam minutes).
LAG_UNITS = {"burst": (60, "min"), "live": (1, "s")}
FOOTER = ("Final stage = mart visible (dbt) when known, else loaded. Faint: parsed, cleaned. "
          "Lag = vertical gap to the diagonal, skew = horizontal gap.")
FOOTER_CHARS_PER_COL = 100  # characters of 8 pt footer text that fit across one 7-inch column


def scaled(pts: list[tuple[float, float]], per: float) -> list[tuple[float, float]]:
    """Points with both coordinates divided by `per` seconds (a top-panel axis unit)."""
    return [(x / per, y / per) for x, y in pts]


def final_points(rows: list[dict], with_ids: bool = False) -> list[tuple]:
    """Rebased (x, y) of the final stage, sorted by event time; (x, y, match_id) with_ids."""
    pts = []
    for r in rows:
        x, y = seconds(r.get("sim_event_time"), r.get("origin")), seconds(r.get("final_at"), r.get("origin"))
        if x is not None and y is not None:
            pts.append((x, y, r["match_id"]))
    pts.sort()
    return pts if with_ids else [(x, y) for x, y, _ in pts]


def annotation(pts: list[tuple[float, float]], at: float | None = None) -> dict | None:
    """The example gaps drawn on the chart at processing time p (default: median processing
    time): the lag of the event processed nearest p, and the skew p - frontier(p)."""
    if not pts:
        return None
    ys = sorted(y for _, y in pts)
    p = at if at is not None else ys[(len(ys) - 1) // 2]
    x, y = min(pts, key=lambda q: (abs(q[1] - p), q[0]))
    front = frontier(pts, p)
    return {"p": p, "lag_x": x, "lag_y": y, "lag": round(y - x, 3), "frontier": front,
            "skew": None if front is None else round(p - front, 3)}


def chart_style(sessions: dict[str, list[dict]]) -> tuple[dict, list[str]]:
    style = {name: (COLORS[i % len(COLORS)], MARKERS[i % len(MARKERS)]) for i, name in enumerate(sessions)}
    groups = [g for g in ("burst", "live") if any(r["kind"] == g for rows in sessions.values() for r in rows)]
    return style, groups


def chart(sessions: dict[str, list[dict]], out: Path, annotate_at: float | None = None,
          html: bool = True) -> list[Path]:
    import matplotlib.pyplot as plt

    fig = chart_figure(sessions, annotate_at)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    written = [out]
    if html:
        try:
            written.append(chart_html(sessions, out.with_suffix(".html"), *chart_style(sessions)))
        except ImportError:
            pass
    return written


def chart_figure(sessions: dict[str, list[dict]], annotate_at: float | None = None):
    """The matplotlib figure: per group, event vs processing time on top, lag over time below."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import ScalarFormatter

    style, groups = chart_style(sessions)
    fig, axes = plt.subplots(2, len(groups), figsize=(7 * len(groups), 11), squeeze=False, facecolor=SURFACE)
    for col, group in enumerate(groups):
        ax, lag_ax = axes[0][col], axes[1][col]
        per, unit = TOP_UNITS[group]
        lag_per, lag_unit = LAG_UNITS[group]
        lo, hi, annotated = math.inf, -math.inf, False
        for name, rows in sessions.items():
            rows = [r for r in rows if r["kind"] == group]
            color, marker = style[name]
            for lane in dict.fromkeys(r["lane"] for r in rows):
                lane_rows = [r for r in rows if r["lane"] == lane]
                label = name if lane == "replay" else f"{name} ({lane})"
                lane_marker = marker if lane == "replay" else "x"
                for stage in EARLIER:
                    if pts := sorted(points(lane_rows, stage)):
                        ax.plot(*zip(*scaled(pts, per)), color=color, alpha=0.25, lw=1, marker=lane_marker, ms=4)
                pts = final_points(lane_rows)
                if not pts:
                    continue
                top = scaled(pts, per)
                ax.plot(*zip(*top), color=color, lw=2, marker=lane_marker, ms=6, label=label)
                lo, hi = min(lo, *(min(p) for p in top)), max(hi, *(max(p) for p in top))
                by_y = sorted(pts, key=lambda q: q[1])
                lag_ax.plot([y / lag_per for _, y in by_y], [(y - x) / lag_per for x, y in by_y], color=color,
                            lw=2, marker=lane_marker, ms=6, label=label)
                if not annotated and lane == "replay" and (a := annotation(pts, annotate_at)):
                    annotated = True
                    gap = dict(arrowstyle="<->", color=INK, lw=1.2)
                    lag_x, lag_y, p = a["lag_x"] / per, a["lag_y"] / per, a["p"] / per
                    ax.annotate("", xy=(lag_x, lag_y), xytext=(lag_x, lag_x), arrowprops=gap)
                    ax.text(lag_x, (lag_x + lag_y) / 2, f"  lag {fmt(a['lag'], 's')}",
                            color=INK, fontsize=9, va="center")
                    if a["frontier"] is not None and a["skew"] > 0:
                        front = a["frontier"] / per
                        ax.annotate("", xy=(front, p), xytext=(p, p), arrowprops=gap)
                        ax.text((front + p) / 2, p, f"skew {fmt(a['skew'], 's')}\n",
                                color=INK, fontsize=9, ha="center", va="bottom")
        if lo < hi:
            pad = (hi - lo) * 0.05
            ax.plot([lo - pad, hi + pad], [lo - pad, hi + pad], color=MUTED, ls="--", lw=1, label="ideal (y = x)")
        title = "Burst sessions" if group == "burst" else "Live session"
        start = "T0 (restore start)" if group == "burst" else "--since"
        ax.set_title(f"{title}: event time vs processing time", color=INK, loc="left")
        ax.set_xlabel(f"event time, {unit} since {start}"
                      + (" (scheduled replay of match end)" if group == "burst" else ""), color=MUTED)
        ax.set_ylabel(f"processing time, {unit} since {start}", color=MUTED)
        lag_ax.set_title(f"{title}: lag over time (final stage)", color=INK, loc="left")
        lag_ax.set_xlabel(f"processing time, {lag_unit} since {start}", color=MUTED)
        lag_ax.set_ylabel(f"lag, {lag_unit} (processing - event)", color=MUTED)
        if group == "live":
            lag_ax.set_yscale("log")  # replays lag a day, Steam pages minutes
        for a in (ax, lag_ax):
            for axis in (a.xaxis, a.yaxis):
                if isinstance(axis.get_major_formatter(), ScalarFormatter):
                    axis.get_major_formatter().set_useOffset(False)  # plain values, never "+3.559e4"
            a.set_facecolor(SURFACE)
            a.grid(color=GRID, lw=0.8)
            a.tick_params(colors=MUTED)
            for spine in a.spines.values():
                spine.set_color(GRID)
            if a.get_legend_handles_labels()[0]:
                a.legend(frameon=False, fontsize=9)
    # one column is too narrow for the footer on one line, so wrap it and leave room for every line
    footer = textwrap.wrap(FOOTER, FOOTER_CHARS_PER_COL * len(groups))
    fig.text(0.01, 0.005, "\n".join(footer), color=MUTED, fontsize=8, va="bottom")
    fig.tight_layout(rect=(0, 0.005 + 0.015 * len(footer), 1, 1))
    return fig


def chart_html(sessions, out: Path, style, groups) -> Path:
    html_figure(sessions, style, groups).write_html(out, include_plotlyjs="cdn")
    return out


def html_figure(sessions, style, groups):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    plotly_markers = {"o": "circle", "s": "square", "^": "triangle-up", "D": "diamond", "v": "triangle-down",
                      "P": "cross", "X": "x"}
    titles = [f"{g}: event vs processing" for g in groups] + [f"{g}: lag over time" for g in groups]
    fig = make_subplots(rows=2, cols=len(groups), subplot_titles=titles)
    for col, group in enumerate(groups, start=1):
        per, unit = TOP_UNITS[group]
        lo, hi = math.inf, -math.inf
        for name, rows in sessions.items():
            color, marker = style[name]
            for lane in dict.fromkeys(r["lane"] for r in rows if r["kind"] == group):
                lane_rows = [r for r in rows if r["kind"] == group and r["lane"] == lane]
                triples = final_points(lane_rows, with_ids=True)
                if not triples:
                    continue
                pts, ids = [(x, y) for x, y, _ in triples], [m for _, _, m in triples]
                label = name if lane == "replay" else f"{name} ({lane})"
                sym = plotly_markers[marker] if lane == "replay" else "x"
                top = scaled(pts, per)
                num = ".0f" if per == 1 else ".2f"
                fig.add_trace(go.Scatter(x=[p[0] for p in top], y=[p[1] for p in top], mode="lines+markers",
                                         name=label, legendgroup=label, text=ids,
                                         line=dict(color=color, width=2), marker=dict(symbol=sym),
                                         hovertemplate=f"match %{{text}}<br>event %{{x:{num}}}{unit}"
                                                       f"<br>processed %{{y:{num}}}{unit}"),
                              row=1, col=col)
                by_y = sorted(pts, key=lambda q: q[1])
                fig.add_trace(go.Scatter(x=[y for _, y in by_y], y=[y - x for x, y in by_y], mode="lines+markers",
                                         name=label, legendgroup=label, showlegend=False,
                                         line=dict(color=color, width=2), marker=dict(symbol=sym),
                                         hovertemplate="processed %{x:.0f}s<br>lag %{y:.0f}s"), row=2, col=col)
                lo, hi = min(lo, *(min(p) for p in top)), max(hi, *(max(p) for p in top))
        if lo < hi:
            fig.add_trace(go.Scatter(x=[lo, hi], y=[lo, hi], mode="lines", name="ideal (y = x)",
                                     showlegend=col == 1, line=dict(color=MUTED, dash="dash", width=1)),
                          row=1, col=col)
        fig.update_xaxes(title_text=f"event time, {unit}", row=1, col=col)
        fig.update_yaxes(title_text=f"processing time, {unit}", row=1, col=col)
        if group == "live":
            fig.update_yaxes(type="log", row=2, col=col)
    fig.update_layout(height=900, template="plotly_white")
    return fig


# ---------- CLI ----------

def load_sessions(out_dir: Path, names: list[str]) -> dict[str, list[dict]]:
    sessions = {}
    for name in names:
        path = out_dir / f"{name}.csv"
        if not path.exists():
            raise SystemExit(f"{path} not found; run extract --session {name} first")
        sessions[name] = read_csv(path)
    return sessions


def cmd_extract(args) -> None:
    client = s3_client()
    if args.kind == "burst":
        if not args.run_id:
            raise SystemExit("--run-id is required for a burst session")
        plan, log = read_restore_log(client, args.cold_bucket or args.bucket, args.cold_prefix, args.run_id)
        t0 = burst_origin(plan, log)
        ids = {int(r["match_id"]) for r in log}
        stages = read_stage_times(client, args.bucket, lambda m, _: m in ids, not args.no_parquet)
        tasks, runs = maybe_airflow(args.airflow, args.airflow_url, t0 - dt.timedelta(minutes=5))
        rows = burst_rows(args.session, args.run_id, plan, log, stages, tasks, runs)
    else:
        if not (args.since and args.until):
            raise SystemExit("--since and --until are required for a live session")
        since, until = to_utc(args.since), to_utc(args.until)
        stages = read_stage_times(client, args.bucket,
                                  lambda _, st: st.get("arrived_at") is not None and since <= st["arrived_at"] <= until,
                                  not args.no_parquet)
        ids = set(stages)
        conn = warehouse_connect()
        try:
            wh = read_warehouse(conn, sorted(ids), since, until)
        finally:
            conn.close()
        tasks, runs = maybe_airflow(args.airflow, args.airflow_url, since - dt.timedelta(minutes=5))
        rows = live_rows(args.session, since, until, stages, wh, tasks, runs)
    path = args.out_dir / f"{args.session}.csv"
    write_csv(rows, path)
    print(f"wrote {len(rows)} rows to {path}")


def cmd_chart(args) -> None:
    sessions = load_sessions(args.out_dir, args.sessions)
    out = args.out or args.out_dir / "chart.png"
    for path in chart(sessions, out, args.annotate_at, html=not args.no_html):
        print(f"wrote {path}")


def cmd_summary(args) -> None:
    sessions = load_sessions(args.out_dir, args.sessions)
    text = markdown(summarize([r for rows in sessions.values() for r in rows]))
    print(text)
    path = args.out_dir / "summary.md"
    path.write_text(text + "\n")
    print(f"\nwrote {path}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--out-dir", type=Path, default=DEFAULT_OUT, help=f"default {DEFAULT_OUT}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ex = sub.add_parser("extract", parents=[common], help="write {out-dir}/{session}.csv")
    ex.add_argument("--session", required=True)
    ex.add_argument("--kind", choices=("burst", "live"), required=True)
    ex.add_argument("--run-id", help="cold_restore run id (burst)")
    ex.add_argument("--since", help="UTC ISO start of a live session")
    ex.add_argument("--until", help="UTC ISO end of a live session")
    ex.add_argument("--bucket", default=BUCKET)
    ex.add_argument("--cold-bucket", help="bucket holding the restore log, default --bucket")
    ex.add_argument("--cold-prefix", default=COLD_PREFIX)
    ex.add_argument("--no-parquet", action="store_true", help="use LastModified for parsed_at, skip reading Parquet")
    ex.add_argument("--airflow", action="store_true",
                    help="also read task start times and dbt_build runs (mart_visible_at) from the Airflow API")
    ex.add_argument("--airflow-url", default=os.environ.get("AIRFLOW_API_URL", "http://127.0.0.1:8080"))
    ex.set_defaults(func=cmd_extract)

    for name, func, text in (("chart", cmd_chart, "write chart.png (and chart.html)"),
                             ("summary", cmd_summary, "print and write summary.md")):
        p = sub.add_parser(name, parents=[common], help=text)
        p.add_argument("--sessions", required=True, type=lambda s: [x for x in s.split(",") if x])
        p.set_defaults(func=func)
    ch = sub.choices["chart"]
    ch.add_argument("--out", type=Path, help="PNG path, default {out-dir}/chart.png")
    ch.add_argument("--annotate-at", type=float, help="processing time (s since start) for the example gaps")
    ch.add_argument("--no-html", action="store_true")

    args = ap.parse_args(argv)
    args.out_dir = args.out_dir.expanduser()
    args.func(args)


if __name__ == "__main__":
    main()
