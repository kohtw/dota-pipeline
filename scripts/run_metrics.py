"""
Pipeline metrics for the report, from the Airflow REST API (the local stack needs no login).

  Run success:  share of finished runs that succeeded, per DAG, with scheduled runs shown
                separately (the proposal's target: 95% of scheduled runs over 7 days).
  Run time:     median, 95th percentile and max duration per DAG.
  Task time:    the same per task (mapped tasks pooled), plus failures and retried attempts.

Run from the repo root while the stack is up:
  python3 scripts/run_metrics.py                  # last 7 days, Markdown tables
  python3 scripts/run_metrics.py --days 1 --csv metrics.csv
"""

import argparse
import csv
import datetime as dt
import math
from dataclasses import asdict, dataclass

import requests

PAGE = 100


@dataclass
class RunStats:
    dag_id: str
    finished: int
    succeeded: int
    failed: int
    scheduled_finished: int
    scheduled_succeeded: int
    median_s: float | None
    p95_s: float | None
    max_s: float | None

    @property
    def success_rate(self) -> float | None:
        return self.succeeded / self.finished if self.finished else None

    @property
    def scheduled_success_rate(self) -> float | None:
        return self.scheduled_succeeded / self.scheduled_finished if self.scheduled_finished else None


@dataclass
class TaskStats:
    dag_id: str
    task_id: str
    runs: int
    failed: int
    retried: int
    median_s: float | None
    p95_s: float | None
    max_s: float | None


def percentile(values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile; None for no values."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(math.ceil(pct / 100 * len(ordered)) - 1, 0)]


def summarize_runs(runs: list[dict]) -> list[RunStats]:
    stats = []
    for dag_id in sorted({r["dag_id"] for r in runs}):
        done = [r for r in runs if r["dag_id"] == dag_id and r["state"] in ("success", "failed")]
        scheduled = [r for r in done if r["run_type"] == "scheduled"]
        durations = [r["duration"] for r in done if r.get("duration") is not None]
        stats.append(RunStats(
            dag_id=dag_id,
            finished=len(done),
            succeeded=sum(r["state"] == "success" for r in done),
            failed=sum(r["state"] == "failed" for r in done),
            scheduled_finished=len(scheduled),
            scheduled_succeeded=sum(r["state"] == "success" for r in scheduled),
            median_s=percentile(durations, 50), p95_s=percentile(durations, 95), max_s=max(durations, default=None),
        ))
    return stats


def summarize_tasks(task_instances: list[dict]) -> list[TaskStats]:
    stats = []
    for dag_id, task_id in sorted({(t["dag_id"], t["task_id"]) for t in task_instances}):
        tis = [t for t in task_instances if t["dag_id"] == dag_id and t["task_id"] == task_id
               and t["state"] not in ("skipped", "removed", None)]
        if not tis:
            continue
        durations = [t["duration"] for t in tis if t.get("duration") is not None]
        stats.append(TaskStats(
            dag_id=dag_id, task_id=task_id, runs=len(tis),
            failed=sum(t["state"] in ("failed", "upstream_failed") for t in tis),
            retried=sum(max(t.get("try_number") or 1, 1) - 1 for t in tis),
            median_s=percentile(durations, 50), p95_s=percentile(durations, 95), max_s=max(durations, default=None),
        ))
    return stats


def fetch_all(session: requests.Session, url: str, key: str, params: dict) -> list[dict]:
    items: list[dict] = []
    while True:
        r = session.get(url, params={**params, "limit": PAGE, "offset": len(items)}, timeout=30)
        r.raise_for_status()
        body = r.json()
        items += body[key]
        if not body[key] or len(items) >= body["total_entries"]:
            return items


def fmt_s(seconds: float | None) -> str:
    return "–" if seconds is None else f"{seconds:.1f}s" if seconds < 120 else f"{seconds / 60:.1f}m"


def fmt_pct(rate: float | None) -> str:
    return "–" if rate is None else f"{rate:.1%}"


def markdown(runs: list[RunStats], tasks: list[TaskStats], since: dt.datetime) -> str:
    lines = [f"Since {since:%Y-%m-%d %H:%M} UTC", "",
             "| DAG | Finished runs | Success | Scheduled success | Median | p95 | Max |",
             "|---|---|---|---|---|---|---|"]
    lines += [f"| {s.dag_id} | {s.finished} | {fmt_pct(s.success_rate)} | "
              f"{fmt_pct(s.scheduled_success_rate)} ({s.scheduled_succeeded}/{s.scheduled_finished}) | "
              f"{fmt_s(s.median_s)} | {fmt_s(s.p95_s)} | {fmt_s(s.max_s)} |" for s in runs]
    lines += ["", "| DAG | Task | Runs | Failed | Retries | Median | p95 | Max |",
              "|---|---|---|---|---|---|---|---|"]
    lines += [f"| {t.dag_id} | {t.task_id} | {t.runs} | {t.failed} | {t.retried} | "
              f"{fmt_s(t.median_s)} | {fmt_s(t.p95_s)} | {fmt_s(t.max_s)} |" for t in tasks]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=7)
    ap.add_argument("--base-url", default="http://127.0.0.1:8080")
    ap.add_argument("--csv", help="also write per-task rows to this CSV file")
    args = ap.parse_args()

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
    api = f"{args.base_url}/api/v2"
    with requests.Session() as session:
        token = session.get(f"{args.base_url}/auth/token", timeout=10)
        if token.ok and "access_token" in token.text:
            session.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        filters = {"start_date_gte": since.isoformat()}
        runs = fetch_all(session, f"{api}/dags/~/dagRuns", "dag_runs", filters)
        tis = fetch_all(session, f"{api}/dags/~/dagRuns/~/taskInstances", "task_instances", filters)

    run_stats, task_stats = summarize_runs(runs), summarize_tasks(tis)
    print(markdown(run_stats, task_stats, since))
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(task_stats[0]).keys()) if task_stats else ["dag_id"])
            writer.writeheader()
            writer.writerows(asdict(t) for t in task_stats)
        print(f"\nwrote {len(task_stats)} task rows to {args.csv}")


if __name__ == "__main__":
    main()
