"""Resource charts for a benchmark run: CPU and memory by container group (from docker stats), plus
host CPU, memory, free disk and the spark pool when bench/monitor.sh's CSV is given.

  python bench/charts.py --stats docker_stats.csv [--monitor monitor.csv] [--sessions sessions.csv]
      [--cpus 32] [--smooth 4] [--title "..."] --out resources.png

docker stats rows: ts,name,"1.2GiB / 7.6GiB",14.8%,98.6% (a header row is optional).
sessions.csv rows: name,start_utc,end_utc (end may be blank while a session runs); shaded on every panel.
"""
import argparse
import csv
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

UNITS = {"B": 1 / 2**30, "KiB": 1 / 2**20, "kB": 1e3 / 2**30, "MiB": 1 / 2**10, "MB": 1e6 / 2**30,
         "GiB": 1.0, "GB": 1e9 / 2**30, "TiB": 2**10}
GROUPS = ["job containers", "airflow", "airflow-db", "warehouse", "garage", "other"]
COLORS = {"job containers": "#d95f02", "airflow": "#1f77b4", "airflow-db": "#9ecae1",
          "warehouse": "#2ca02c", "garage": "#9467bd", "other": "#bbbbbb"}


def gib(text: str) -> float:
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    return float(m.group(1)) * UNITS.get(m.group(2), 0.0) if m else 0.0


def group(name: str) -> str:
    if not re.search(r"-\d+$", name) or "pipeline" not in name:
        return "job containers"  # parse and clean containers get random docker names
    for key in ("airflow-db", "warehouse", "garage"):
        if key in name:
            return key
    return "airflow" if "airflow" in name else "other"


def read_stats(path: Path) -> tuple[pd.DataFrame, float]:
    rows, total = [], 0.0
    with path.open() as f:
        for r in csv.reader(f):
            if len(r) < 5 or not r[0][:2].isdigit():
                continue
            used, _, limit = r[2].partition("/")
            total = max(total, gib(limit))
            rows.append((pd.Timestamp(r[0]), group(r[1]), gib(used), float(r[4].rstrip("%") or 0) / 100))
    df = pd.DataFrame(rows, columns=["ts", "group", "mem_gib", "cores"])
    df["ts"] = df["ts"].dt.round("15s")
    return df, total


def stacked(df: pd.DataFrame, col: str, smooth: int) -> pd.DataFrame:
    wide = df.pivot_table(index="ts", columns="group", values=col, aggfunc="sum").fillna(0)
    wide = wide.reindex(columns=[g for g in GROUPS if g in wide.columns])
    return wide.rolling(smooth, min_periods=1).mean()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", type=Path, required=True)
    ap.add_argument("--monitor", type=Path)
    ap.add_argument("--sessions", type=Path)
    ap.add_argument("--cpus", type=float, help="machine cores, for the CPU capacity line")
    ap.add_argument("--smooth", type=int, default=4, help="rolling mean over this many samples (4 = 1 min)")
    ap.add_argument("--title", default="Resource use during the run")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    stats, mem_total = read_stats(a.stats)
    mon = pd.read_csv(a.monitor, parse_dates=["ts"]) if a.monitor and a.monitor.exists() else None
    if mon is not None and mon.empty:
        mon = None
    sessions = []
    if a.sessions and a.sessions.exists():
        for r in csv.reader(a.sessions.open()):
            if len(r) >= 2 and r[1][:2].isdigit():
                sessions.append((r[0], pd.Timestamp(r[1]), pd.Timestamp(r[2]) if len(r) > 2 and r[2] else None))

    panels = ["cpu", "mem", "jobs"] + (["host", "disk"] if mon is not None else [])
    fig, axes = plt.subplots(len(panels), 1, figsize=(11, 2.6 * len(panels) + 0.6), sharex=True)
    end = stats["ts"].max()

    cpu = stacked(stats, "cores", a.smooth)
    axes[0].stackplot(cpu.index, cpu.T.values, labels=cpu.columns, colors=[COLORS[g] for g in cpu.columns], alpha=0.85)
    if a.cpus:
        axes[0].axhline(a.cpus, color="black", lw=1, ls="--", label=f"{a.cpus:g} cores")
    axes[0].set_ylabel("CPU (cores)")
    axes[0].legend(loc="upper left", fontsize=8, ncol=4)

    mem = stacked(stats, "mem_gib", a.smooth)
    axes[1].stackplot(mem.index, mem.T.values, labels=mem.columns, colors=[COLORS[g] for g in mem.columns], alpha=0.85)
    if mem_total:
        axes[1].axhline(mem_total, color="black", lw=1, ls="--", label=f"{mem_total:.1f} GiB available")
        axes[1].legend(loc="upper left", fontsize=8)
    axes[1].set_ylabel("Memory (GiB)")

    ax = axes[2]
    jobs = stats[stats["group"] == "job containers"].groupby("ts").size().reindex(cpu.index, fill_value=0)
    ax.step(jobs.index, jobs.values, where="post", color=COLORS["job containers"], label="job containers running")
    if mon is not None:
        ax.step(mon["ts"], mon["spark_slots"], where="post", color="black", lw=1, ls="--", label="spark pool slots")
        ax.step(mon["ts"], mon["spark_waiting"], where="post", color="#7f7f7f", lw=1, label="jobs waiting for a slot")
    ax.set_ylabel("Jobs")
    ax.legend(loc="upper left", fontsize=8)

    if mon is not None:
        axes[3].plot(mon["ts"], mon["cpu_busy_pct"], color="#d62728", label="host CPU busy %")
        axes[3].plot(mon["ts"], mon["mem_used_pct"], color="#1f77b4", label="host memory used %")
        axes[3].set_ylim(0, 100)
        axes[3].set_ylabel("Host %")
        axes[3].legend(loc="upper left", fontsize=8)
        axes[4].plot(mon["ts"], mon["disk_free_gb"], color="#8c564b")
        axes[4].set_ylabel("Free disk (GB)")
        end = max(end, mon["ts"].max())

    for name, start, stop in sessions:
        for ax in axes:
            ax.axvspan(start, stop or end, color="#f2f2f2", zorder=0)
        axes[0].annotate(name, (start, 1.0), xycoords=("data", "axes fraction"), va="top", fontsize=8)
    for ax in axes:
        ax.grid(alpha=0.3)
    axes[-1].set_xlabel("UTC")
    fig.suptitle(a.title)
    fig.tight_layout()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(a.out, dpi=130)
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
