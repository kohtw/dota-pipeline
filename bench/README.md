# VM throughput benchmark (archive)

This branch is a snapshot of a one-off benchmark run on 15 November 2026. It is not merged and is not production. The submitted pipeline is `master`. Results are in [RESULTS.md](RESULTS.md).

The test asked how fast the replay lane (download → parse → clean → load → dbt) runs, where it saturates, and how latency behaves below and above that point. It ran on a laptop and on a 32-vCPU cloud VM.

## What this branch adds on top of `master`

| Commit | Change | Why |
|---|---|---|
| Configurable batch sizes and caps | `PARSE_BATCH`, `CLEAN_BATCH`, `LOAD_BATCH`, `PARALLEL_PARSES`, `PARALLEL_CLEANS` can be set in `.env` (`dags/dota/config.py`). Unset, they keep the laptop defaults (20 per run, 2 jobs) | A 32-core machine needs more than 20 matches per run and more than 2 parallel jobs |
| `bench/monitor.sh` | Samples host CPU, memory, load, free disk and the `spark` pool (slots, running, waiting) every 15 s | Resource data for the charts and the auto-tuner |
| `bench/autotune.sh` | Adjusts the `spark` pool once a minute: +2 slots while jobs wait and CPU and memory are below 70%, −2 when either goes above 90% (CPU) or 85% (memory), with a 3-minute cooldown | Finds the slot count the machine can sustain |
| `bench/charts.py` | CPU and memory by container group (from `docker stats`), plus host metrics and the pool, with sessions shaded | Resource figures |
| dbt incremental patch | `stg_replay_player_observations` and `team_advantage_timeline` become incremental tables keyed on `match_id` | dbt became the bottleneck at ~300 matches (see RESULTS.md). The output is identical to a full rebuild. Limitation: a match reloaded after a re-clean needs `dbt build --full-refresh` |

## Setup

| | Laptop | VM |
|---|---|---|
| Machine | MacBook, Docker Desktop with 10 cores and 7.65 GiB | GCE n2-standard-32: 32 vCPU, 125 GiB, 250 GB disk |
| `spark` pool | 2 slots | 10 at start, auto-tuned to 16 |
| Batch and caps | defaults | `PARSE_BATCH=CLEAN_BATCH=LOAD_BATCH=1000`, `PARALLEL_PARSES=PARALLEL_CLEANS=32` |
| `dem_retention` | `keep` | `delete` |
| Other pools | `warehouse_write` 1 (unchanged) | same |

Each parse or clean job is its own container, capped at 2 GiB, and takes one `spark` slot.

## Method

Matches come from the cold store (see `docs/cold-storage.md` on `master`). Real replays are staged in Garage, and the `cold_restore` DAG releases them into `replays/` at a chosen rate. Each release is a match's event time; every later stage records its processing time. That lets us set the arrival rate exactly and measure, per match and per stage:

- **Lag:** processing time minus event time, for one match.
- **Skew:** how far the processed watermark (the latest event time whose matches have all finished a stage) trails the wall clock.

Sessions:

| Session | Input | Purpose |
|---|---|---|
| S1 | 300 matches released at once, auto-tuner on | Find the slot count and the throughput ceiling μ |
| S2b | 4.65 matches/min for 20 min (about 0.85 μ), pool fixed, dbt paused | Steady state below capacity |
| S3 | 7.67 matches/min for 15 min (about 1.4 μ), pool fixed, dbt paused until the end | Overload |

μ is measured on S1's final stable window: from 3 minutes after the auto-tuner's last change to the last load (147 loads in 26.8 minutes at 16 slots).

A first S2 was aborted, because a dbt build held the only `warehouse_write` slot for 36 minutes and no load could run. That finding and the patch for it are in RESULTS.md.

## Rerunning it

1. Start the stack with the batch and cap values above in `.env`, then set the pool: `airflow pools set spark 10 "Parser and Spark clean containers"`.
2. Stage replays in the cold store (see `docs/cold-storage.md`).
3. From the repo root, start `bench/monitor.sh`, `bench/autotune.sh` (S1 only) and a `docker stats` loop, all writing to `$OUT`.
4. Release a session: `airflow dags trigger cold_restore --conf '{"count": 300, "spacing": "uniform", "rate_per_min": 300}'`. Record `name,start_utc,end_utc` in `$OUT/sessions.csv`.
5. When it has drained:
   - Per-stage latency: `python scripts/latency_report.py extract --session S1 --kind burst --run-id <cold_restore run id> --airflow`, then `... chart --sessions S1`.
   - Resources: `python bench/charts.py --stats docker_stats.csv --monitor monitor.csv --sessions sessions.csv --cpus 32 --out resources.png`.

The summary data from this run is in `data/`, and the figures in `figures/`.
