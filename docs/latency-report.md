# Latency report

`scripts/latency_report.py` places a demo session on an event time vs processing time chart and
reports lag, skew and throughput. It only reads from Garage, the warehouse and (optionally) the
Airflow API.

## Definitions

- **Event time**: when the match really ended. For replays this is `end_time` from the replay
  header (`clean.matches`, or the restore log for a burst). For the Steam lane it is
  `start_time + duration` from `stg.steam_matches`.
- **Processing time**: when a stage finished with the match (see the table below).
- **Lag** of one match at one stage: `stage time - event time`. On the chart it is the vertical
  gap between the match's point and the diagonal.
- **Skew** at processing time `p`: `skew(p) = p - frontier(p)`, where `frontier(p)` is the latest
  event time among the matches already processed by `p`. On the chart it is the horizontal gap
  between the diagonal and the curve. It is a step function: it grows while nothing finishes and
  drops when a newer match finishes. Max skew is the largest value just before a match finishes,
  so it always includes at least one gap between arrivals (30 s at 2 a minute).
- **Rebasing.** Both axes are seconds since the session start.
  - Burst: the start is `T0`, when the restore run began (`t0` in `_plan.json`, else the earliest
    `scheduled_at - scheduled_offset_s`). A match's simulated event time is
    `T0 + scheduled_offset_s`, the moment the schedule replays its end. So `x = scheduled_offset_s`,
    `y = stage time - T0`, and the ideal is `y = x`. The real end time stays in `event_time`.
  - Live: the start is `--since`, and the event time is the real one. Replays arrive about a day
    after the match ends, so live points sit far left of the burst ones. The chart gives live its
    own panels and a log scale for lag.

## Sources per stage

| Column | Source |
|---|---|
| `released_at` | burst only: restore log row (`cold/restore_log/{run_id}/{match_id}.json`) |
| `arrived_at` | `LastModified` of `replays/{match_id}.dem`. Garage stamps a multipart object with the time its upload began, so for a burst it is raised to `released_at` when earlier; for live it is the upload start |
| `parsed_at` | `parsed_at` column of `parsed/_complete/match_id={id}/` Parquet; newest part file's `LastModified` with `--no-parquet` |
| `cleaned_at` | `LastModified` of `clean/_complete/match_id={id}/_SUCCESS` |
| `rejected` | any object under `clean/_rejected/match_id={id}/` |
| `loaded_at` | `LastModified` of `clean/_loaded/match_id={id}/_loaded` (`warehouse.loaded_marker_key`); Steam lane: `stg.steam_matches.loaded_at` |
| `mart_visible_at` | with `--airflow`: end of the first successful `dbt_build` run that started at or after `loaded_at` |
| `*_started_at` | with `--airflow`: start of the match's `parse_replay`, `spark_clean` and `load_one` task (map label = match id) |
| `queued_at` | live only: `ingest.download_queue.added_at` |

A marker older than the match's arrival (or `T0`) is left over from an earlier run and is
ignored. A missing stage is an empty cell. The final stage of a row is `mart_visible_at` when
known, else `loaded_at`.

## Running

From the repo root on the host, with the stack up. Set `S3_ENDPOINT` (default
`http://127.0.0.1:3900`), `S3_ACCESS_KEY`, `S3_SECRET_KEY` and, for live sessions, the
`WAREHOUSE_*` variables (host `127.0.0.1`, port `5433`). The Airflow API is read only with
`--airflow` (default `http://127.0.0.1:8080`, or `AIRFLOW_API_URL`). The local stack hands out a
token without a login; on a stack that needs one, set `AIRFLOW_API_USER` and
`AIRFLOW_API_PASSWORD`. If the API is down, extract warns and leaves those columns empty.

Outputs go to `--out-dir`, default `~/Downloads/dota-latency` (outside the repo, so nothing is
committed by accident).

```sh
# 1. live run: note the UTC start and end of the session
python3 scripts/latency_report.py extract --session live --kind live \
    --since 2026-11-15T09:00:00Z --until 2026-11-15T10:00:00Z --airflow

# 2. and 3. bursts: the run id of each cold_restore run (rate_per_min 2, speed 1 and 2)
python3 scripts/latency_report.py extract --session burst1x --kind burst --run-id 'manual__2026-11-15T10:00:00+00:00' --airflow
python3 scripts/latency_report.py extract --session burst2x --kind burst --run-id 'manual__2026-11-15T11:00:00+00:00' --airflow

python3 scripts/latency_report.py chart --sessions live,burst1x,burst2x     # chart.png and chart.html
python3 scripts/latency_report.py summary --sessions live,burst1x,burst2x   # prints and writes summary.md
```

Run extract after the pipeline has drained (dbt has run), or run it again later; it overwrites
`{session}.csv`. Burst extract also reads a single `restore_log/{run_id}.jsonl` from older runs.
`chart --annotate-at SECONDS` picks the processing time where the example gaps are drawn
(default: the median processing time). The gaps are drawn on the first session of each panel.

## Reading the chart

- Top row: event time (x) against processing time (y). The dashed line is the ideal, where every
  match is processed the instant it happens. The thick line per session is the final stage; the
  faint lines are parsed and cleaned. The further above the diagonal, the larger the lag.
  Burst axes are in seconds since T0; the live axes are in hours since `--since`, since live
  replays arrive about a day after the match.
- A curve that runs parallel to the diagonal means the pipeline keeps up: lag is constant. A
  curve that bends away means a backlog is building: lag grows with each match.
- Flat steps are batches: several matches finish at the same processing time (one dbt run makes
  a whole batch visible at once).
- Bottom row: lag of the final stage against processing time. Flat or saw-toothed means steady
  state; a rising trend means arrivals outpace the slowest stage.

## Summary columns

Per session, lane and stage: `count` of matches with that stage, p50/p95/max lag (nearest rank),
max skew, `out/min` (rate the stage finished matches: (n - 1) over the first-to-last span),
`in/min` (the same for arrivals: `released_at` for bursts, `arrived_at` for live replays,
`loaded_at` for Steam), `planned/min` (`rate_per_min * speed`), `backlog` (arrived but no time for
this stage at extraction; rejected matches do not count from cleaned on) and `rejected`.
