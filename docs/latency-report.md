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
  gap between the match's dot and the diagonal.
- **Watermark** at processing time `p`: `watermark(p)`, the latest event time among the matches
  whose stage finished at or before `p`. It never goes back, so on the chart it is a step curve.
- **Skew** at processing time `p`: `skew(p) = p - watermark(p)`. On the chart it is the
  horizontal gap between the curve and the diagonal. It grows while nothing newer finishes and
  drops when a newer match finishes. Max skew is the largest value just before the watermark
  advances, so it always includes at least one gap between arrivals (30 s at 2 a minute).
- **Max lag vs the curve.** On a step curve the largest vertical gap equals the max skew. A match
  that finishes after a later one sits above the curve, so the max lag of single matches can be
  larger. Both maxima on the chart are the ones in `summary.md`.
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
`chart --annotate-at SECONDS` draws the lag and skew at that processing time instead of their
maxima. Each session gets its own arrows in its colour.

## Reading the chart

- Top row: event time (x) against processing time (y). The dashed diagonal is the ideal, where
  every match is processed the instant it happens. The thick line per session is the processed
  watermark of the final stage (mart visible when known, else loaded); the thin dashed line is the
  loaded watermark, so the gap between the two is the time dbt adds. Faint dots are single
  matches. Matches rejected by clean have no final stage and are left out; the legend counts them.
  Burst axes are in minutes since T0; the live axes are in hours since `--since`, since live
  replays arrive about a day after the match.
- The arrows mark the max lag (at its match) and the max skew (at its processing time). With
  `--annotate-at SECONDS` they show the lag and skew at that processing time instead.
- A curve that runs parallel to the diagonal means the pipeline keeps up: lag is constant. A
  curve that bends away means a backlog is building: lag grows with each match.
- A tall vertical stretch is a wait: nothing newer finished for that long. A long horizontal step
  is a batch: one dbt run makes many matches visible at once.
- Bottom row: lag of the final stage against processing time. Flat or saw-toothed means steady
  state; a rising trend means arrivals outpace the slowest stage.

## Loads and dbt never overlap

`load_pending` (`apply_schema`, `load_one`), `steam_summary_elt` (`load_pages`) and both
`dbt_build` tasks share the one-slot pool `warehouse_write`. dbt tests such as
`kda_matches_events` compare live `clean.*` tables with marts built earlier in the same run. When
a load committed mid-build, the test saw matches the marts did not have, and the build failed: 9
of them in the 60-match burst, each costing about 4.5 minutes of mart freshness.

The tradeoff: a load that becomes ready during a build waits for it, and loads run one at a time
(a load takes about 2 s), so `loaded` lag can rise by up to one build. In return every build sees
a consistent warehouse and none fails for it, so `mart_visible` lag should drop. Loads already
queued usually go first: `load_one` has a higher priority weight than `build` (Airflow's default
downstream rule), so a build tends to run between load batches rather than between single loads.

## Summary columns

Per session, lane and stage: `count` of matches with that stage, p50/p95/max lag (nearest rank),
max skew, `out/min` (rate the stage finished matches: (n - 1) over the first-to-last span),
`in/min` (the same for arrivals: `released_at` for bursts, `arrived_at` for live replays,
`loaded_at` for Steam), `planned/min` (`rate_per_min * speed`), `backlog` (arrived but no time for
this stage at extraction; rejected matches do not count from cleaned on) and `rejected`.
