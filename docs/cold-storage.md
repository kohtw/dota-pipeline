# Cold storage

The cold store is an archive of compressed replays in a bucket on the pipeline's S3 endpoint
(Garage by default; the `S3_*` settings can point at another S3-compatible store). The
`cold_restore` DAG releases replays from it back into Garage's hot `replays/` prefix on a timed
schedule.

**Ingest stays API → Garage.** `dota_daily_download` picks matches and `download_pending` fetches them; that is the only ingest path. The cold store is
for reprocessing and for burst and replay tests. The collector's Google Drive stockpile is imported
once, as a seed. Nothing reads from Drive in the pipeline.

## Why

- Valve deletes replays after about two weeks. After that, a match can only be parsed again from a copy.
- Parser or clean changes need old matches parsed again. The hot `replays/*.dem` may be gone by then
  (`dem_retention`). See [Running again](#running-again): the parsed marker has to go too.
- Load tests need replays that arrive at a known rate, faster than live, with real timing.

## Layout

Bucket and prefix are configurable. The defaults are the raw bucket (`raw`) and prefix `cold/`.

```
cold/replays/{match_id}.dem.zst     replay, zstd as Valve serves it (about 37 MB)
cold/manifest.jsonl                 one line per match
cold/restore_log/{run_id}/_plan.json         a restore run's plan
cold/restore_log/{run_id}/{match_id}.json    one release's log row
```

A manifest line:

```json
{"match_id": 9069761188, "match_seq_num": 7616367185, "start_time": 1793442909, "duration": 1530,
 "end_time": 1793444439, "bytes": 30092593, "sha256": "…", "seed_run": "seed-20261115T020000Z"}
```

Times are unix seconds, and `end_time = start_time + duration`. `bytes` and `sha256` describe the
`.dem.zst` file. `seed_run` names the seed invocation that added the match. There is one line
per `match_id`. Seeding a match again replaces its line.

The code lives in `dags/dota/cold.py`, which covers the layout, the manifest, the schedule and the
release. It knows nothing about Drive. The cold prefix must not be empty or sit under `replays/`,
`parsed/`, `clean/` or `steam_raw/` in the hot bucket.

## Seeding (one time, on the host)

`downloader/seed_cold_store.py` sits beside the collector, because it is the only part that uses
rclone and Drive. It picks `--count` matches that the collector uploaded (`status = 'uploaded'`
in `~/.local/share/dota-pipeline/state.db`), in `match_seq_num` order. By default it starts just
after the newest match already in the manifest. Each match's `start_time` and `duration` come from
its saved Steam page. A match that is missing from its page is skipped with a warning; its duration
is never guessed. The skipped and failed matches are listed at the end of the run. A later default
run starts after them, so they stay out unless you pass `--start-seq`.

"Contiguous" is looser than it sounds. Valve numbers matches in roughly the order they end; a
neighbour can end a few seconds earlier. The gaps are also between the matches the collector
sampled and uploaded, not between every match Valve played. They are real gaps between real
matches, but sparser than the live feed. Every match from one invocation gets the same `seed_run`,
and a restore never mixes two seed runs, so a hole between runs never stretches one schedule.
Each replay is downloaded with rclone and checked: it must be one complete zstd frame that holds a
Dota 2 replay. The replay is then uploaded unchanged, and the manifest is merged. The staging
folder is deleted afterwards.

With the stack up, from the repo root (the local keys are the `x-s3-creds` in `docker-compose.yml`):

```
export S3_ACCESS_KEY=... S3_SECRET_KEY=...       # S3_ENDPOINT defaults to http://127.0.0.1:3900
uv run --no-project --with boto3 --with zstandard --with requests \
    python downloader/seed_cold_store.py --count 30 --dry-run
uv run --no-project --with boto3 --with zstandard --with requests \
    python downloader/seed_cold_store.py --count 30
```

Flags:

- `--start-seq`: the first `match_seq_num` to consider.
- `--remote`: the rclone path that holds `steam/` and `replays/`. The default is `DRIVE_DEST`.
- `--state-db`: the collector's SQLite state file.
- `--bucket` and `--prefix`: where the cold store is.
- `--staging`: the parent folder for the staging folder.
- `--keep-staging`: keep the staging folder after the run.
- `--steam-listing` and `--replay-listing`: saved `rclone lsf --format ps` output. Listing `steam/`
  on Drive takes about a minute, and these flags skip it.
- `--dry-run`: prints the selection and sizes. It fetches only the small Steam pages and writes nothing.

## Restoring

Trigger `cold_restore` by hand. It has no schedule. Like every new DAG it starts paused, and a
paused DAG leaves triggered runs queued, so unpause it first. Unpausing it starts nothing.

| param | default | meaning |
|---|---|---|
| `count` | 30 | replays to release: the next fresh ones in `end_time` order |
| `speed` | 1.0 | multiplies the rate |
| `rate_per_min` | 2.0 | base rate. 2 a minute is the measured live rate through the APIs |
| `spacing` | `event` | `event` keeps the real end-time gaps, scaled to a mean of `rate_per_min * speed`. `uniform` releases one every `1 / (rate_per_min * speed)` minutes |
| `start_after_match_id` | none | start after this match in `end_time` order |
| `cold_bucket`, `cold_prefix` | `raw`, `cold/` | where the cold store is |

```
docker compose exec airflow-scheduler airflow dags trigger cold_restore \
    --conf '{"count": 30, "speed": 10, "spacing": "event"}'
```

```
plan -> release_one (one per replay: wait -> release)
```

`plan` takes the next `count` manifest entries in `end_time` order, after `start_after_match_id`
when it is set. It leaves out any match that is already in `replays/` or has a parsed
`_complete` marker. It reads the marker the way `parse_pending` does. All the entries come from the
seed run of the first one. A run picks fewer than `count` when that seed run runs out, and it says
so in the task log. Back-to-back runs (1x, then 2x) therefore get fresh matches with no settings.

`plan` then fixes the run's start, T0, 30 s after planning, so every wait has deferred before its
replay is due. It gives each replay a `scheduled_at` of T0 plus its offset and saves the whole plan
as `restore_log/{run_id}/_plan.json`. For `event` spacing, with n replays picked, the offset of
replay i is `(end_i - end_0) / (end_last - end_0) * (n - 1) / (rate_per_min * speed)` minutes. If
all the replays ended in the same second, the DAG uses `uniform` spacing instead. A plan longer
than 24 hours is refused.

Each replay then gets its own `wait` and `release` (a mapped task group), so release i waits only
for wait i:

- `wait` is a deferrable `DateTimeSensorAsync` on `scheduled_at`. It waits in the triggerer, so it
  holds no worker slot.
- `release` streams the cold object and decompresses it. The replay is written to
  `replays/{match_id}.dem` the same way `download_pending` writes it: the header must be a Dota 2
  replay, and the object only appears once it is complete. The sha256 is checked before the
  upload finishes, so a damaged archive never shows up in `replays/`.
- Up to 4 releases run at once, so replays due close together are not queued behind each other.
- A replay that turned up in `replays/` after the plan is skipped: the task ends as skipped and
  emits nothing. A failed release fails only its own task, and the rest still run.
- Releases do not retry, because a late second try would blur the timing the run is there to
  measure.

### Parse on arrival

Every successful release emits the asset `REPLAYS_ARRIVED` (`s3://raw/replays`), one event per
replay. `download_pending` emits it too, once per downloaded batch. `parse_pending` is scheduled on
that asset, with its hourly run kept as a fallback, so a restored replay starts parsing as soon as
it lands. Clean, load and dbt then follow through their own assets.

`parse_pending` has `max_active_runs=1`. Events that arrive while it runs are covered by its next
run, which parses up to 20 replays from whatever is in Garage. During a fast burst, parsing
therefore runs in batches, and the parser itself, not the release schedule, sets the latency.

On Garage, put cold objects with a single PUT, as the seed script does. Garage reports a
multipart object's composite CRC32 as a whole-object checksum, so boto3 rejects every GET of
that object with `FlexibleChecksumError`. Hot replays are multipart, like the live ones, which is
fine: Spark reads them through s3a, not boto3.

### Running again

A match with a parsed `_complete` marker is never picked again, and `parse_pending` would not
parse it again anyway. To reprocess matches, delete their `replays/{id}.dem` and
`parsed/_complete/match_id={id}/` (and any `parsed/_failed/match_id={id}/`), then trigger a run
with `start_after_match_id` set to the match just before them.

## Release log

Each release writes `{cold_prefix}restore_log/{run_id}/{match_id}.json` in the cold bucket, next to
the run's `_plan.json`. The row is written as `in_flight` just before the upload, then again with
the outcome, so a release killed mid-upload still leaves a row that names it.
`cold.read_log(client, bucket, prefix, run_id)` returns the plan and the rows in schedule order.

```json
{"run_id": "arrival_smoke_1", "match_id": 9069761188, "seed_run": null, "end_time": 1793444439,
 "scheduled_offset_s": 4.588, "scheduled_at": "2026-11-14T17:09:25.814+00:00",
 "started_at": "2026-11-14T17:09:28.299+00:00", "released_at": "2026-11-14T17:09:28.733+00:00",
 "finished_at": "2026-11-14T17:09:28.733+00:00", "bytes_in": 30092593, "bytes_out": 52419333,
 "status": "released", "speed": 10.0, "spacing": "event", "rate_per_min": 2.0}
```

| field | meaning |
|---|---|
| `end_time` | when the match ended (unix seconds): event time |
| `scheduled_offset_s`, `scheduled_at` | when the plan said to release it |
| `started_at` | upload start; null when skipped |
| `released_at` | upload complete, when the replay appeared in `replays/`; null unless `released` |
| `finished_at` | when the release was done with the row, whatever the status |
| `status` | `released`, `skipped`, `failed`, `in_flight` (only while uploading) or `interrupted` (stopped by a timeout or a kill) |
| `run_id`, `seed_run`, `speed`, `rate_per_min`, `spacing` | the run's settings and the match's seed run |

A failed row also has an `error`. For an event-time against processing-time chart, use
`end_time` against `released_at` on the `released` rows. `started_at - scheduled_at` is the lag of
the wait and the worker slot, and `released_at - started_at` is the transfer time.

`seed_run` is null for matches seeded before seed runs existed. On the local stack,
`started_at` comes about 2.5 s after `scheduled_at`: the sensor wakes, then the scheduler starts
the release task.
