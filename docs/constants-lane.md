# Constants lane

Reference data ("constants") is the heroes, items, patches and game modes the marts label matches
with. It is not static: heroes get renamed, items are reworked or removed, and every patch adds
rows. The `refresh_constants` DAG keeps every version it sees in the warehouse, and dbt turns that
history into slowly changing dimensions (type 2: one row per version, with a validity window).

1. `refresh_constants` (daily, starts paused) fetches Valve's dota2.com datafeed (`herolist`,
   `herodata` per hero with a 0.5 s pause, `itemlist`, `patchnoteslist`) and OpenDota's
   `/constants/game_mode`, and saves one raw snapshot to Garage. It parses everything first, so a
   feed whose shape changed fails the run and saves nothing.
2. Its `load` task reads the snapshot back, parses it with `dags/dota/constants.py`, and appends to
   `stg.constant_*` only the rows that differ from that key's latest stored version. It emits
   `CONSTANTS_LOADED`, which wakes `dbt_build`. When nothing changed it is skipped and dbt is not
   woken.
3. dbt builds `dim_hero_versions` / `dim_item_versions` (every version, with `valid_from`,
   `valid_to`, `is_current`) and the current dims `dim_heroes`, `dim_items`, `dim_patches`,
   `dim_game_modes`.

DAG: `dags/refresh_constants.py`. Logic: `dags/dota/constants_load.py` (snapshot, diff, load),
`dags/dota/datafeed.py` (Valve client), `dags/dota/opendota.py` (`game_modes`). DDL:
`warehouse/init/06_constants_tables.sql`. Models: `dbt/models/dims/`.

## Garage

| Path | Content |
|---|---|
| `constants/fetched_at={YYYYMMDDTHHMMSSZ}/herolist.json.gz` | The datafeed's `herolist` response |
| `constants/fetched_at={...}/herodata.json.gz` | One object keyed by hero id (as a string): each hero's full `herodata` response |
| `constants/fetched_at={...}/itemlist.json.gz` | The datafeed's `itemlist` response |
| `constants/fetched_at={...}/patchnoteslist.json.gz` | The datafeed's `patchnoteslist` response |
| `constants/fetched_at={...}/game_mode.json.gz` | OpenDota's `/constants/game_mode` response |

All gzipped JSON, written as is. The time is the fetch time in UTC (ISO 8601 basic format, so the
key has no colons). A snapshot is about 450 KB gzipped (4 MB before), almost all of it `herodata`.

## Warehouse tables

Append-only, one row per key each time a snapshot shows a changed version.

| Table | Key | Columns |
|---|---|---|
| `stg.constant_heroes` | `(hero_id, observed_at)` | `hero_id`, `npc_name`, `localized_name`, `primary_attr`, `roles` |
| `stg.constant_items` | `(item_id, observed_at)` | `item_id`, `name`, `localized_name`, `is_recipe`, `neutral_tier` |
| `stg.constant_patches` | `(patch, observed_at)` | `patch`, `released_at` |
| `stg.constant_game_modes` | `(game_mode_id, observed_at)` | `game_mode_id`, `name`, `label` |

Every table also has `observed_at` (the snapshot's fetch time, UTC, no time zone) and
`source_snapshot` (its Garage prefix). The columns are the seed columns, with the same meaning.
A blank string from the feed is stored as NULL, as the seed CSVs load it.

Rules:

- A row is inserted only when the key is new or a column differs from the key's latest version
  observed at or before the snapshot. Loading the same snapshot again, or an older one, inserts
  nothing.
- Keys never disappear. A hero or item the feed stops listing is neither deleted nor flagged: its
  last version stays current, because old replays still use it.

## Seeds are the baseline

`dbt/seeds/baseline_{heroes,items,patches,game_modes}.csv` hold the version of every key before the
DAG first observed it. The dims treat it as valid from 1970-01-01, so they work on a warehouse where
the DAG has never run (the dev DuckDB, a fresh volume). A history row that repeats the previous
version (for example the first snapshot, which mostly repeats the baseline) opens no new window.

`scripts/refresh_constants.py` rewrites the baseline CSVs from the live feed, with the DAG's fetch and
parse code. Because the baseline is valid from the beginning of time, refreshing it rewrites the start
of history. With the DAG running, refresh it only to add new keys, not after every patch.

## Dims

| Model | Grain | Notes |
|---|---|---|
| `dim_hero_versions` | One row per hero version | `valid_from`, `valid_to` (NULL = current), `is_current` |
| `dim_item_versions` | One row per item version | as above |
| `dim_heroes` | One row per hero | Current version. Same columns and types as the old seed |
| `dim_items` | One row per item | Current version. Same columns and types as the old seed |
| `dim_patches` | One row per patch | Latest known `released_at` |
| `dim_game_modes` | One row per game mode | Latest name and label |

The marts join the current dims, so a renamed hero shows its current name on every match. To label
a match with the name it had at the time, join the versions model as of the match's start:
`valid_from <= t and (valid_to is null or t < valid_to)`. `dim_patches` gives the patch a match was
played on: the latest patch released before its start.

Tests: the old seed column tests on the current dims, and `valid_windows` on the versions models (no
repeated `(key, valid_from)`, at most one current row per key, no overlapping windows).
