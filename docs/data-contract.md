# Data contract: raw layer

What the parser produces and where it lands in the data lake. Ingestion, Postgres/dbt and the frontend build against this. Change it by PR, and tell everyone downstream.

## Conventions

- **Key:** every row and file carries `match_id` (int64, the Dota match ID).
- **Time:** `tick` (int) is the time key in all raw files. It is the replay's own counter at 30 ticks per second, starting at replay start rather than at the 0:00 game clock.
  - In-game clock (done in clean, not raw): `game_time_s = (tick - game_start_tick) / 30`. `game_start_tick` comes from `match.json`. Pre-game is negative.
- **Heroes:** always the `npc_dota_hero_*` name (e.g. `npc_dota_hero_ember_spirit`). This is the same form as in the combat log, so files join on it.
- **Teams:** `2` = Radiant, `3` = Dire.
- **CSV:** UTF-8, comma-separated, header row, RFC 4180 quoting. An empty cell means null.
- **Load CSVs by header name, never by column position.** Columns may be added. They are never renamed or removed without a contract change.
- **Raw is faithful:** raw holds source values only. Converting, renaming, splitting by type and deduplicating all happen in clean.

## Object store layout

S3-compatible store (Garage, in docker compose). Endpoint `http://garage:3900` inside compose, or `http://localhost:3900` from the host. Region `us-east-1`, path-style addressing. Credentials are in `docker-compose.yml` (dev only).

```
raw/                                    bucket
  replays/{match_id}.dem                decompressed replay
  parsed/{match_id}/match.json          match summary
  parsed/{match_id}/combatlog.csv       every combat log entry
  parsed/{match_id}/positions.csv       hero positions, 1 Hz
  parsed/{match_id}/names.csv           CombatLogNames lookup table
clean/{table}/match_id={match_id}/part.parquet    (owned by ingestion, TBD)
```

Writes are idempotent per match: re-running a match overwrites the same keys.

## Files

### `match.json`

| Field | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `game_mode` | int | Dota game mode ID |
| `winner` | int | `2` Radiant, `3` Dire |
| `end_time` | int64 | Unix seconds |
| `game_start_tick` | int | Tick of the horn (0:00); gamerules state 5 (GAME_IN_PROGRESS) |
| `game_start_time` | float | Same moment on the combat log `timestamp` clock (seconds) |
| `players[]` | array | One per player, ordered by slot |
| `players[].slot` | int | 0–4 Radiant, 5–9 Dire |
| `players[].hero` | string | `npc_dota_hero_*` |
| `players[].team` | int | `2` / `3` |

### `combatlog.csv`

One row per combat log entry. This is a wide, sparse file: every field Clarity's `CombatLogEntry` exposes is a column, and a cell is empty when that entry does not carry the field. The meaning of some columns depends on `type` (e.g. `value` is damage for DAMAGE, gold for GOLD, and an item name index for PURCHASE).

Core columns, always first:

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `tick` | int | Tick at which the entry was delivered |
| `type` | string | e.g. `DOTA_COMBATLOG_DAMAGE`, `_GOLD`, `_PURCHASE`, `_DEATH`, `_MODIFIER_ADD`, `_GAME_STATE` |
| `timestamp` | float | Combat log's own clock (seconds). Use `tick` for joins |
| `attacker_name` | string | `npc_*` / `dota_*` name |
| `target_name` | string | |
| `inflictor_name` | string | Ability, item or modifier |
| `value` | int | Meaning depends on `type` |
| `value_name` | string | `value` resolved through the names table (e.g. PURCHASE → `item_*`), else empty |

The remaining ~72 columns follow alphabetically, in snake_case of the Clarity field (e.g. `gold_reason`, `xp_reason`, `health`, `location_x`, `location_y`, `attacker_team`, `target_team`, `attacker_hero`, `target_illusion`, `stun_duration`, `networth`). Booleans are `true`/`false`. `assist_players` is a `;`-separated list of player IDs.

### `positions.csv`

One row per real hero (illusions excluded) every 30 ticks (1 s).

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `tick` | int | Multiple of 30 |
| `hero` | string | `npc_dota_hero_*` |
| `team` | int | `2` / `3` |
| `x` | float | World units, about −8000 to 8000 |
| `y` | float | World units |

### `names.csv`

The replay's `CombatLogNames` string table.

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `idx` | int | Index referenced by the combat log (e.g. PURCHASE `value`). Name columns in `combatlog.csv` are already resolved |
| `name` | string | |

## Ownership

| Layer | Owner |
|---|---|
| Parser output (this file's schemas) | Role 3: replay parsing |
| `raw/` upload, `clean/` layout | Role 2: ingestion |
| Postgres `raw.*` tables, dbt | Role 4 |
| Reads from Postgres | Role 5: frontend |
| DAG wiring, retries | Role 1: orchestration |
