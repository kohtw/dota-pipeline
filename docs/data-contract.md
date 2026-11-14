# Data contract: raw layer

What the parser produces and where it lands in the data lake. Spark clean jobs, Postgres/dbt and the frontend build against this. Change it by PR, and tell everyone downstream.

Tables and columns are defined in [`spark_jobs/replay_spec.py`](../spark_jobs/replay_spec.py). This file documents them; keep the two in sync.

## Conventions

- **Key:** every row carries `match_id` (int64, the Dota match ID). Datasets are partitioned by it.
- **Time:** `tick` (int) is the time key in all raw tables. It is the replay's own counter at 30 ticks per second, starting at replay start rather than at the 0:00 game clock.
  - In-game clock (done in clean, not raw): `game_time_s = (tick - game_start_tick) / 30`. `game_start_tick` is derived in clean from `gamerules` (first row with `m_pGameRules.m_nGameState` = 5). Pre-game is negative.
- **Teams:** `2` = Radiant, `3` = Dire.
- **Raw is unresolved:** the parser writes source values only. Columns are named after the replay property they hold. Names are indexes into a string table (`names`, `entity_names`), entities are referenced by handle, positions are grid cell + offset, player IDs are the replay's own (`m_iPlayerID` = 2 × slot). Illusions are not removed. Resolving, filtering, diffing and coordinate math all happen in clean.
- **Null handle:** `16777215` means "no entity" in any handle column.
- **Sampling:** existing `snapshot` tables (`gamerules`, `heroes`, `player_resource`, `team_data`) are sampled every 30 ticks (1 s). `kda_changes` reads every tick and emits the first row and subsequent property changes per entity/index. Event tables (`combatlog`, `wards`, `item_entities`) have exact ticks.
- **Parquet:** typed columns (int32, int64, float32, boolean, string); null = value absent. Column names may contain dots (e.g. `CBodyComponent.m_cellX`); quote them in Spark SQL with backticks.
- **Read columns by name, never by position.** Columns may be added. They are never renamed or removed without a contract change.

## Object store layout

S3-compatible store (Garage, in docker compose). Endpoint `http://garage:3900` inside compose, or `http://localhost:3900` from the host. Region `us-east-1`, path-style addressing. Credentials are in `docker-compose.yml` (dev only). Spark reads and writes it as `s3a://`.

```
raw/                                                      bucket
  replays/{match_id}.dem                                  decompressed replay
  parsed/{table}/match_id={match_id}/part-*.parquet       one dataset per table below
  parsed/_complete/match_id={match_id}/part-*.parquet     marker: match_id, parsed_at (timestamp)
  parsed/_failed/match_id={match_id}/{run}                marker: the parser crashed in this sweep run
clean/{table}/match_id={match_id}/part-*.parquet          (Spark clean jobs, TBD)
clean/_rejected/match_id={match_id}/_REJECTED             marker: the clean job rejected this match
steam_raw/seq_{start_seq}.json.gz                         one raw Steam match-history page
steam_raw/_loaded/{page file name}                        marker: this page is loaded into the warehouse
```

Writes are idempotent per match: re-parsing a match replaces only its `match_id=` partition in each table.

`replays/{match_id}.dem` is deleted after a successful load when the Airflow Variable `dem_retention` is `delete` (default `keep`). Re-parsing a match then needs its replay downloaded again.

`_complete` is written after every table, so a match is fully parsed only once its `_complete` partition exists. Downstream should read a match only then. A table with no rows for a match (e.g. `wards` when no ward was placed) has no partition for it. `dags/dota/contract.py` checks parsed output against this file.

### Steam pages and markers

| Path | Written by | Content |
|---|---|---|
| `steam_raw/seq_{start_seq}.json.gz` | `steam_fetch` (hourly), `dota_daily_download` | One raw Steam match-history page: the API's `result` object, as gzipped JSON |
| `steam_raw/_loaded/{page file name}` | `steam_summary_elt` | Empty marker, written after the page is loaded |
| `parsed/_failed/match_id={id}/{run}` | `parse_pending` | Empty marker, one per sweep run in which the parser container crashed (non-zero exit other than 137). After 3, the sweep skips the match; deleting the prefix makes it eligible again |
| `clean/_rejected/match_id={id}/_REJECTED` | `clean_pending` | Empty marker, written when the clean job rejects the match. Deleting it makes the match eligible again |

## Tables

| Table | Kind | Rows |
|---|---|---|
| `match` | file header | 1 per match |
| `players` | file header | 1 per player |
| `combatlog` | events | every combat log entry |
| `names` | string table | `CombatLogNames` |
| `entity_names` | string table | `EntityNames` |
| `gamerules` | snapshot, 1 Hz | game state |
| `heroes` | snapshot, 1 Hz | every hero entity (illusions included): position + item handles |
| `player_resource` | snapshot, 1 Hz | per player: team, hero, level, K/D/A |
| `kda_changes` | snapshot, every tick, changes only | per player index: initial team/slot/hero/K/D/A and subsequent property changes |
| `team_data` | snapshot, 1 Hz | per player: gold, XP, last hits, damage, wards, ... |
| `wards` | entity created / deleted | observer and sentry wards |
| `item_entities` | entity created / deleted | every item entity, with its name index |

Columns common to the kinds:
- snapshot: `match_id`, `tick` (multiple of the table's sampling interval), `handle` (entity handle), `class` (entity class), then the properties. The default interval is 30 ticks; `kda_changes` uses 1.
- snapshot over an array: same plus `index` (array position). Unused array entries (all properties null) are not written.
- created / deleted: `match_id`, `tick` (exact), `event` (`created` / `deleted`), `handle` (pairs the two rows), `class`, then the properties. Entities still alive at replay end have no `deleted` row.

### `match`

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `game_mode` | int | Dota game mode ID |
| `game_winner` | int | `2` Radiant, `3` Dire |
| `end_time` | int64 | Unix seconds |

### `players`

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `slot` | int | Position in the replay file's player list (0–4 Radiant, 5–9 Dire) |
| `hero_name` | string | `npc_dota_hero_*` |
| `game_team` | int | `2` / `3` |

### `combatlog`

One row per combat log entry. Wide and sparse: every field Clarity's `CombatLogEntry` exposes is a column, null when that entry does not carry the field. The meaning of some columns depends on `type` (e.g. `value` is damage for DAMAGE, gold for GOLD, and a `names` index for PURCHASE).

Core columns, always first:

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `tick` | int | Tick at which the entry was delivered |
| `type` | string | e.g. `DOTA_COMBATLOG_DAMAGE`, `_GOLD`, `_PURCHASE`, `_DEATH`, `_MODIFIER_ADD`, `_GAME_STATE` |
| `timestamp` | float | Combat log's own clock (seconds). Use `tick` for joins |
| `attacker_name_idx` | int | Index into `names` |
| `target_name_idx` | int | Index into `names` |
| `inflictor_name_idx` | int | Index into `names` (ability, item or modifier) |
| `value` | int | Meaning depends on `type` |

The remaining ~72 columns follow alphabetically, in snake_case of the Clarity field (e.g. `gold_reason`, `xp_reason`, `health`, `location_x`, `location_y`, `attacker_team`, `target_team`, `attacker_hero`, `target_illusion`, `stun_duration`, `networth`). The other name fields are also indexes (`damage_source_name_idx`, `target_source_name_idx`). `assist_players` is a `;`-separated string of player IDs.

### `names` / `entity_names`

| Column | Type | Notes |
|---|---|---|
| `match_id` | int64 | |
| `idx` | int | `names`: what combatlog `*_name_idx` (and PURCHASE `value`) hold. `entity_names`: what `item_entities.m_pEntity.m_nameStringTableIndex` holds |
| `name` | string | e.g. `npc_dota_hero_*`, `item_*` |

### `gamerules`

Snapshot of `CDOTAGamerulesProxy`.

| Column | Type | Notes |
|---|---|---|
| `m_pGameRules.m_nGameState` | int | `5` = game in progress (horn) |
| `m_pGameRules.m_flPreGameStartTime` | float | Combat log `timestamp` clock; `0.0` until set |
| `m_pGameRules.m_flGameStartTime` | float | Horn on the combat log `timestamp` clock; `0.0` until set |
| `m_pGameRules.m_flGameEndTime` | float | `0.0` until set |
| `m_pGameRules.m_bGamePaused` | bool | |

### `heroes`

Snapshot of every `CDOTA_Unit_Hero_*` entity, illusions included.

| Column | Type | Notes |
|---|---|---|
| `m_iPlayerID` | int | 2 × slot |
| `m_iTeamNum` | int | `2` / `3` |
| `m_hReplicatingOtherHeroModel` | int | Null handle on real heroes; illusions hold another hero's handle |
| `CBodyComponent.m_cellX`, `CBodyComponent.m_cellY` | int | Grid cell |
| `CBodyComponent.m_vecX`, `CBodyComponent.m_vecY` | float | Offset in cell. World x = `cellX * 128 + vecX - 16384` (same for y) |
| `m_hItems.0000` … `m_hItems.0016` | int | Item entity handle per slot: 0–5 inventory, 6–8 backpack, 9–14 stash, 15 teleport, 16 neutral. Join `item_entities.handle` for the item |

### `player_resource`

Snapshot of `CDOTA_PlayerResource`, over the player arrays (`index` = player array position).

| Column | Type | Notes |
|---|---|---|
| `index` | int | Player array position |
| `m_vecPlayerData.m_iPlayerTeam` | int | `2` / `3` |
| `m_vecPlayerTeamData.m_iTeamSlot` | int | 0–4 within the team |
| `m_vecPlayerTeamData.m_nSelectedHeroID` | int | Valve numeric hero ID |
| `m_vecPlayerTeamData.m_iLevel`, `.m_iKills`, `.m_iDeaths`, `.m_iAssists` | int | |

### `kda_changes`

Raw `CDOTA_PlayerResource` counters, inspected every tick. Columns are `match_id`,
`tick`, `handle`, `class`, `index` and:

- `m_vecPlayerData.m_iPlayerTeam`
- `m_vecPlayerTeamData.m_iTeamSlot`
- `m_vecPlayerTeamData.m_nSelectedHeroID`
- `m_vecPlayerTeamData.m_iKills`
- `m_vecPlayerTeamData.m_iDeaths`
- `m_vecPlayerTeamData.m_iAssists`

All six properties are integers. The first nonempty property tuple is emitted per
entity/index, including zero counters. Later rows are emitted only when any of
these properties changes, including team/slot/hero changes. Counter differences
are calculated in clean; raw values are not changed. A wholly absent array entry
is skipped and resets that entry's change baseline. Older parsed matches need
this table added before passing the updated parse contract.

### `team_data`

Snapshot of `CDOTA_DataRadiant` / `CDOTA_DataDire` over `m_vecDataTeam`. `class` gives the team; `index` = team slot 0–4. Join to `player_resource` on team + `index` = `m_vecPlayerTeamData.m_iTeamSlot`.

| Column | Type |
|---|---|
| `m_vecDataTeam.m_iNetWorth`, `.m_iReliableGold`, `.m_iUnreliableGold`, `.m_iTotalEarnedGold`, `.m_iTotalEarnedXP` | int |
| `m_vecDataTeam.m_iLastHitCount`, `.m_iDenyCount` | int |
| `m_vecDataTeam.m_flHeroDamage`, `.m_flTowerDamage`, `.m_fHealing`, `.m_fStuns` | float |
| `m_vecDataTeam.m_iObserverWardsPlaced`, `.m_iSentryWardsPlaced`, `.m_iCampsStacked`, `.m_iRunePickups`, `.m_iTowerKills`, `.m_iRoshanKills` | int |

### `wards`

Created / deleted of `CDOTA_NPC_Observer_Ward*`. `class` `CDOTA_NPC_Observer_Ward` = observer, `CDOTA_NPC_Observer_Ward_TrueSight` = sentry.

| Column | Type | Notes |
|---|---|---|
| `m_nPlayerOwnerID` | int | 2 × slot |
| `m_iTeamNum` | int | `2` / `3` |
| `CBodyComponent.m_cellX`, `.m_cellY`, `.m_vecX`, `.m_vecY` | int / float | As in `heroes` |

### `item_entities`

Created / deleted of every `CDOTA_Item*` entity (`class` e.g. `CDOTA_Item_TeleportScroll`).

| Column | Type | Notes |
|---|---|---|
| `m_pEntity.m_nameStringTableIndex` | int | Index into `entity_names` (e.g. `item_tpscroll`) |

Hero inventory at tick T: `heroes` row at T → each `m_hItems.*` handle → `item_entities.handle` → `entity_names.idx`.

## Ownership

| Layer | Owner |
|---|---|
| Parser output (this file's schemas) | Role 3: replay parsing |
| `raw/` upload, `clean/` layout | Role 2: ingestion |
| Postgres `raw.*` tables, dbt | Role 4 |
| Reads from Postgres | Role 5: frontend |
| DAG wiring, retries | Role 1: orchestration |
