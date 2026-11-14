# Steam lane

The Steam lane loads match summaries from the Steam Web API method `GetMatchHistoryBySequenceNum`.

1. `steam_fetch` writes raw pages to Garage as `steam_raw/seq_{start_seq}.json.gz`.
2. `steam_summary_elt` flattens each page into three `stg` tables in the warehouse Postgres.
3. `dbt_build` builds the marts from those tables.

The loader only flattens. It does not filter matches or apply rules. Rules such as "ranked All Pick
only" live in dbt (`stg_steam_ranked`).

DDL: `warehouse/init/04_steam_tables.sql`. Loader: `dags/dota/steam_summary.py`.

## Tables

| Table | Grain | Key |
|---|---|---|
| `stg.steam_matches` | One row per match | `match_id` |
| `stg.steam_players` | One row per listed player in a match | `(match_id, player_slot)` |
| `stg.steam_picks_bans` | One row per draft entry (pick or ban) in a match | `(match_id, ord)` |

A match has rows in `stg.steam_picks_bans` only if its page lists `picks_bans`. On the pages
checked, every All Pick (mode 22) and Turbo (mode 23) match listed it, including all ranked All
Pick matches. Single Draft (4) and Ability Draft (18) matches did not.

## Columns

Every column except `source_page`, `loaded_at` and `ord` has the same name as the Steam field. A
field missing from the page is stored as NULL.

### `stg.steam_matches`

| Column | Type | Notes |
|---|---|---|
| `match_id` | BIGINT | |
| `match_seq_num` | BIGINT | |
| `start_time` | BIGINT | Unix seconds |
| `duration` | INTEGER | Seconds |
| `game_mode` | INTEGER | 22 is All Pick |
| `lobby_type` | INTEGER | 7 is ranked |
| `radiant_win` | BOOLEAN | |
| `human_players` | INTEGER | |
| `radiant_score` | INTEGER | Radiant kills |
| `dire_score` | INTEGER | Dire kills |
| `first_blood_time` | INTEGER | Seconds |
| `pre_game_duration` | INTEGER | Seconds |
| `cluster` | INTEGER | Server cluster |
| `leagueid` | INTEGER | 0 outside leagues |
| `source_page` | TEXT | Garage key of the page the row came from |
| `loaded_at` | TIMESTAMP | Load time, UTC, no time zone |

### `stg.steam_players`

| Column | Type | Notes |
|---|---|---|
| `match_id` | BIGINT | |
| `player_slot` | INTEGER | 0-4 Radiant, 128-132 Dire |
| `hero_id` | INTEGER | |
| `kills`, `deaths`, `assists` | INTEGER | |
| `leaver_status` | INTEGER | 0 means the player stayed |
| `team_number` | INTEGER | 0 Radiant, 1 Dire |
| `last_hits`, `denies` | INTEGER | |
| `gold_per_min`, `xp_per_min` | INTEGER | |
| `level` | INTEGER | Final hero level |
| `net_worth` | INTEGER | |
| `hero_damage`, `tower_damage`, `hero_healing` | INTEGER | |
| `gold`, `gold_spent` | INTEGER | Gold at the end, gold spent in total |
| `item_0` to `item_5` | INTEGER | Item ids in the main slots, 0 when empty |
| `backpack_0` to `backpack_2` | INTEGER | Item ids in the backpack, 0 when empty |
| `item_neutral` | INTEGER | Neutral item id, 0 when empty |

### `stg.steam_picks_bans`

| Column | Type | Notes |
|---|---|---|
| `match_id` | BIGINT | |
| `ord` | INTEGER | The Steam `order` field. Renamed because `ORDER` is a reserved word. |
| `is_pick` | BOOLEAN | False for a ban |
| `hero_id` | INTEGER | |
| `team` | INTEGER | 0 Radiant, 1 Dire |

## Excluded fields

| Field | Reason |
|---|---|
| `account_id` | Personal data. No mart needs it. |
| `ability_upgrades` | Nested list. No mart needs it. |
| Other match fields (`tower_status_*`, `barracks_status_*`, `flags`, `engine`) | No mart needs them. They stay in the raw page. |
| Other player fields (`team_slot`, `hero_variant`, `item_neutral2`, `aghanims_*`, `moonshard`, `scaled_*`, `additional_units`) | No mart needs them. They stay in the raw page. |

The raw pages stay in Garage, so a later change can load any excluded field without new Steam calls.

## Loading and idempotency

- `load_steam` runs the DDL file, deletes the batch's match ids from all three tables, and inserts
  the new rows. All of this is one transaction per page.
- Within a batch, the last row for a key wins.
- After a page commits, the loader writes an empty marker at `steam_raw/_loaded/seq_{start_seq}.json.gz`.
  A page with a marker is skipped.
- Columns added later use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. Tables on an existing volume
  gain them the next time the DDL runs. Old rows get NULL in the new columns until they are reloaded.

## Backfill

To reload every page, for example after adding columns:

1. Deploy the change.
2. Delete the keys under `steam_raw/_loaded/`. Leave the pages under `steam_raw/` in place.
3. Trigger `steam_summary_elt`. Each run loads up to 50 pages, so repeat it until no unloaded pages remain.
4. Check that the row counts in `stg.steam_matches` and `stg.steam_players` did not change, and
   that `source_page` is filled.

A reload makes no Steam calls. It reads only the pages already in Garage.
