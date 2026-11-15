# Data contract: clean layer and warehouse

DRAFT for review by Maximus (Spark clean job) and Brennan (dashboard). Owner: Chris.

The clean layer turns the raw Parquet tables ([data-contract.md](data-contract.md)) into tables you can use without knowing the replay's internals: names resolved, illusions removed, one game clock, one player numbering. Spark writes these tables; they are loaded as-is into Postgres schema `clean`; dbt builds the `marts` schema from them; the dashboard reads `marts` only.

```
raw/parsed/*.parquet --Spark--> clean/{table}/match_id={id}/part-*.parquet --load--> Postgres clean.* --dbt--> Postgres marts.*
```

The DDL is in [`warehouse/init/`](../warehouse/init/). This file documents it; keep the two in sync.

## Conventions

- **Grain:** one sentence per table saying what one row means (e.g. "one player at one second of one match"). The key is whatever makes that sentence unique. Every table below states both.
- **Key:** every table carries `match_id` (int64). Loading is idempotent per match: delete that `match_id`, insert its rows, in one transaction.
- **`player_slot`:** the one player ID used everywhere in clean and marts. `0-4` Radiant, `5-9` Dire; equals `players.slot` in raw. It is `(team - 2) * 5 + team_slot`. Raw uses other IDs (`m_iPlayerID`, array `index`); convert them to this and drop them.
- **Game clock:** `game_time_s = (tick - game_start_tick) / 30.0`. `game_start_tick` is the first `gamerules` tick with `m_pGameRules.m_nGameState = 5`. Pre-game is negative. `tick` is kept next to it as the join key.
- **Teams:** `2` Radiant, `3` Dire.
- **World coordinates:** `x = m_cellX * 128 + m_vecX - 16384`, same for `y`.
- **Real heroes only:** drop illusion entities from position/inventory processing and illusion participants from damage processing. For deaths, exclude illusion victims but retain illusion attackers: the real victim's death and the owner's kill credit can still count.
- **Scoreboard credit:** per-tick `kda_changes` counters establish deaths, kills and assists. Keep direct last hitter separate from credited killer. Every counter increment must be accounted for and final totals must equal the final sampled scoreboard. Reports identify `credit_method = per_tick_counters` and `contract_version = kda-credit-v2`.
- **Names resolved:** `*_name_idx` columns are joined to `names.name`; item handles through `item_entities` to `entity_names`.
- **Nulls:** a missing value is `NULL`, never `0`. Exception: key columns cannot be null (see `inflictor_name`).
- **Money:** `net_worth` (gold plus the value of items) and `unspent_gold` (gold in hand) are different numbers and are never mixed.
- **Types:** `BIGINT`, `INTEGER`, `DOUBLE PRECISION`, `TEXT`, `BOOLEAN`. Parquet and Postgres use the same column names.

## Tables

### `matches`

**Grain:** one match. **Key:** `match_id`.

| Column            | Type             | Built from                                                  |
| ----------------- | ---------------- | ----------------------------------------------------------- |
| `match_id`        | BIGINT           | raw `match`                                                 |
| `game_mode`       | INTEGER          | `match.game_mode`                                           |
| `winner_team`     | INTEGER          | `match.game_winner`                                         |
| `end_time`        | BIGINT           | `match.end_time` (Unix seconds)                             |
| `game_start_tick` | INTEGER          | first `gamerules` tick with state 5                         |
| `duration_s`      | DOUBLE PRECISION | last `gamerules` row: `m_flGameEndTime - m_flGameStartTime` |
| `assist_attribution_complete` | BOOLEAN, not null | False when ambiguous groups have withheld assist links |
| `assist_attribution_assumed` | BOOLEAN, not null | True when published event links depend on participant or same-side assumptions; independent of completeness |

### `players`

**Grain:** one player in one match. **Key:** `(match_id, player_slot)`.

| Column        | Type    | Built from                                                                                               |
| ------------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `player_slot` | INTEGER | `players.slot`                                                                                           |
| `team`        | INTEGER | `players.game_team`                                                                                      |
| `team_slot`   | INTEGER | `player_slot % 5`                                                                                        |
| `hero_name`   | TEXT    | `players.hero_name` (`npc_dota_hero_*`)                                                                  |
| `hero_id`     | INTEGER | `player_resource.m_vecPlayerTeamData.m_nSelectedHeroID` (Valve's number; joins to Steam). NULL if absent |

### `player_stats`

**Grain:** one player at one second of one match. **Key:** `(match_id, player_slot, tick)`.

Built from `player_resource` joined to `team_data` on `tick` and `(team, team_slot)`. In `team_data`, `class` gives the team (`CDOTA_DataRadiant` = 2, `CDOTA_DataDire` = 3) and `index` is `team_slot`. In `player_resource`, team is `m_vecPlayerData.m_iPlayerTeam` and `team_slot` is `m_vecPlayerTeamData.m_iTeamSlot`. Skip rows whose team is not 2 or 3.

| Column                                   | Type             | Built from                                                          |
| ---------------------------------------- | ---------------- | ------------------------------------------------------------------- |
| `player_slot`, `tick`                    | INTEGER          | as above                                                            |
| `game_time_s`                            | DOUBLE PRECISION | game clock                                                          |
| `level`, `kills`, `deaths`, `assists`    | INTEGER          | `player_resource` `m_iLevel`, `m_iKills`, `m_iDeaths`, `m_iAssists` |
| `net_worth`                              | INTEGER          | `m_iNetWorth`                                                       |
| `unspent_gold`                           | INTEGER          | `m_iReliableGold + m_iUnreliableGold`                               |
| `total_earned_gold`, `total_earned_xp`   | INTEGER          | `m_iTotalEarnedGold`, `m_iTotalEarnedXP`                            |
| `last_hits`, `denies`                    | INTEGER          | `m_iLastHitCount`, `m_iDenyCount`                                   |
| `hero_damage`, `tower_damage`, `healing` | DOUBLE PRECISION | `m_flHeroDamage`, `m_flTowerDamage`, `m_fHealing`                   |

### `positions`

**Grain:** one real hero at one second of one match. **Key:** `(match_id, player_slot, tick)`.

| Column                | Type                      | Built from               |
| --------------------- | ------------------------- | ------------------------ |
| `player_slot`         | INTEGER                   | `heroes.m_iPlayerID / 2` |
| `tick`, `game_time_s` | INTEGER, DOUBLE PRECISION | as above                 |
| `x`, `y`              | DOUBLE PRECISION          | world coordinates        |

If a player has two real hero entities at one tick (e.g. Meepo clones), keep the lowest `handle`.

### `damage_events`

**Grain:** all damage from one attacker to one target by one source at one tick. **Key:** `(match_id, tick, attacker_name, target_name, inflictor_name)`.

Only `DOTA_COMBATLOG_DAMAGE` rows where the attacker or the target is a real hero. Rows with the same key are summed.

| Column                         | Type                      | Built from                                                            |
| ------------------------------ | ------------------------- | --------------------------------------------------------------------- |
| `tick`, `game_time_s`          | INTEGER, DOUBLE PRECISION |                                                                       |
| `attacker_name`, `target_name` | TEXT                      | `names.name` for `*_name_idx`                                         |
| `inflictor_name`               | TEXT, not null            | ability or item; NULL index (auto attack) becomes `dota_unknown`      |
| `attacker_slot`, `target_slot` | INTEGER, nullable         | `player_slot` of the hero via `hero_name`; NULL for creeps and towers |
| `damage`                       | INTEGER                   | sum of `value`                                                        |
| `hits`                         | INTEGER                   | number of rows summed                                                 |

### `deaths`

**Grain:** one scoreboard-counted real hero death. **Key:** `(match_id, tick, victim_slot)`.

Start from real-hero `DOTA_COMBATLOG_DEATH` records, including illusion attackers but excluding
illusion victims. Merge consistent self/non-self duplicate records for the same death; conflicting
sources block the match. Count a death only when its victim's death counter rises in the configured
window (default event tick through four ticks later, inclusive). Reincarnation records with no
counter rise are not scoreboard deaths and remain review notes.

| Column | Type | Meaning |
| --- | --- | --- |
| `tick` | INTEGER | Combat-log death tick, not the counter-update tick |
| `game_time_s` | DOUBLE PRECISION | Game clock at the death tick |
| `victim_slot` | INTEGER, not null | Real victim's normalized player slot |
| `killer_slot` | INTEGER, nullable | Direct last hitter's hero slot; NULL for a tower, creep or unmapped unit |
| `killer_name` | TEXT, nullable | Direct last hitter's name, including non-player units |
| `credited_killer_slot` | INTEGER, nullable | Player whose kill counter establishes credit; 0-9, NULL only if nobody received player kill credit |
| `link_status` | TEXT, not null | `exact`, `participant_assumed`, or `same_side_possible` |

Resolve source ownership and counter increases together. `killer_slot` is not a scoreboard kill
key: a tower can be the last hitter while `credited_killer_slot` identifies the credited player.
A decrease, missing increment, inconsistent final total, conflicting source, ambiguous kill
assignment or exceeded search limit blocks completion. Assist-only ambiguity can publish partial
output after a separate bounded search proves every credited killer is unique, including under
relaxed participant/team constraints. The bounded attribution solver is in `spark_jobs/clean_replays.py`.

`participant_assumed` means relaxing participant constraints allows a different link, or the
assist-participant fallback selected one allocation from multiple counter-consistent answers.
The fallback is attempted only on ambiguous groups; lists are not mandatory for unique assists.
`same_side_possible` means allowing same-team credit allows a different link. These labels retain
link uncertainty even when final player totals are exact; exact totals alone do not prove each
individual assist-to-death association.

### `death_assists`

**Grain:** one player credited with an assist on one scoreboard death.
**Key:** `(match_id, tick, victim_slot, assist_slot)`.

| Column | Type | Meaning |
| --- | --- | --- |
| `tick` | INTEGER | The linked death's combat-log tick |
| `victim_slot` | INTEGER, not null | Victim of the linked death |
| `assist_slot` | INTEGER, not null | Player whose assist counter establishes credit |
| `same_side` | BOOLEAN, not null | Whether assister and victim are on the same team |

Credit comes from per-tick assist-counter increases, not from counting `assist_players`.
The raw list provides participant constraints, uses normalized slots 0-9 directly, and may omit
credited players or include the killer. Never divide those participant IDs by two. Retain actual
same-team credit, including a victim's recorded assist increase, with `same_side = true`.

When ambiguity remains, omit all assist links in that timing group. Publish its established
hero deaths and uniquely proven kill credits. Set `assist_attribution_complete = false` on the
match. Keep recorded player K/D/A unchanged. The review report's `assist_attribution` includes
per-player `unresolved_assists`, affected death ticks/victims, raw participant lists and the
counter increases that were withheld. Every counter increase is accounted for once; published
assist counts plus unresolved counts must equal each player's recorded assists exactly.
No arbitrary victim assignment is published. Other timing groups retain their links.
Both match flags are required explicit booleans on new output. They are independent;
partial and assumed attribution can coexist in one match.

### Upgrading existing warehouses

`warehouse/init/02_clean_tables.sql` declares the credit fields for fresh tables and
ends with repeat-safe `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements for existing
Postgres/DuckDB databases. The same file adds the two match-quality flags
for existing tables, preserving NULL for unverified historical rows.
Existing rows retain NULL credit fields because their old last-hitter data does not establish
scoreboard credit or confidence. Re-clean and reload those matches; never backfill
`credited_killer_slot = killer_slot`, `link_status = exact`, or `same_side = false` by assumption.
The new clean contract and loader reject incoming legacy/missing/null credit labels. Fresh tables
also require non-null `link_status` and `same_side`; upgraded legacy columns remain nullable until
historical rows have been replaced. `load_pending.apply_schema` applies init/migration SQL before
new Parquet rows are inserted, independently of the later dbt build.

dbt counts kills by `credited_killer_slot` and retains published assist links for K/D/A
reconciliation. Complete matches require exact assist equality; partial matches require linked
assists not to exceed the recorded total. Exact explanations for withheld counts are validated
by the cleaner and saved in its report. Legacy unknown flags fail the K/D/A check until revalidated.
The dashboard's participation numerator is `kill_involvements`, which adds
credited kills to eligible assists on kills credited to that player's team. Same-side assists
and assists on deaths with no player-credited kill remain in K/D/A but are excluded from this
numerator. Rebuild the marts after migrating and reloading historical clean rows.

### `purchases`

**Grain:** the items one player bought at one tick. **Key:** `(match_id, tick, player_slot, item_name)`.

From `DOTA_COMBATLOG_PURCHASE` rows. `player_slot` is the target hero; `item_name` is `names.name` for `value`. `quantity` counts identical rows at the same tick.

### `final_inventory`

**Grain:** one item slot of one player at the last snapshot. **Key:** `(match_id, player_slot, inventory_slot)`.

Last `heroes` row per real hero. For each `m_hItems.NNNN` handle that is not `16777215`: `inventory_slot` is `NNNN` (0-5 inventory, 6-8 backpack, 9-14 stash, 15 teleport, 16 neutral); `item_name` comes from `item_entities.handle` then `entity_names.idx`. Empty slots have no row.

## Marts (dbt, schema `marts`)

Built by [`dbt/`](../dbt/). The dashboard queries these. Each has dbt tests on its key.

| Mart                 | Grain                               | Answers                                             |
| -------------------- | ----------------------------------- | --------------------------------------------------- |
| `match_scoreboard`   | one player in one match             | final K/D/A, last hits, net worth, GPM, final items |
| `match_inventory`    | one occupied item slot of one player in one match | final item identity, display name and inventory location |
| `player_timeseries`  | one player at one second            | net worth and XP over time                          |
| `damage_by_pair`     | attacker, target, source in a match | who damaged whom, and share of team hero damage     |
| `kill_participation` | one player in one match             | share of team kills the player took part in         |
| `hero_performance_summary` | one hero in qualifying Steam matches | average combat, economy and contribution metrics with observation counts |
| `hero_matchup_summary` | one ordered hero/opponent pair in qualifying Steam matches | whole-match results against each enemy hero and overall win-rate context |
| `team_advantage_timeline` | one replay match at each ten-second observation time | Radiant versus Dire net worth, complete-team coverage and two-minute economic swings |
| `match_deaths` | one scoreboard death | where it happened (corpse), credited killer, last-hitter kind, assisters, time dead, fight number |
| `match_fights` | one fight (deaths under 15 s apart) | kind, deaths per side, winner, hero damage per side, net-worth swing |
| `match_fight_players` | one player in one fight | K/D/A and enemy-hero damage dealt and taken in the fight window |
| `player_damage_timeline` | one player in one game minute | damage dealt to heroes, buildings, Roshan and units; damage taken from heroes and units |
| `player_purchases` | the items one player bought at one tick | build order, display names, recipe completions, consumable flag |
| `player_lanes` | one player in one match | lane and lane role from laning-phase positions, stats at 10:00 |
| `match_positions` | one hero every five seconds | position and alive flag (map replay, fight paths) |
| `hero_item_timings` | one hero and item across parsed replays | how often and when the hero first bought the item |

### Event marts

Built from `clean.deaths`, `death_assists`, `positions`, `damage_events` and `purchases` for the
dashboard's match timeline, map, fights, laning, items and damage views and the Replay insights page.
Two staging views feed them:

- `stg_hero_deaths`: a dead hero's entity stays at its corpse until it respawns and jumps to the
  fountain. The corpse is the first position sample after the death tick, and the respawn is the first
  later sample away from it (searched up to 240 seconds). On the first 62 replays every death found a
  corpse and a respawn; average time dead rises from about 16 s early to about 80 s after 40 minutes.
- `stg_hero_positions`: every position sample flagged `is_alive`, so lanes and the replay do
  not count corpse time.

Assumptions, also in each model's SQL:

- **Fights** follow the OpenDota rule: a new fight starts after more than 15 seconds without a death.
  Fights are not split by place, so two pick-offs on opposite lanes in the same seconds share a fight.
  The fight window runs 10 s before the first death to 3 s after the last.
- **Lanes** use fixed zones of world coordinates over 1:00-10:00 (own base left out); the most visited
  zone wins at 40% or more, else `roaming`. On the 62 replays checked this gave a 2-1-2 split per team
  in 61 matches.
- **Damage** to buildings matches target names containing `tower`, `fort`, `rax`, `fillers` or
  `healers`. A minute with no damage is a measured zero: the combat log covers the whole replay.
- **Consumables** are a fixed list in `player_purchases`. Item costs are not in `dim_items`, so the
  views mark recipe purchases (item completions) instead of guessing which items are "big".

### `match_inventory`

**Grain:** one occupied slot of one player at the final recorded hero snapshot.
**Key:** `(match_id, player_slot, inventory_slot)`, exposed as `inventory_key`.

Reads `clean.final_inventory` and left joins `dim_items.name = item_name`. Keeps the original
`item_name` and supplies `item_display_name` from Valve's nonblank localized name. Unknown items
and unnamed recipes use their internal name with `item_` removed and underscores replaced by spaces.

`location` groups slots as `main` (0-5), `backpack` (6-8), `stash` (9-14), `teleport` (15),
and `neutral` (16). Exact slot numbers and duplicate items remain separate rows. Empty source
slots have no row; this mart does not synthesize empty slots or establish inventory coverage.
It does not change the main-inventory fields in `match_scoreboard`.

### `team_advantage_timeline`

**Grain:** one replay match at one aligned game-clock observation time.
**Key:** `(match_id, observation_time_s)`.

Built from `stg_replay_player_observations`, which selects each roster player's
latest snapshot at or before each ten-second grid time. The grid begins at zero
and ends at the last complete ten-second boundary within `matches.duration_s`.
It does not extend to the last available snapshot when the replay records extra
time beyond the match duration. Rosters must contain five distinct players per team.

| Column | Type | Built from |
| ------ | ---- | ---------- |
| `match_id`, `observation_time_s` | BIGINT, INTEGER | replay ID and requested game-clock grid time |
| `radiant_net_worth`, `dire_net_worth` | BIGINT, nullable | sum of five usable observations for clean team 2 or 3 |
| `net_worth_advantage` | BIGINT, nullable | Radiant total minus Dire total; requires both complete totals |
| `radiant_players_observed`, `dire_players_observed` | BIGINT | usable player observations, from 0 to 5 |
| `max_snapshot_age_s` | DOUBLE PRECISION, nullable | oldest selected snapshot age, including stale or otherwise unusable values |
| `net_worth_advantage_change_120s` | BIGINT, nullable | current advantage minus advantage at exactly 120 seconds earlier |

A usable snapshot has non-null, nonnegative net worth and age from zero through
five seconds inclusive. Never use future snapshots or skip a newer missing value
to retrieve an older valid one. Pregame snapshots can support time zero under the
same freshness rule. Recorded zero is valid; missing net worth is not zero.

Incomplete coverage leaves the affected team's total NULL. A complete opposing
team's total can remain available, but advantage requires both teams. The two-minute
change is NULL before 120 seconds or when either endpoint is unavailable. Keep
incomplete grid rows so charts show gaps instead of concealing missing observations.

Positive advantage or change favours Radiant; negative favours Dire. These are
sampled economic positions and changes, not win probabilities or causal explanations.
Full alignment rules and the real-data audit are in
[team-advantage-contract.md](team-advantage-contract.md).

### `hero_performance_summary`

**Grain:** one hero in the ranked All Pick Steam sample. **Key:** `hero_id`.

Built from `stg_steam_ranked`: lobby 7, mode 22, at least 900 seconds, ten human
players and a known winner. Matches with leavers are kept. Hero names come from a
left join to `dim_heroes`; missing names do not remove observed heroes.

| Column | Type | Built from |
| ------ | ---- | ---------- |
| `hero_id`, `localized_name` | INTEGER, TEXT (nullable name) | Steam hero ID and `dim_heroes` name |
| `appearances`, `wins`, `losses` | BIGINT | qualifying appearances; wins when `radiant_win = on_radiant`; losses = appearances - wins |
| `win_rate` | DOUBLE PRECISION | wins / appearances, from 0 to 1 |
| `avg_kills`, `avg_deaths`, `avg_assists` | DOUBLE PRECISION, nullable | average recorded final K/D/A counts |
| `avg_gpm`, `avg_xpm` | DOUBLE PRECISION, nullable | average Steam `gold_per_min` and `xp_per_min` |
| `avg_hero_damage_per_min`, `avg_hero_healing_per_min`, `avg_tower_damage_per_min` | DOUBLE PRECISION, nullable | average of each appearance's source total / (duration / 60.0) |
| `avg_match_duration_s` | DOUBLE PRECISION | average qualifying match duration in seconds |
| `kills_observations`, `deaths_observations`, `assists_observations`, `gpm_observations`, `xpm_observations`, `hero_damage_observations`, `hero_healing_observations`, `tower_damage_observations` | BIGINT | non-null observations contributing to each metric |

Missing values stay NULL; recorded zeros count. Each appearance has equal weight,
including for per-minute metrics. No observations means a NULL average and a zero
observation count. Counts and win rates use the same population as `hero_pick_win_rate`.
These are sample descriptions, not causal hero-strength rankings: skill, role,
duration and game version can affect comparisons. Steam damage totals need not use
the same inclusion rules as replay damage events. Full column definitions and
examples are in [hero-performance-contract.md](hero-performance-contract.md).

### `hero_matchup_summary`

**Grain:** one ordered hero/opponent pair in the ranked All Pick Steam sample.
**Key:** `(hero_id, opponent_hero_id)`.

Built from a self-join of `stg_steam_ranked` on match ID and opposing teams. Uses
the same qualification rules as the other hero marts; leavers are kept. NULL hero
IDs cannot form pairs. Unknown hero names do not remove observed pairs.

| Column | Type | Built from |
| ------ | ---- | ---------- |
| `hero_id`, `opponent_hero_id` | INTEGER | selected hero and opposing hero IDs from Steam |
| `hero_name`, `opponent_name` | TEXT, nullable | two left joins to `dim_heroes.localized_name` |
| `matches`, `wins`, `losses` | BIGINT | distinct match/hero/opponent observations; wins from the selected hero's side; losses = matches - wins |
| `win_rate` | DOUBLE PRECISION | wins / matches, from 0 to 1 |
| `overall_matches`, `overall_win_rate` | BIGINT, DOUBLE PRECISION | selected hero's picks and win rate from `hero_pick_win_rate` |
| `win_rate_difference_pp` | DOUBLE PRECISION | 100 * (win_rate - overall_win_rate), in percentage points |

Reverse pairs are separate rows with equal match counts and opposite outcomes.
One match contributes several opponent pairs, so their counts must not be summed
as distinct matches. The overall rate includes the pairing being examined.
Small samples can give unstable results; these are whole-match team outcomes,
not lane performance, direct damage or causal counter strength. Full definitions
and examples are in [hero-matchup-contract.md](hero-matchup-contract.md).

`kill_participation` retains `kills_from_events`, `deaths_from_events`, and published
`assists_from_events`. Full recorded totals remain in `match_scoreboard`. `assists_on_team_kills` is the subset
eligible for participation, and `kill_involvements = kills_from_events + assists_on_team_kills`.
The ratio is `kill_involvements / team_kills`. Both the numerator and ratio are NULL when
assist attribution is partial or unknown; the ratio is also NULL for teams with no credited kills.
The dashboard uses this same numerator in its count and percentage displays. Link-status labels
remain in clean output and do not silently remove otherwise established scoreboard totals.

Later: `death_context`, `economy_gap`.

## Assumptions to verify on a real parsed match

These original replay checks predate the counter-credit implementation. The current K/D/A contract above was checked on the ten local parsed matches; other game modes and fresh samples still need validation.

1. `heroes.m_iPlayerID / 2` equals `players.slot` (raw contract says `m_iPlayerID = 2 x slot`).
2. Combat-log flag columns exist under the names used (`attacker_hero`, `target_hero`, `attacker_illusion`, `target_illusion`). Raw contract lists only `attacker_hero` and `target_illusion` as examples.
3. Verified correction: `assist_players` already uses normalized slots 0-9, unlike `m_iPlayerID`. It is a participant list, not the assist-credit ledger.
4. A PURCHASE row's `target` is the buyer and `value` is a `names` index.
5. `m_flGameEndTime` is set by the last `gamerules` row.
6. Steam tables (`steam_matches`, `steam_players`) are not defined yet. See the next section for what is already known.

## Verified against real Steam pages (10 Nov)

Checked on 3 real `GetMatchHistoryBySequenceNum` pages (300 matches, 145 ranked All Pick, 1,450 players):

- Each match record carries the full scoreboard per player: `kills`, `deaths`, `assists`, `last_hits`, `denies`, `level`, `net_worth`, `gold` (unspent), `gold_spent`, `gold_per_min`, `xp_per_min`, `hero_damage`, `tower_damage`, `hero_healing`, `item_0`-`item_5`, `backpack_0`-`2`, `item_neutral`. So match-level data is already in the pages Winston collects.
- **Slot rule confirmed:** Steam `player_slot` is 0-4 Radiant and 128-132 Dire. `(ps & 127) + 5 * (ps >> 7)` equals `team_number * 5 + team_slot` for all 1,450 players.
- **Team numbering differs from the replay:** Steam `team_number` is `0` Radiant / `1` Dire. The replay uses `2` / `3`. Convert with `team_number + 2`.
- `hero_id` is Valve's number and is never 0, so it joins to `players.hero_id`. Items are numeric ids, not names: an item id-to-name lookup table is needed.
- `account_id` is anonymous (`4294967295`) for 840 of 1,450 players (58%). Never use it as a key.
- `leaver_status` is non-zero for 50 of 1,450 players (3.4%): treat those matches as lower quality.
- Team score (`radiant_score`, `dire_score`) is **not** the sum of player kills: it is at least that sum in every match, and equal in only 33 of 145. Do not use it as the kill-participation denominator. Compare player-level K/D/A instead.
- No end time: use `start_time + pre_game_duration + duration`.
