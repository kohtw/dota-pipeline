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
- **Real heroes only:** drop any hero entity whose `m_hReplicatingOtherHeroModel` is not `16777215` (illusions). Drop combat-log rows where an illusion is the attacker or target.
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

**Grain:** one real hero death. **Key:** `(match_id, tick, victim_slot)`.

From `DOTA_COMBATLOG_DEATH` rows whose target is a real hero.

| Column                | Type              | Built from                                             |
| --------------------- | ----------------- | ------------------------------------------------------ |
| `tick`, `game_time_s` |                   |                                                        |
| `victim_slot`         | INTEGER           | target hero to slot                                    |
| `killer_slot`         | INTEGER, nullable | attacker hero to slot; NULL if a creep or tower killed |
| `killer_name`         | TEXT              | attacker name                                          |

### `death_assists`

**Grain:** one player credited with an assist on one death. **Key:** `(match_id, tick, victim_slot, assist_slot)`.

Split the `;`-separated `assist_players` string of the death row; each player ID divided by 2 is an `assist_slot`.

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
| `player_timeseries`  | one player at one second            | net worth and XP over time                          |
| `damage_by_pair`     | attacker, target, source in a match | who damaged whom, and share of team hero damage     |
| `kill_participation` | one player in one match             | share of team kills the player took part in         |
| `hero_performance_summary` | one hero in qualifying Steam matches | average combat, economy and contribution metrics with observation counts |
| `hero_matchup_summary` | one ordered hero/opponent pair in qualifying Steam matches | whole-match results against each enemy hero and overall win-rate context |
| `team_advantage_timeline` | one replay match at each ten-second observation time | Radiant versus Dire net worth, complete-team coverage and two-minute economic swings |

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

Later: `death_context`, `economy_gap`.

## Assumptions to verify on a real parsed match

Written from the raw contract alone. Nobody has checked these against real Parquet yet.

1. `heroes.m_iPlayerID / 2` equals `players.slot` (raw contract says `m_iPlayerID = 2 x slot`).
2. Combat-log flag columns exist under the names used (`attacker_hero`, `target_hero`, `attacker_illusion`, `target_illusion`). Raw contract lists only `attacker_hero` and `target_illusion` as examples.
3. `assist_players` holds the same IDs as `m_iPlayerID`.
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
