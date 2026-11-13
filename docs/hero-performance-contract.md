# Hero performance summary contract

Postgres table: `marts.hero_performance_summary`, built by
`dbt/models/marts/hero_performance_summary.sql`. This document defines its output
contract. The hero page loads the selected hero's performance profile from this table.

## Population and grain

One row per non-null `hero_id` observed in `stg_steam_ranked`. The key is `hero_id`.
The staging model contains one player appearance per `(match_id, player_slot)` and
keeps ranked All Pick matches only: lobby 7, mode 22, duration at least 900 seconds,
ten human players and a known winner. Matches with leavers remain included, matching
`hero_pick_win_rate`. A hero with no qualifying appearances has no row.

This is Steam summary data, independent of replay parsing and cleaning. Hero names
come from a left join to `dim_heroes`; an unknown name must not remove a hero.

## Output columns

All counts are `BIGINT`; averages and rates are `DOUBLE PRECISION` without display
rounding. Only `localized_name` and averages with no observations may be null.

| Column | Type | Definition |
| --- | --- | --- |
| `hero_id` | INTEGER | Unique, non-null Steam hero ID |
| `localized_name` | TEXT, nullable | Display name from `dim_heroes` |
| `appearances` | BIGINT | Number of qualifying player appearances for this hero |
| `wins` | BIGINT | Appearances where `radiant_win = on_radiant` |
| `losses` | BIGINT | `appearances - wins` |
| `win_rate` | DOUBLE PRECISION | `wins / appearances`, a ratio from 0 to 1 |
| `avg_kills` | DOUBLE PRECISION, nullable | Mean recorded final kills per appearance |
| `avg_deaths` | DOUBLE PRECISION, nullable | Mean recorded final deaths per appearance |
| `avg_assists` | DOUBLE PRECISION, nullable | Mean recorded final assists per appearance |
| `avg_gpm` | DOUBLE PRECISION, nullable | Mean Steam `gold_per_min`; not reconstructed from gold |
| `avg_xpm` | DOUBLE PRECISION, nullable | Mean Steam `xp_per_min` |
| `avg_hero_damage_per_min` | DOUBLE PRECISION, nullable | Mean of each appearance's `hero_damage / (duration / 60.0)` |
| `avg_hero_healing_per_min` | DOUBLE PRECISION, nullable | Mean of each appearance's `hero_healing / (duration / 60.0)` |
| `avg_tower_damage_per_min` | DOUBLE PRECISION, nullable | Mean of each appearance's `tower_damage / (duration / 60.0)` |
| `avg_match_duration_s` | DOUBLE PRECISION | Mean match duration in seconds across all appearances |
| `kills_observations` | BIGINT | Appearances with non-null kills |
| `deaths_observations` | BIGINT | Appearances with non-null deaths |
| `assists_observations` | BIGINT | Appearances with non-null assists |
| `gpm_observations` | BIGINT | Appearances with non-null `gold_per_min` |
| `xpm_observations` | BIGINT | Appearances with non-null `xp_per_min` |
| `hero_damage_observations` | BIGINT | Appearances with non-null hero damage |
| `hero_healing_observations` | BIGINT | Appearances with non-null hero healing |
| `tower_damage_observations` | BIGINT | Appearances with non-null tower damage |

## Calculation rules

- Average only recorded values. Missing values remain null; never substitute zero.
  Recorded zero values contribute to both the mean and its observation count.
- No observations means a null average and an observation count of zero.
- Duration is available and positive for every qualifying appearance, so each
  contribution rate's observation count equals its source field's non-null count.
- Per-minute rates are calculated per appearance before averaging. Each appearance
  has equal weight; this is not total damage divided by total minutes. For example,
  12,000 damage over 20 minutes and 12,000 over 40 minutes average to 450 damage/minute.
- GPM values 400, 600 and null average to 500 with two observations. Healing values
  0, 100 and null average to 50 with two observations.
- Counts and win rates must reconcile with `hero_pick_win_rate` for the same hero.
  Coverage counts lie between zero and appearances; wins plus losses equal appearances.
- Do not add a combined KDA ratio, composite strength score or sample-size filter to
  this mart. Preserve observed heroes and expose sample sizes for the dashboard.

## Interpretation

These are descriptive statistics for the collected sample, not a representative
estimate of all Dota matches or a causal hero-strength ranking. Player skill, role,
match duration and game version can affect comparisons. Per-minute normalisation
does not remove those differences. Expose appearances and metric coverage when
displaying results. Do not assume Steam hero-damage totals have the same event
inclusion rules as the replay-based `damage_by_pair` mart.

## Validation

Run the fixture checks against Postgres from the repository root:

```powershell
python scripts/test_hero_performance_postgres.py
```

The Compose warehouse must be running and the Airflow image must already be built.
The script creates a temporary database, loads `warehouse/fixtures/sample_steam.sql`,
seeds hero names, builds both hero marts and runs their dbt tests. It removes the
temporary database on success or failure. It does not load fixtures into the
dashboard database or start Airflow services.

The fixture covers wins and losses on both sides, different match durations,
recorded zeros, partially missing metrics, entirely missing metrics, unknown hero
names, NULL hero IDs and each existing match exclusion. Leavers remain included.
Existing hero 1 and hero 3 pick/win expectations are preserved.

`hero_performance_fixture_values` is enabled only with the dbt variable
`hero_performance_fixture_tests: true`, which the script supplies. Its expected-row
checks fail when required rows or fixture inputs are missing. Normal builds leave
this fixture-only test disabled. The outcome, metric coverage and cross-mart tests
run normally on the warehouse, including checks for missing heroes between marts,
invalid counts, non-finite averages and incorrect NULL handling.

To also check the dashboard's real Postgres loader and hero navigation against the
populated fixture, use the host environment with the dashboard dependencies and pytest:

```powershell
.\.venv\Scripts\python.exe scripts/test_hero_performance_postgres.py --dashboard-tests
```

These checks compare GPM, damage rates and observation counts with direct source
SQL, and open the hero page against the temporary database. They do not require a
parsed replay. Normal dashboard runs use the configured warehouse, not fixtures.
