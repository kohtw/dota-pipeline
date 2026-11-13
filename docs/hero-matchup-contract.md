# Hero matchup summary contract

Postgres table: `marts.hero_matchup_summary`, built by
`dbt/models/marts/hero_matchup_summary.sql`. This document defines its output
contract. The hero page loads opponent results for the selected hero from this table.

## Population and grain

One row per ordered `(hero_id, opponent_hero_id)` pair observed on opposing teams
in `stg_steam_ranked`. The composite key is `(hero_id, opponent_hero_id)`.
Slark against Axe and Axe against Slark are separate rows, each describing the
selected hero's perspective. Same-team appearances do not create opponent pairs.

Use the existing ranked All Pick sample: lobby 7, mode 22, duration at least 900
seconds, ten human players and a known winner. Matches with leavers remain included,
matching `hero_pick_win_rate` and `hero_performance_summary`. A pair with no
qualifying observations has no row. No minimum sample threshold is applied in the mart.

This is Steam summary data, independent of replay parsing and cleaning. Required
inputs are `match_id`, `hero_id`, `on_radiant` and `radiant_win` from staging.
`on_radiant` is derived from Steam's `player_slot`, not replay slot numbering.
Hero names come from two left joins to `dim_heroes`; unknown names do not remove pairs.

## Output columns

All counts are `BIGINT`; rates and percentage-point differences are `DOUBLE
PRECISION` without display rounding. Only the two hero names may be null.

| Column | Type | Definition |
| --- | --- | --- |
| `hero_id` | INTEGER | Selected hero's non-null Steam hero ID |
| `hero_name` | TEXT, nullable | Selected hero's `dim_heroes.localized_name` |
| `opponent_hero_id` | INTEGER | Enemy hero's non-null Steam hero ID |
| `opponent_name` | TEXT, nullable | Enemy hero's `dim_heroes.localized_name` |
| `matches` | BIGINT | Qualifying matches containing these heroes on opposing teams |
| `wins` | BIGINT | Pairing matches where `radiant_win = on_radiant` for the selected hero |
| `losses` | BIGINT | `matches - wins` |
| `win_rate` | DOUBLE PRECISION | `wins / matches`, a ratio from 0 to 1 |
| `overall_matches` | BIGINT | Selected hero's `picks` from `hero_pick_win_rate` |
| `overall_win_rate` | DOUBLE PRECISION | Selected hero's `win_rate` from `hero_pick_win_rate` |
| `win_rate_difference_pp` | DOUBLE PRECISION | `100 * (win_rate - overall_win_rate)`, in percentage points |

## Input checks

- Keep the staging filters unchanged so the three hero marts share one population.
- Check `(match_id, player_slot)` is unique and both fields are non-null.
- Steam player slots must be 0-4 Radiant or 128-132 Dire. Invalid slots fail a dbt
  test rather than being silently assigned to a team for matchup analysis.
- Each non-null hero ID must appear at most once per qualifying match, across both
  teams. Duplicate hero appearances fail a dbt test instead of inflating pair counts.
- Exclude null hero IDs from both sides of the pairing CTE. Keep these rows
  in shared staging; they must not change the existing qualifying-match denominator.
- A missing hero ID can leave fewer than five known opponents for a player. Do not
  fabricate an opponent or assume the observed pair counts sum to five times picks.

## Calculation rules

- Join known hero appearances within the same match where `on_radiant` differs.
  Count each `(match_id, hero_id, opponent_hero_id)` once.
- A pair's `matches` cannot exceed either hero's qualifying appearance count.
  Pairing counts overlap: one match normally contributes five enemy pairings for
  each hero. Summing pair counts is not a count of distinct matches.
- Every ordered pair must have its reverse, with equal match count. Forward wins
  equal reverse losses, and forward losses equal reverse wins.
- `wins + losses = matches`; win rates are finite and between 0 and 1.
- Overall context comes from the existing pick/win mart, not a sum across opponents.
  It includes the matchup being examined, so the difference is not a comparison
  against a separate control sample.
- Example: 3 wins from 10 pairing matches gives a 30% win rate. If the hero's overall
  rate is 45%, `win_rate_difference_pp` is -15, displayed as -15 percentage points.

## Interpretation

The mart describes whole-match team results when two heroes appeared on opposing
teams. It does not measure lane performance, direct damage, duels or causal counter
strength. The other eight players, skill, roles and game version can affect results.
Small samples can produce unstable rates. The dashboard must show match counts and
describe the feature as results against enemy heroes rather than best counters.

## Validation

Run the populated fixture and rejection checks from the repository root:

```powershell
python scripts/test_hero_performance_postgres.py --matchup-tests
```

The Compose warehouse must be running and the Airflow image must already be built.
The existing hero fixture runner creates a temporary Postgres database, loads
`warehouse/fixtures/sample_steam.sql`, seeds hero names, builds all three hero marts
and runs their tests. It removes the temporary database on success or failure.
The dashboard database is not seeded or rebuilt by this command.

The fixture preserves the existing performance and pick/win expectations. It also
checks repeated matchups with wins and losses, team changes, ally-only pairs,
unknown names, missing hero IDs and excluded matches. The fixture produces 190
ordered match-level pair observations and 78 aggregated ordered pairs.

`hero_matchup_fixture_values` is enabled only with the dbt variable
`hero_matchup_fixture_tests: true`, supplied by `--matchup-tests`. Expected-row
checks fail if required pairs are missing, including when the mart is empty.
Normal warehouse builds leave this fixture-only test disabled. Outcome, overall
context and reverse-pair checks run normally alongside key and not-null tests.

`hero_matchup_matches_steam_sources` independently builds Radiant and Dire hero
arrays from the source `steam_matches` and `steam_players` tables, expands their
opponent combinations in both directions and compares every pair's match, win
and loss counts with the mart. It bypasses the staging view and the model's
player self-join. Missing or extra mart pairs fail the check. The same test runs
on populated fixtures and normal Postgres warehouse builds; an empty source
requires an empty mart and does not establish real-data sample coverage.

After the valid fixture build, the runner introduces three temporary errors in
that test database: an invalid player slot, a duplicate hero within a match and a
deleted expected matchup row. Each targeted dbt test must report a data failure;
an infrastructure or compilation error is not accepted as a successful rejection.
The runner restores each case before continuing and removes the database afterward.

## Dashboard

**Results against enemy heroes** appears below the selected hero's performance
profile. Opponents start in descending match-count order, with search by name or
hero ID and a **Minimum matchup matches** filter. The table shows match counts,
wins, losses, matchup win rate, overall hero win rate and the difference in
percentage points. Selecting **Inspect an opponent** opens the same figures in a
detail view. Rates and differences stay numeric for sorting.

Filters and opponent selection are scoped to the selected hero and do not change
the hero URL or the existing **Minimum picks** control. Empty filters explain how
to recover. Unknown names fall back to `Hero <ID>`, and repeated names include the
ID in their labels. Missing or invalid matchup data leaves the other hero sections
usable. Inconsistent overall context asks for a rebuild rather than mixing samples.

Run the real Postgres loader and dashboard checks against the temporary populated
fixture with the host dashboard dependencies and pytest installed:

```powershell
.\.venv\Scripts\python.exe scripts/test_hero_performance_postgres.py --dashboard-tests
```

These checks compare the loader with stored mart values and exercise opponent
selection and filter recovery. The normal dashboard always reads its configured
warehouse; fixture records are never substituted into that warehouse.
