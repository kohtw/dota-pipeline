# Team advantage timeline contract

Postgres table: `marts.team_advantage_timeline`, built by
`dbt/models/marts/team_advantage_timeline.sql`. Player alignment is defined in
`dbt/models/staging/stg_replay_player_observations.sql`. This document defines the
output contract. The match overview and player pages read this mart for team
context and economic review intervals.

## Population and grain

One replay match at one aligned game-clock observation time. The composite key is
`(match_id, observation_time_s)`. Inputs are `clean.matches`, `clean.players` and
`clean.player_stats`; no Steam summary data is used.

Use a fixed ten-second grid from zero through `floor(duration_s / 10) * 10`,
inclusive. Match duration must be positive and finite. Require exactly five
distinct roster players per team, with clean teams 2 (Radiant) and 3 (Dire),
player slots 0-4 and 5-9 respectively. Invalid duration or roster structure must
fail validation rather than produce a partial team estimate.

## Output columns

| Column | Type | Definition |
| --- | --- | --- |
| `match_id` | BIGINT | Replay match identifier |
| `observation_time_s` | INTEGER | Requested game-clock time, in ten-second increments |
| `radiant_net_worth` | BIGINT, nullable | Sum of all five usable Radiant player observations |
| `dire_net_worth` | BIGINT, nullable | Sum of all five usable Dire player observations |
| `net_worth_advantage` | BIGINT, nullable | Radiant net worth minus Dire net worth |
| `radiant_players_observed` | BIGINT | Usable Radiant observations, from 0 to 5 |
| `dire_players_observed` | BIGINT | Usable Dire observations, from 0 to 5 |
| `max_snapshot_age_s` | DOUBLE PRECISION, nullable | Largest age among selected snapshots across both teams; NULL if none were selected |
| `net_worth_advantage_change_120s` | BIGINT, nullable | Current advantage minus advantage at exactly observation_time_s - 120 |

## Alignment rules

- The `stg_replay_player_observations` model has one row per roster player
  at each requested grid time. Keep selected snapshot tick, game time, age, net
  worth and usability so totals and later player contributions can be reconciled.
- For each player, select the latest snapshot at or before the requested time,
  ordered by game time descending and then tick descending. Never select a future
  snapshot, even if it is closer to the requested time.
- Select the latest snapshot first, then assess its validity. Do not skip a newer
  NULL or negative net-worth value to retrieve an older valid value.
- A usable observation has non-null, nonnegative net worth and a snapshot age from
  zero through five seconds, inclusive. Missing observations are not zero values.
- Pregame snapshots can support the zero-time observation only if they meet the
  same freshness rule. Snapshots beyond the recorded match duration are not used.
- `max_snapshot_age_s` describes all selected snapshots, including stale ones or
  ones with unusable net worth. It is a coverage diagnostic, not a guarantee that
  every selected snapshot contributed to a team total.
- Invalid snapshot keys, non-finite times, or times that move backward with tick
  must be flagged in validation. Do not silently repair them by averaging rows.

## Calculation rules

- Produce a team total only when all five roster players have usable values.
  A complete team's total may remain available when the opposing team is incomplete.
- Calculate advantage only when both team totals are available. Positive means
  Radiant leads, negative means Dire leads, and zero means equal net worth.
- Calculate the two-minute change by joining the exact earlier grid time, not by
  assuming twelve preceding rows always represent 120 seconds.
- Before 120 seconds, or if either advantage endpoint is unavailable, the change
  is NULL. Keep incomplete grid rows so the dashboard can show gaps.
- A positive change means the balance moved toward Radiant; it does not necessarily
  mean the current leader changed. A negative change means it moved toward Dire.
- Example: both teams begin at 20,000. Two minutes later Radiant has 22,000 and
  Dire has 25,000. Advantage changes from zero to -3,000 even though both teams gained.
- Use recorded net worth, not unspent gold or total earned gold. Do not round
  values in the mart or convert incomplete sums into team totals.

## Missing-data and boundary behaviour

| Situation | Required output |
| --- | --- |
| Recorded net worth is zero | Usable when fresh; include zero in the team sum |
| No snapshot exists at or before the grid time | NULL snapshot fields, unusable player observation |
| Latest snapshot has NULL or negative net worth | Unusable; retain its identity and do not fall back |
| Snapshot age is exactly five seconds | Usable when net worth is valid |
| Snapshot age exceeds five seconds | Unusable; retain its age for diagnostics |
| A future snapshot is closer than the preceding one | Use only the preceding snapshot, subject to freshness |
| One player lacks a usable observation | Affected team total NULL; retain the grid row |
| Opposing team still has five usable observations | Preserve that team's total; advantage remains NULL |
| Either two-minute endpoint is unavailable | Change NULL, even if the other endpoint is complete |
| Observation time is below 120 seconds | Two-minute change NULL |
| Duration is not divisible by ten | Stop at the preceding complete ten-second boundary |
| Duration or roster structure is invalid | dbt validation fails; do not accept partial team estimates |

`replay_observation_alignment_rules` checks snapshot identity, latest-at-or-before
selection and the inclusive freshness rule. `team_advantage_missing_data_rules`
checks NULL propagation, coverage bounds, the advantage sign convention and exact
two-minute endpoint arithmetic. `team_advantage_grid_rules` checks that every
match retains its expected grid. Input and uniqueness checks remain in place.

## Interpretation

This is sampled economic advantage, not win probability or total combat strength.
As-of alignment permits small differences between players' actual snapshot times.
The dashboard must identify the Radiant-minus-Dire perspective and show missing
coverage. It may invert the sign for a selected team's view without changing the
stored convention. Large changes identify periods to review; the metric alone
does not identify their cause.

## Input audit

Checked the local Postgres warehouse on 14 Nov 2026 with
`scripts/audit_team_advantage_inputs.sql`. The audit runs in a read-only transaction:

```powershell
Get-Content -Raw scripts/audit_team_advantage_inputs.sql | docker compose exec -T warehouse psql -X -v ON_ERROR_STOP=1 -U warehouse -d warehouse
```

| Check | Observed result |
| --- | --- |
| Replay matches available | 1: `9079738443` |
| Recorded match duration | 531.3333740234375 seconds |
| Roster | 5 Radiant, 5 Dire; no invalid slot/team mappings |
| Player snapshots | 9,180 total; 918 per player |
| Snapshot time range | -242 through 675 seconds for every player |
| Median, 99th-percentile and maximum consecutive gap | 1 second each |
| Missing or negative net worth | 0 |
| Non-finite or nonincreasing snapshot times | 0 |
| Duplicate snapshot keys or snapshots without roster players | 0 |
| Difference from documented tick-to-clock formula | 0 seconds |
| Pregame snapshots | 2,420 |
| Snapshots beyond recorded match duration | 1,440 |
| Proposed grid coverage | All 54 observations complete, from 0 through 530 seconds |
| Oldest selected snapshot age on the grid | 0 seconds, including at time zero |

Retain the proposed five-second freshness limit: this replay needs no extension
to obtain complete coverage. This audit verifies input availability and alignment
feasibility for one short replay only; it does not validate a built timeline mart
or establish coverage for other replays. Re-run it when additional matches load.
The snapshot stream extends beyond the recorded duration, so the grid must use
the recorded match boundary rather than the last available snapshot time.

## Initial model validation

Built both models against the existing Postgres replay on 14 Nov 2026. All 17
input, key, alignment, grid, reconciliation, missing-data and not-null checks passed.
The mart contains 54 complete observations
from zero through 530 seconds, with 42 available two-minute changes. The table
model built in about one second on this replay.

All 54 team totals and advantage values matched a separate query grouping the
source snapshots at exact grid times. This direct comparison is possible because
this replay has exact-time snapshots at every grid point. Every two-minute change
also matched its exact endpoint difference. All selected snapshot ages were zero.
This real replay does not exercise missing-data and freshness-boundary cases;
the controlled fixtures below cover them separately.

## Controlled Postgres validation

Run the fixture checks from the repository root:

```powershell
python scripts/test_team_advantage_postgres.py
```

The Compose warehouse must be running and the Airflow image must already be built.
The script creates a temporary database, applies the clean-table DDL and loads
`warehouse/fixtures/sample_team_advantage.sql`. It builds only the aligned-player
view and team advantage mart, then removes the database on success or failure.
It does not seed the dashboard warehouse.

| Fixture | Behaviour checked |
| --- | --- |
| `9200000001` | Both teams gain while Radiant loses relative advantage; lead later changes sides; duration 135 stops the grid at 130 despite later snapshots |
| `9200000002` | Zero net worth remains valid; newer NULL and negative values do not fall back; complete coverage can recover |
| `9200000003` | Missing earlier or current endpoints leave two-minute changes NULL |
| `9200000004` | Pregame snapshots support time zero; age exactly 5 is usable, age 5.001 is stale; closer future snapshots are ignored |
| `9200000005` | A valid roster with no snapshots retains every grid row, with zero observed players and NULL totals |
| `9200000006` | Equal snapshot times choose the higher tick deterministically |

Together these produce 370 aligned-player rows and 37 timeline rows, including two
available two-minute changes. `team_advantage_fixture_values` checks hand-calculated
answers and requires expected rows to exist. It is enabled only by the dbt variable
`team_advantage_fixture_tests: true`, supplied by the runner. Normal warehouse builds
leave this fixture-only test disabled.

`replay_observation_grid_complete` checks each roster player's full grid.
`team_advantage_matches_sources` ranks raw snapshot candidates with a window
function, independently of the staging model's lateral selection, then compares
both team totals, player counts, maximum snapshot age and advantage at every time.
Both checks run on fixtures and normal Postgres builds, alongside the existing
input, alignment, uniqueness and missing-data checks.

After the valid build, the runner temporarily supplies an invalid duration and an
invalid roster, checks that the snapshot primary key rejects a duplicate, and
deletes a required timeline point to confirm that fixture validation fails. Each
mutation is confined to the temporary database. Invalid-input tests must report
data failures; compilation or connection errors do not count as successful rejection.

## Dashboard

**Team net-worth advantage** shows the full-match Radiant-minus-Dire timeline on
the match overview and near the top of each player page. Above zero means Radiant
is ahead; below zero means Dire is ahead. The sign convention stays the same when
selecting a Dire player or comparing players across teams.

The page reports complete observation coverage and leaves missing values as chart
gaps. **Inspect incomplete coverage** lists the affected times, player counts and
oldest selected snapshot age. Charts do not connect across missing observations.

**Economic swings over two minutes** plots each complete endpoint difference.
Positive changes moved toward Radiant and negative changes moved toward Dire,
regardless of which team currently leads. Intervals without both complete endpoints
are unavailable, including the first two minutes and short matches.

On a player page, select **Two-minute interval to review**, then **Review this
interval** to update the existing progression range. This filters player net-worth
and XP charts only. Team charts, final statistics, inventory and full-match damage
remain unchanged. The match overview directs users to open a player for interval
review. A missing or invalid team mart leaves other match analysis available.

Offline dashboard checks cover the sign convention, measured zero, missing-data
gaps, interval application, comparison and optional-mart failure. The opt-in
`DASHBOARD_TEST_TEAM_ADVANTAGE=1` check in
`tests/test_team_advantage_dashboard.py` reads the configured Postgres warehouse,
compares the loader with stored mart values and applies a real interval in the
player page. It requires an analysed replay with at least one complete two-minute
interval and does not seed or change warehouse data.
