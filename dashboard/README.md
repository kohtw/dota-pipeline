# Dota 2 analytics dashboard

The Streamlit app runs as a Docker Compose service and reads dbt marts from the analytics
Postgres warehouse. Hero statistics shows aggregate picks, results, performance and enemy
matchups. Match analysis shows team net-worth advantage, the scoreboard, final player stats,
kill participation, net worth and XP over time, and hero damage by target and source. Items are
shown as the final main-inventory snapshot.

## Run with Docker

Keep Docker Desktop running and run these PowerShell commands from the repository root:

```powershell
docker compose up -d --build dashboard
```

Open [http://localhost:8501](http://localhost:8501). Compose builds the dashboard image, starts
the warehouse if needed and waits for it to be healthy. The dashboard does not require Airflow.
The image installs `dashboard/requirements.txt`; no host Python setup is required.

Compose requires `AIRFLOW_FERNET_KEY` in the repository's root `.env`, even when only
starting the dashboard. Follow [the stack setup](../CONTRIBUTING.md#running-locally) for a new
installation. Preserve an existing key and `.env`; copying `.env.example` over it erases its values.

Compose supplies the dashboard's `WAREHOUSE_*` settings. Inside the container, Postgres is
`warehouse:5432`; the published `localhost:5433` port is for host tools. No PowerShell database
variables are needed to run the dashboard. Use an account permitted to read the marts when
configuring another environment.

To start the full pipeline stack after its initial setup:

```powershell
docker compose up -d --build --remove-orphans
```

After dashboard code changes, rebuild it with the first command above. To rebuild only the
dashboard while the warehouse is already healthy:

```powershell
docker compose up -d --build --no-deps dashboard
```

Check service status and dashboard logs, or stop the dashboard:

```powershell
docker compose ps
docker compose logs --tail 100 dashboard
docker compose stop dashboard
```

## Warehouse data

Starting Postgres creates the source tables on a new volume; it does not process matches or build
marts. Match analysis requires the clean, load and dbt stages to have completed for at least one
real replay match. It reads `marts.match_scoreboard`, `marts.kill_participation`,
`marts.player_timeseries` and `marts.damage_by_pair`; team context reads
`marts.team_advantage_timeline`. Hero statistics reads `marts.hero_pick_win_rate`,
`marts.hero_performance_summary` and `marts.hero_matchup_summary`, built by dbt after Steam
summaries have loaded. Each section works independently; an empty API mart does not prevent
match analysis. Opening a page does not run ingestion or dbt.

Check the available analysed matches:

```powershell
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT match_id, COUNT(*) AS players FROM marts.match_scoreboard GROUP BY match_id ORDER BY match_id;"
```

## Using the dashboard

Open [http://localhost:8501](http://localhost:8501). The sidebar switches between **Hero statistics** and
**Match analysis**. A new visit opens Hero statistics. Opening that section does not query replay marts.

Match analysis selects the lowest available match ID when no match is specified. An analysed
match can also be opened with `?match_id=9079738443`, and a player with
`?match_id=9079738443&player_slot=0`, provided that match and player exist in the configured warehouse.
These existing links still open Match analysis. Explicit section links use `?view=heroes` or
`?view=matches`; switching sections clears the other section's hero or match/player parameters.
Within Match analysis, select a hero to see the player page and use **Compare with** to add another player.
The match header shows match ID, result and duration. Participants are identified by hero, team
and slot; account names are not part of the dashboard's mart contract.
Hero labels use Valve's `dim_heroes` display names across scoreboards, player pages, comparison
selectors and charts. Unknown heroes fall back to a readable replay identifier.

**Final inventory** on player pages shows six main slots in a 2×3 grid, three backpack slots,
separate neutral and TP slots, and six stash slots in a collapsed expander. Comparison mode gives
each player their own loadout. Labels come from `marts.match_inventory` and Valve's `dim_items`
seed. Exact slot positions and duplicate items are preserved; unknown or unnamed items use readable
fallback names. An absent slot says **Not recorded**, and a player without inventory rows shows
an unavailable message. The scoreboard keeps its compact **Final main inventory** column.

The **Progression time range** filters net worth and accumulated XP only. Final statistics, kill
participation, inventory and full-match damage remain independent of that range.

In **Hero damage dealt**, the summary lists each selected attacker, their total recorded damage to
heroes (including self-damage), and their share of their own team's recorded hero damage. Opposing
players use different team totals. In **Who did each player damage?**, rows identify the targets
receiving damage and colours identify the attackers. Bar labels show damage points; hovering names
both attacker and target and identifies self-damage. Other heroes appear as targets, not additional
comparison players. An absent bar means no damage recorded for that pair. The **Attack and ability
breakdown** lists attacker, target, self-damage status, source, damage and recorded hits.

Kill participation uses credited kills plus eligible assists on kills credited to the
player's team. The count beside the percentage uses that same numerator. Unusual
same-team assist credit stays in the displayed K/D/A but does not increase participation.

## Match search

The **Find a match** form accepts a numeric match ID with optional surrounding whitespace and
checks it against Postgres's positive BIGINT range. It rejects empty input, letters, decimals,
negative values and out-of-range numbers before querying the warehouse. Input is retained after
submission so it can be corrected.

Submitting **View match** opens the requested scoreboard and writes a shareable URL such as
`?view=matches&match_id=9079738443`. A search from a player page clears the player selection and
opens the new match's scoreboard. Existing links and the initial default-match display continue
to work.

If a valid ID is absent, the page identifies that match as unavailable and keeps the search form
visible. It does not silently display another match, download a replay or queue processing.
Connection failures are reported separately from unavailable matches. A valid search can be
retried or corrected using the same form.

**Available examples** offers up to five distinct match IDs read from `marts.match_scoreboard`,
ordered by ID. Clicking one opens its scoreboard through the same route as search, clears any
player or aggregate-hero selection, and updates the search field. The examples refresh from the
warehouse on each page run; raw replays and Steam-only matches are not advertised as analysed.
No example buttons appear when there are no analysed matches. If the optional example lookup
fails, a notice appears while match search and the current match remain usable.

On the current local reference setup, match `9079738443` has already passed cleaning, loading and
dbt and can be used to check the dashboard. Other installations should use an ID returned by the
available-match query above.

To demonstrate the search flow:

1. Open **Match analysis** and click an **Available examples** button, or enter `9079738443` and
   select **View match** on the current local setup.
2. Confirm the scoreboard's match ID matches the selection and refresh the page to check its link.
3. Select a hero in the scoreboard, then use **Compare with** to inspect a second player.
4. Submit a match ID from that player page; it opens the requested scoreboard and clears the comparison.
5. Try a malformed ID or an ID absent from the warehouse. The page explains the problem; correct
   the field or choose an available example to recover.

Search covers analysed replay matches in the scoreboard mart. An ID present only in parsed files,
Garage replay storage or Steam summary tables is not yet available for this view.

Each loader uses a short-lived read-only transaction. Connection attempts time out after five
seconds, statements after fifteen seconds, and connections close on success or failure. Queries
read the current marts on each page run.

## Team advantage

The match overview shows the Radiant and Dire scoreboards before **Team net-worth advantage**.
Player pages put team analysis at the bottom, after the player's statistics, inventory, damage
and progression. Both pages read team advantage from
`marts.team_advantage_timeline`. Positive values mean Radiant leads; negative values mean Dire
leads. Zero means equal net worth. This fixed perspective also applies when viewing a Dire player.
The metric describes economic position, not win probability or combat strength.

The full-match chart uses ten-second observations. Each team total requires five player
snapshots no older than five seconds. Missing coverage produces gaps, with complete-observation
counts and an expandable coverage table. Hover details show team totals and observed-player counts.

**Economic swings over two minutes** shows changes in that advantage: positive moves toward
Radiant, negative toward Dire. Both teams can gain net worth while the relative advantage shifts.
The chart identifies periods to review without assigning a cause. Short matches and intervals
with incomplete endpoints do not display invented changes.

On a player page, choose **Two-minute interval to review** and select **Review this interval**.
The existing progression range updates to those endpoints for player net worth and XP. Team
charts and final statistics, inventory, kill participation and full-match damage stay unchanged.
Player progression charts keep every recorded measurement and missing-value gap, with fine lines
and regular game-clock ticks. Dense series hide overlapping markers; sparse series and isolated
measurements keep small markers. Hover details show exact recorded values for each player.
If the team mart is missing or invalid, other match analysis remains available. Calculation rules
and Postgres validation are in [the team advantage contract](../docs/team-advantage-contract.md).

## Hero statistics

The page describes the collected sample of ranked All Pick matches lasting at least 15 minutes,
with ten human players and a known winner. It displays the dataset's qualifying match count and
number of observed heroes, a searchable hero selector, and the selected hero's picks, wins,
losses, pick rate and win rate.

- Pick rate is picks divided by qualifying matches; win rate is wins divided by picks.
- Losses are picks minus wins. Pick counts provide context for rates from small samples.
- The match count repeats in every mart row and is displayed once rather than summed.
- Heroes without observations are absent. A missing name is shown as `Hero <ID>`.
- Missing, non-finite or out-of-range rates display as unavailable rather than zero.
- The sortable comparison table starts with the most-picked heroes. **Minimum picks** filters
  this table while keeping the selected hero and dataset-wide totals visible.
- An empty mart displays **No hero statistics available yet**; test records are never substituted.

Hero selections are saved in the URL, for example `?view=heroes&hero_id=1`. Opening that link in
a new session restores the same hero if observations are present. Changing **Minimum picks**
leaves the selected hero and URL unchanged; the threshold itself is local to the page session.
Malformed or repeated hero parameters show an error and leave the selector unselected until a
valid choice is made. An unobserved hero shows a notice instead of invented zero-valued metrics.
The comparison table remains available to help choose another hero.

The page's data contract is `marts.hero_pick_win_rate`:

| Column | Type | Meaning |
|---|---|---|
| `hero_id` | Integer | Stable hero identifier and selector value |
| `localized_name` | Nullable text | Display name; falls back to `Hero <ID>` |
| `picks`, `wins` | Integer counts | Hero observations and wins |
| `ranked_matches` | Integer count | Shared number of qualifying matches |
| `pick_rate`, `win_rate` | Floating-point ratios | Values from 0 to 1, displayed as percentages |

Losses are derived for display. Counts and ratios remain numeric, including in the sortable table.
The loader checks hero-ID uniqueness, valid counts and a consistent shared match denominator.
Offline hero tests use controlled records matching this contract; a populated API dataset is
not required to run those tests. The running app always reads the configured warehouse.

The selected hero's **Performance profile** reads `marts.hero_performance_summary`:

- **Combat** shows average final kills, deaths and assists.
- **Economy** shows average Steam-reported gold and XP per minute.
- **Contribution** shows average hero damage, hero healing and tower damage per minute.
- Combat, Economy and Contribution appear in separate bordered cards, with vertically stacked
  metrics and dividers. Recorded counts appear when coverage is incomplete. Missing values display
  as unavailable; recorded zeros remain zero and contribute to the average.
- Per-minute contribution is calculated for each appearance before averaging, with equal weight
  per appearance. The profile also shows its appearance count and average match length.

These averages describe the collected sample, not performance targets or a hero-strength ranking.
Skill, role and game version can affect comparisons. The **Minimum picks** control still filters
only the comparison table; it does not change the selected hero's profile. A missing, invalid or
unavailable performance mart leaves pick/win statistics and navigation usable. When appearance
or win counts disagree between hero marts, the profile asks for a rebuild rather than combining
inconsistent summaries. Build both hero marts after loading Steam data. Full metric definitions
and Postgres validation commands are in [the performance contract](../docs/hero-performance-contract.md).

**Results against enemy heroes** reads `marts.hero_matchup_summary` for the selected hero:

- Opponents start with the most-observed pairings. Search by opponent name or hero ID, or use
  **Minimum matchup matches** to hide smaller samples. Small samples can give unstable rates;
  the filter does not guarantee reliable results.
- The sortable table shows matches, wins, losses, matchup win rate and **Difference (pp)**.
  An explanation above the table defines the difference from the hero's overall win rate, already
  displayed in the hero summary, and gives a numerical example.
- **Inspect an opponent** opens a detail view from the selected hero's perspective. Opponent
  controls are scoped to that hero and leave the hero URL and **Minimum picks** unchanged.
- These are whole-match team outcomes from Steam summaries, not lane results, direct combat
  or causal counters. Other players, skill, roles and game version affect comparisons. Counts
  overlap across opponents and must not be added to obtain distinct matches.
- Missing, invalid or unavailable opponent results leave pick/win statistics and the performance
  profile usable. Empty filters offer a recovery message; unknown names use `Hero <ID>`.

The [matchup contract](../docs/hero-matchup-contract.md) documents the fields and Postgres checks.

## Troubleshooting

- Missing `WAREHOUSE_*` setting: check the dashboard service's Compose environment and recreate
  it after correcting the configuration. PowerShell variables are not needed for the standard setup.
- Connection failure: check Docker, the warehouse's health and the configured host, port and credentials.
- Rejected login: check `WAREHOUSE_USER` and `WAREHOUSE_PASSWORD`. Some connection-time errors are
  reported as a general connection failure because the driver supplies no structured error code.
- Missing or out-of-date marts: run the pipeline's dbt build after the source data has loaded.
- No matches: the scoreboard mart exists but contains no analysed matches yet.
- No hero statistics: the hero mart is empty; load Steam summaries and build dbt to populate it.
- Invalid hero counts or inconsistent match totals: inspect and rebuild the hero mart before using its rates.
- Unavailable match or player: the requested ID is not present in the configured warehouse.
- Permission failure: the database account needs access to the `marts` schema and its queried tables.
- Query cancelled or timed out: retry the page; if it persists, check the warehouse's workload.

The app displays fixed database error messages rather than raw driver details. A failure in the
optional damage query leaves the player's other statistics visible. A failed progression query
stops the player page from showing incomplete charts. Invalid links provide a route back to an
available scoreboard.

## Final-slot inventory loader

The final-slot inventory loader `load_player_inventory` reads `marts.match_inventory` for the
selected match and player, with an optional comparison player, in one parameterized read-only query.
It returns a mapping from player slot to records ordered by `inventory_slot`, each containing
`inventory_slot`, `location`, `item_name` and `item_display_name`. Duplicate items in different
slots stay separate. A player without records maps to `None`; omitted slots have unknown coverage
and are not labelled empty. The current mart has no coverage flag to confirm an empty loadout.
Duplicate slots, invalid slot/location combinations and missing names raise a database error.
Player pages load inventory independently; a failed inventory query leaves other analysis available.

## Tests

The default dashboard tests run offline with controlled cursor results and mocked app loaders.
They exercise configuration, connection cleanup, record mapping, hero metrics and filtering,
bookmark recovery, missing/zero values, match-ID validation and submission, example selection,
unavailable-match recovery, and navigation between independently available sections.
The tests use mocked warehouse access and need no Steam key or live warehouse. Run them in a
temporary dashboard container with the repository mounted read-only. The command installs pytest
inside that disposable container; it does not change the dashboard image or host environment.
Docker and the root Compose `.env` are required; the initial pytest installation needs network access.

```powershell
docker compose run --rm --no-deps --entrypoint sh -e PYTHONDONTWRITEBYTECODE=1 -v "${PWD}:/workspace:ro" -w /workspace dashboard -c "python -m pip install --user pytest && python -m pytest -p no:cacheprovider tests/test_dashboard_connection.py tests/test_dashboard_db.py tests/test_dashboard_app.py tests/test_hero_statistics.py tests/test_match_search.py tests/test_team_advantage_dashboard.py -q"
```

To check the real Postgres loaders and player navigation, keep the warehouse running and supply
an already analysed match. The test container inherits the Compose database settings. These
integration checks only read the warehouse and do not seed, delete or rebuild its data.

The live suite compares scoreboard values, kill participation, inventories, progression and damage
with direct SQL. It also checks read-only mode, transaction-local query cancellation, cleanup after
errors, real match search and example buttons, player comparison/navigation and the current hero
mart/page. An empty API mart is a valid live result and must produce the empty state. Offline tests
remain responsible for populated hero examples, controlled zero/null cases, missing dependencies,
invalid settings and unavailable-data messages.

```powershell
docker compose run --rm --no-deps --entrypoint sh -e PYTHONDONTWRITEBYTECODE=1 -e DASHBOARD_TEST_MATCH_ID=9079738443 -e DASHBOARD_TEST_TEAM_ADVANTAGE=1 -v "${PWD}:/workspace:ro" -w /workspace dashboard -c "python -m pip install --user pytest && python -m pytest -p no:cacheprovider tests/test_dashboard_postgres.py tests/test_team_advantage_dashboard.py -q"
```

Replace the example match ID with one returned by the available-match query. The opt-in team
advantage check also requires an analysed replay with at least one complete two-minute interval.
