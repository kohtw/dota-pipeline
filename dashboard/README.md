# Dota 2 analytics dashboard

The Streamlit app reads dbt marts from the analytics Postgres warehouse. Hero statistics shows
aggregate hero picks and results. Match analysis shows the scoreboard, final player stats, kill
participation, net worth and XP over time, and hero damage by target and source. Items are shown
as the final main-inventory snapshot.

## Install dependencies

Run these PowerShell commands from the repository root. The dashboard was verified with Python
3.14. Use your existing `.venv` if it has already been created.

```powershell
if (-not (Test-Path .\.venv\Scripts\python.exe)) { python -m venv .venv }
.\.venv\Scripts\python.exe -m pip install -r dashboard/requirements.txt
```

## Prepare the warehouse

Keep Docker Desktop running and start the warehouse if needed:

```powershell
docker compose up -d warehouse
```

Compose also requires `AIRFLOW_FERNET_KEY` in the repository's root `.env`, even when only
starting the warehouse. Follow [the stack setup](../CONTRIBUTING.md#running-locally) for a new
installation. Preserve an existing key and `.env`; copying `.env.example` over it erases its values.

Starting Postgres creates the source tables on a new volume; it does not process matches or build
marts. Match analysis requires the clean, load and dbt stages to have completed for at least one
real replay match. It reads `marts.match_scoreboard`, `marts.kill_participation`,
`marts.player_timeseries` and `marts.damage_by_pair`. Hero statistics reads `marts.hero_pick_win_rate`,
built by dbt after Steam summaries have loaded. Each section works independently; an empty API mart
does not prevent match analysis. Opening a page does not run ingestion or dbt.

Check the available analysed matches:

```powershell
docker compose exec warehouse psql -U warehouse -d warehouse -c "SELECT match_id, COUNT(*) AS players FROM marts.match_scoreboard GROUP BY match_id ORDER BY match_id;"
```

## Configure and run

Set warehouse variables in the same PowerShell session that runs Streamlit. Compose reads the
root `.env` for its own services; it does not export those values into this session.

```powershell
$env:WAREHOUSE_HOST = "localhost"
$env:WAREHOUSE_PORT = "5433"
$env:WAREHOUSE_DB = "warehouse"
$env:WAREHOUSE_USER = "warehouse"
$env:WAREHOUSE_PASSWORD = "warehouse" # Local Compose development credential only.

.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
```

Host, port, database and user default to the values above when absent; the password must be set
explicitly. A dashboard running on the Compose network should use host `warehouse` and port `5432`.
Use an account permitted to read the marts when connecting to another environment.

Open the local URL printed by Streamlit. The sidebar switches between **Hero statistics** and
**Match analysis**. A new visit opens Hero statistics. Opening that section does not query replay marts.

Match analysis selects the lowest available match ID when no match is specified. An analysed
match can also be opened with `?match_id=9079738443`, and a player with
`?match_id=9079738443&player_slot=0`, provided that match and player exist in the configured warehouse.
These existing links still open Match analysis. Explicit section links use `?view=heroes` or
`?view=matches`; switching sections clears the other section's hero or match/player parameters.
Within Match analysis, select a hero to see the player page and use **Compare with** to add another player.
The match header shows match ID, result and duration. Participants are identified by hero, team
and slot; account names are not part of the dashboard's mart contract.

**Final main inventory** shows the main item slots at match end, including passive items. The
**Progression time range** filters net worth and accumulated XP only. Final statistics, kill
participation, inventory and full-match damage remain independent of that range.

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

## Troubleshooting

- Missing `WAREHOUSE_*` setting: set the reported variable in the terminal running Streamlit.
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

## Tests

The default dashboard tests run offline with controlled cursor results and mocked app loaders.
They exercise configuration, connection cleanup, record mapping, hero metrics and filtering,
bookmark recovery, missing/zero values, match-ID validation and submission, example selection,
unavailable-match recovery, and navigation between independently available sections.
No Steam key, Docker services or warehouse login is needed for this offline suite.

```powershell
.\.venv\Scripts\python.exe -m pip install pytest
.\.venv\Scripts\python.exe -m pytest tests/test_dashboard_connection.py tests/test_dashboard_db.py tests/test_dashboard_app.py tests/test_hero_statistics.py tests/test_match_search.py -q
```

To check the real Postgres loaders and player navigation, set `WAREHOUSE_*` as above and supply
an already analysed match. These integration checks only read the warehouse and do not seed,
delete or rebuild its data.

The live suite compares scoreboard values, kill participation, inventories, progression and damage
with direct SQL. It also checks read-only mode, transaction-local query cancellation, cleanup after
errors, real match search and example buttons, player comparison/navigation and the current hero
mart/page. An empty API mart is a valid live result and must produce the empty state. Offline tests
remain responsible for populated hero examples, controlled zero/null cases, missing dependencies,
invalid settings and unavailable-data messages.

```powershell
$env:DASHBOARD_TEST_MATCH_ID = "9079738443"
.\.venv\Scripts\python.exe -m pytest tests/test_dashboard_postgres.py -q
Remove-Item Env:DASHBOARD_TEST_MATCH_ID
```
