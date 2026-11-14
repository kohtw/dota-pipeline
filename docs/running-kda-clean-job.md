# Counter-based Spark cleaning

`spark_jobs/clean_replays.py` is the main Spark entry point. It produces all nine
clean tables, with the established transformations for the seven tables unrelated
to death credit and counter-established deaths/assists. The legacy event-credit
implementation has been removed; there is no separate `_kda` cleaner file.

Spark handles Parquet reads, slot mapping, name resolution, joins, validation and
Parquet writes. One match's compact counter history and death records are collected
on the driver for the bounded constraint solver included in
`spark_jobs/clean_replays.py`. No separate credit module is required.
This is not a fully distributed attribution solver.

## Required input

All existing parsed tables and their `_complete/match_id=<id>/` marker, plus:

```text
<input_base>/kda_changes/match_id=<id>/part-*.parquet
```

The new table uses raw player-resource columns:

| Column | Type |
| --- | --- |
| `tick` | integer |
| `index` | integer |
| `m_vecPlayerData.m_iPlayerTeam` | integer |
| `m_vecPlayerTeamData.m_iTeamSlot` | integer |
| `m_vecPlayerTeamData.m_nSelectedHeroID` | integer |
| `m_vecPlayerTeamData.m_iKills` | integer |
| `m_vecPlayerTeamData.m_iDeaths` | integer |
| `m_vecPlayerTeamData.m_iAssists` | integer |

Extract every tick and retain the initial zero counters and subsequent changes.
Team/slot/hero identity changes must also emit rows so the cleaner can map early
rows to the roster. Multiple player indices for a slot, or an index changing slots,
are rejected. `match_id` is supplied by the partition; if physically present it must
agree with that partition.

The parser now produces this table using a per-table sampling interval of one tick
and `changes_only=true`. Rebuild the parser image before re-parsing old replays:

```bash
docker build -t dota-parser:kda -f parser/Dockerfile .
docker run --rm \
  -v "$PWD/data/output:/demos:ro" \
  -v "$PWD/data:/out" \
  dota-parser:kda "/demos/*.dem" /out/parsed
```

Renaming the once-per-second `player_resource` table does not provide the missing
precision. The clean job fails with a review report if `kda_changes` is absent.

## Run locally

Use a Python environment with `spark_jobs/requirements.txt` installed and Java 17+
available to Spark. From the repository root:

```bash
python spark_jobs/clean_replays.py \
  data/parsed data/output/clean_kda 9079737909
```

To inspect a preview without a completion marker:

```bash
python spark_jobs/clean_replays.py \
  data/parsed data/output/clean_kda 9079737909 --preview
```

The default counter window is the event tick through four ticks after it, inclusive.
`--window-ticks` and `--max-search-states` configure this window and the search bound.
An exhausted search is a blocking error, never assumed to be a unique solution.

Local and `s3a://` paths use the same conventions as the original cleaner. For S3,
provide the existing S3 environment variables and connector JARs. To run in the
existing clean image, mount `spark_jobs/` and select the new entry point:

```bash
docker run --rm \
  -v "$PWD/spark_jobs:/jobs:ro" \
  -v "$PWD/data/parsed:/input:ro" \
  -v "$PWD/data/output:/output" \
  --entrypoint python dota-spark-clean \
  /jobs/clean_replays.py /input /output/clean_kda 9079737909
```

## Output

Use a separate output root such as `data/output/clean_kda` while evaluating the new
contract. No database load runs automatically.

```text
<output_base>/{table}/match_id=<id>/part-*.parquet
<output_base>/_review/match_id=<id>/report.json
<output_base>/_review/match_id=<id>/{finding}/part-*.parquet
<output_base>/_complete/match_id=<id>/_SUCCESS
```

The seven other tables retain their existing schemas. Credit tables add:

| Table | Added column | Meaning |
| --- | --- | --- |
| `deaths` | `credited_killer_slot` | Player whose kill counter rose; null if nobody received credit |
| `deaths` | `link_status` | `exact`, `participant_assumed`, or `same_side_possible` |
| `death_assists` | `same_side` | Whether credited assister and victim belong to the same team |

`killer_slot`/`killer_name` preserve the direct last hitter. A tower can have a name
and a null player slot while `credited_killer_slot` identifies a credited player.
The death tick and `game_time_s` refer to the combat-log event, not the counter tick.

Counters must start at zero, never decrease and end at the independently sampled
final scoreboard. Every increment must be consumed once. The Spark validation also
recounts output using `credited_killer_slot` and requires exact final K/D/A equality.
Game-recorded same-team assists, including a victim whose own assist counter rises,
are retained with `same_side=true`; they are not filtered out as impossible.

The completion marker is removed before rebuilding and written last only when
validation passes outside preview mode. Blocked or preview runs hold death and
assist tables under `_review/.../candidates/`. An attribution/input failure returns
exit code 2, writes its error in `report.json`, and leaves no completion marker.
Other runtime errors also leave no marker and propagate to the caller.

The clean contract, warehouse DDL/migration and loader now support these columns.
The loader rejects legacy credit files and preserves labels unchanged. Apply the repeat-safe
warehouse init SQL before loading; `load_pending` does this before its mapped loads.
Historical database rows retain unknown credit fields until re-cleaned and reloaded.
The dbt models now use `credited_killer_slot`, preserve all assists for K/D/A reconciliation,
and use team-kill-eligible assists for the participation numerator. After migrating and loading,
rebuild the Postgres marts with the explicit production adapter target:

```bash
dbt build --project-dir dbt --profiles-dir dbt --target prod
```

Provide the project's `WAREHOUSE_*` environment variables. Running the cleaner itself does not
load Postgres or rebuild dbt marts. Integration checks use a separate temporary Postgres database;
they do not update the live warehouse.

## Tests

```bash
python -m pytest tests/test_kda_credit.py tests/test_spark_kda_clean.py -q
```

Tests cover the credit rules independently, plus a Spark tower-last-hit fixture,
strict nine-table validation, output columns, partition writes, preview reruns,
missing inputs, wrong match IDs and invalid mappings/assist tokens. Real-match
validation still requires the new per-tick extraction.
