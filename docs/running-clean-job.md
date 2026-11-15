# Run the Spark clean job

`clean_replays.py` reads one completed parsed match and transforms it into the nine tables specified
by [the clean contract](clean-contract.md) and `warehouse/init/02_clean_tables.sql`. Its command,
paths, image name, and completion behavior follow [the orchestration interface](clean-job-interface.md).
It never contacts Steam or OpenDota or changes the raw Parquet files.

## Local preview

Use Python with Java 17 or newer and `spark_jobs/requirements.txt` installed. No extractor JARs are needed
for local Parquet files. From the repository root:

```bash
python spark_jobs/clean_replays.py data/parsed data/output/clean_kda 9079737909 --preview
```

The invocation is per match and requires the per-tick `kda_changes` table from the updated parser.
See [counter extraction and credit details](running-kda-clean-job.md).
`SPARK_MASTER` defaults to `local[2]`; the default shuffle partition count is four.

Preview writes the seven tables whose rules were validated in the notebooks, plus candidate deaths,
candidate assists, and diagnostic datasets under `<output_base>/_review/match_id=<id>/`. It writes a
JSON report there and exits successfully, but **never writes a clean completion marker**. A preview
is for inspection and must not be loaded into the warehouse as completed output. Structural validation
failures and attribution discrepancies are listed in the report even when preview exits zero.

## Normal run

```bash
python spark_jobs/clean_replays.py data/parsed data/output/clean_kda 9079738443
```

A normal run writes all nine clean tables and the marker last only when keys, required fields,
roster mapping, timing, inventory resolution, and event-versus-final-snapshot K/D/A checks pass.
If structural findings remain after a successful transform, it writes the seven tables and
candidate/review data, returns exit code 2, and omits the marker. Input or attribution failures
return exit code 2 with an error report and no completion marker; no new tables are published.
Other runtime errors return nonzero as well. Airflow can therefore distinguish an
inspectable partial output from a match ready for loading. Do not bypass the marker check.

A rebuild removes its old marker before work begins. Writes replace only the requested match's
partitions, including removing stale rows when an optional table becomes empty. Other matches are
left alone. As with the existing DAG, do not run simultaneous cleaners for the same match/output.
Marker gating prevents consumers from accepting partial writes; it is not a transactional snapshot
for a reader that already started reading a previous completed generation.

## Run for 10 matches

```bash
for folder in data/parsed/_complete/match_id=*; do
  match_id="${folder##*match_id=}"
  python spark_jobs/clean_replays.py \
    data/parsed data/output/clean_assist_test "$match_id"
done
```

## Counter-based death and assist credit

`clean_replays.py` is now the single cleaner. `kda_changes` retains the initial counters and
per-tick changes. Deaths must match death-counter increases, kills use `credited_killer_slot`,
and assists come from assist-counter increases. Direct `killer_slot`/`killer_name` remain last
hitter fields. Unique answers are unchanged. Ambiguous groups retry with assist-participant
restrictions; a unique fallback is labelled `participant_assumed`. If ambiguity remains but all
kill credits are proven unique in a separate search, the match is accepted with all assists in
that timing group withheld. Inconsistent data, ambiguous kills and exhausted searches still block.
Published assists plus per-player unresolved counts must exactly match the recorded scoreboard.
The report uses `contract_version = kda-credit-v2` and includes the unresolved counts and evidence.

The clean contract, warehouse schema and loader include the new fields. Existing warehouses
receive them through `warehouse/init/02_clean_tables.sql`, applied by the load sweep before inserts.
Legacy rows remain unknown until re-cleaned/reloaded; no credit is invented by migration.
dbt uses credited killers, exact complete-match assists and bounded partial-match assist counts. Participation and the
dashboard's involvement count use only eligible assists on the player's team's credited kills.
After loading, rebuild Postgres marts with `dbt build --project-dir dbt --profiles-dir dbt --target prod`.

Hero IDs are selected from the final mapped resource snapshot; nonpositive final selections remain
null. Coordinate/position processing follows the contract, including lowest handle for multiple real
hero entities at a tick. Inventory names are resolved only from item lifetimes active at the final
snapshot. Empty item handles are removed, and unresolved or ambiguous inventory entries block completion.
Pregame rows and negative game times are retained. Missing gold components stay null.

## Docker and Garage

```bash
docker build -t dota-spark-clean -f spark_jobs/Dockerfile .
```

The image entrypoint accepts the three arguments already supplied by `clean_pending`:

```text
s3a://raw/parsed s3a://raw/clean <match_id>
```

Provide `S3_ENDPOINT`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` in the container environment and use the
compose network so Garage is reachable. The image includes Java, PySpark, and matching S3 connector
JARs. No changes to the existing Airflow DAG or shared compose file are needed.

For a local Docker preview:

```bash
docker run --rm -v "$PWD/data/parsed:/input:ro" -v "$PWD/data/output:/output" \
  dota-spark-clean /input /data/output/clean_kda 9079737909 --preview
```

After a successful normal run in Garage, use the existing checker:

```bash
python scripts/check_clean.py <match_id>
```

## Tests

The focused tests use standard-library unittest and local Spark, without Docker, outside APIs, or
real credentials. Run in the environment with PySpark, PyArrow (for checking physical Parquet
columns), and Java installed:

```bash
python -m pytest tests/test_kda_credit.py tests/test_spark_clean.py tests/test_spark_kda_clean.py -q
```

## Updating the stack for partial assist attribution

The nine-table layout and DAG schedules are unchanged. The cleaner writes explicit
`assist_attribution_complete` and `assist_attribution_assumed` booleans on `matches`.
Apply `warehouse/init/02_clean_tables.sql` to an existing warehouse before rebuilding
marts. `load_pending.apply_schema` also applies it automatically before loading new output.
Legacy rows stay NULL until re-cleaned/reloaded. The dashboard preserves recorded K/D/A,
explains partial/assumed attribution, and hides event-based participation for partial/unknown matches.

Test locally before loading, using a separate output directory:

```bash
python spark_jobs/clean_replays.py data/parsed data/output/clean_partial_test MATCH_ID
```

Inspect `_review/match_id=MATCH_ID/report.json`, including `assist_attribution`.
For Airflow, rebuild `dota-spark-clean` and the Compose services (including the dashboard).
The mounted DAG/dbt source is picked up by the existing services. To retry selected matches,
remove their clean completion, rejection/failure and loaded markers as applicable, then trigger
`clean_pending`. Do not delete raw/parsed data or reset unrelated match markers. Reusing a
completed parsed match requires no parsing change for this feature. Unknown historical flags
can make dbt's strict checks fail until those historical matches have been revalidated.
