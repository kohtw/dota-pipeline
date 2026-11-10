# Clean job interface (for Maximus)

You own the Spark clean layer only (`CONTRIBUTING.md` §13), not ingestion or parsing. Your whole job is the per-table Spark transform from parsed Parquet to the 9 clean tables. Orchestration (Airflow) handles everything else: it runs your image once per match and then loads the output into the warehouse.

This interface is fixed. Any change is a PR tagging Winston and the downstream owners. No skeleton Spark file is provided; this doc is the whole hand-off.

## Inputs

Raw parsed Parquet, one folder per table per match, in the existing `raw` bucket:

    s3a://raw/parsed/{table}/match_id={id}/

Raw columns are in `docs/data-contract.md`.

## Outputs

The 9 clean tables, written under the same bucket (`clean/` is a prefix next to `parsed/`):

    s3a://raw/clean/{table}/match_id={id}/part-*.parquet

Tables: `matches`, `players`, `player_stats`, `positions`, `damage_events`, `deaths`, `death_assists`, `purchases`, `final_inventory`.

- Columns, types, grain and keys are fixed by `docs/clean-contract.md` and `warehouse/init/02_clean_tables.sql`. Do not restate or change them.
- Write with `partitionBy("match_id")`, so the part files carry NO `match_id` column (it lives in the path only).
- Teams: 2 = Radiant, 3 = Dire.
- Only `death_assists` and `purchases` may be empty for a match (no parts written is accepted); every other table must have rows.
- Write the completion marker LAST, only after all 9 tables are written:

      s3a://raw/clean/_complete/match_id={id}/

  Any object under that prefix works (e.g. `_SUCCESS`). No marker means the match is treated as unfinished.

## Invocation

- Image: `dota-spark-clean`
- CLI: `clean <input_base> <output_base> <match_id>`
  - `input_base` = `s3a://raw/parsed`
  - `output_base` = `s3a://raw/clean`
- One match per invocation; orchestration maps it over matches (same pattern as the parser's DockerOperator in `dags/parse_pending.py`).
- Idempotent: a re-run overwrites that match's partitions and leaves other matches alone (use `spark.sql.sources.partitionOverwriteMode=dynamic`, as the parser does).
- S3 credentials come from env vars `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`. Mirror the `SparkSession` and `s3a` config in `spark_jobs/parse_replays.py` (`spark_session()`): endpoint, path-style access, region `us-east-1`, keys.

## Validate without Airflow

    python3 scripts/check_clean.py <match_id>

It runs the contract check (`check_parsed` in `dags/dota/clean_contract.py`) against your output in S3 and prints per-table row counts, or the first failure (missing marker, missing/empty table, wrong columns). The harness lands in a later orchestration task; until then, the check function above is the reference for what passes.
