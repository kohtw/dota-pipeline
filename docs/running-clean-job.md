# Run the Spark clean job

`clean_replays.py` reads one completed parsed match and transforms it into the nine tables specified
by [the clean contract](clean-contract.md) and `warehouse/init/02_clean_tables.sql`. Its command,
paths, image name, and completion behavior follow [the orchestration interface](clean-job-interface.md).
It never contacts Steam or OpenDota or changes the raw Parquet files.

## Local preview

Use Python with Java 21 and `spark_jobs/requirements.txt` installed. No extractor JARs are needed
for local Parquet files. From the repository root:

```bash
python spark_jobs/clean_replays.py data/parsed output/clean 9079737909 --preview
```

If your parsed files are in `output/parsed`, use that input path instead. The invocation is per match.
`SPARK_MASTER` defaults to `local[2]`; the default shuffle partition count is four.

Preview writes the seven tables whose rules were validated in the notebooks, plus candidate deaths,
candidate assists, and diagnostic datasets under `<output_base>/_review/match_id=<id>/`. It writes a
JSON report there and exits successfully, but **never writes a clean completion marker**. A preview
is for inspection and must not be loaded into the warehouse as completed output. Structural validation
failures and attribution discrepancies are listed in the report even when preview exits zero.

## Normal run

```bash
python spark_jobs/clean_replays.py data/parsed output/clean 9079738443
```

A normal run writes all nine clean tables and the marker last only when keys, required fields,
roster mapping, timing, inventory resolution, and event-versus-final-snapshot K/D/A checks pass.
If a blocking finding remains, it writes the seven tables and review data, returns exit code 2,
and omits the marker. Other errors return nonzero as well. Airflow can therefore distinguish an
inspectable partial output from a match ready for loading. Do not bypass the marker check.

A rebuild removes its old marker before work begins. Writes replace only the requested match's
partitions, including removing stale rows when an optional table becomes empty. Other matches are
left alone. As with the existing DAG, do not run simultaneous cleaners for the same match/output.
Marker gating prevents consumers from accepting partial writes; it is not a transactional snapshot
for a reader that already started reading a previous completed generation.

## Deaths and assists still under review

The notebooks show that participant IDs already use slots 0-9 and lists can include the killer.
The job uses these observed IDs directly, excludes the direct killer, and deduplicates assist keys.
This intentionally differs from the draft contract's disproven divide-by-two assumption and must
be reconciled with Chris before rollout. Matching snapshot totals on one match is not proof of
correct credit on all matches.

Death events are deduplicated only when full records are identical. Nonidentical records sharing
a death key are retained in `conflicting_deaths` diagnostics and excluded from candidates. A null
killer slot remains null for a non-player attacker, per the current contract. The job does not infer
kill credit from proximity or participant lists. Any resulting K/D/A disagreement blocks completion.

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
docker run --rm -v "$PWD/data/parsed:/input:ro" -v "$PWD/output:/output" \
  dota-spark-clean /input /output/clean 9079737909 --preview
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
python -m unittest discover -s tests -p test_spark_clean.py -v
```
