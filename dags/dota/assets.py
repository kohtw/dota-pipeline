"""Shared Airflow Assets for the pipeline's stage-to-stage triggers.

Each stage produces one Asset; the next stage is scheduled on it, so a finished
upstream DAG run wakes its consumer instead of everyone polling on a timer:

  parse -> RAW_PARSED -> clean -> CLEAN_READY -> load -> CLEAN_LOADED -> dbt

URIs are stable identifiers, not live connections: Airflow treats two Assets as the
same only when their URIs match, so producers and consumers import these objects from
here rather than re-declaring them. The s3:// ones name Garage prefixes, the
postgres:// one the `matches` table of the warehouse `clean` schema, in the
host:port/database/schema/table form the Airflow postgres provider requires (it
rejects any other path shape and fills in port 5432 when it is omitted).
"""

from airflow.sdk import Asset

RAW_PARSED = Asset("s3://raw/parsed")  # parsed Parquet in Garage; produced by parse_pending
CLEAN_READY = Asset("s3://raw/clean")  # clean Parquet in Garage; produced by clean_pending
CLEAN_LOADED = Asset("postgres://warehouse:5432/warehouse/clean/matches")  # warehouse.clean.matches, standing for the clean schema; produced by the load DAG
