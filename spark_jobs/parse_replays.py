"""
PySpark job: replays -> one raw Parquet dataset per table (tables defined in replay_spec.py).

The replay reading itself runs in the JVM (ReplayExtractor, Clarity) on the executors; this script
passes the spec, gets back one DataFrame per table, and writes them.

Output: <out>/<table>/match_id=<id>/part-*.parquet. Re-running a match overwrites only that match.
After all tables, <out>/_complete/match_id=<id>/ marks each match as fully written.

Paths can be local or s3a:// (Garage). For s3a, set env S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY.
Jars: env EXTRACTOR_JARS (set in the dota-parser image), else parser/build/extractor + parser/build/s3a.

Run (in the dota-parser image, see parser/Dockerfile):
  docker run --rm -v "$PWD/data/output:/demos:ro" -v "$PWD/data:/out" dota-parser "/demos/*.dem" /out/parsed
  airflow: replays "s3a://raw/replays/<id>.dem", out "s3a://raw/parsed"
"""

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from replay_spec import SPEC

ROOT = Path(__file__).resolve().parent.parent
LOCAL_JARS = [ROOT / "parser" / "build" / "extractor", ROOT / "parser" / "build" / "s3a"]


def spark_session() -> SparkSession:
    dirs = [Path(os.environ["EXTRACTOR_JARS"])] if "EXTRACTOR_JARS" in os.environ else LOCAL_JARS
    jars = [str(p) for d in dirs for p in sorted(d.glob("*.jar"))]
    if not jars:
        raise SystemExit(f"no jars in {dirs}; run: cd parser && ./gradlew extractorJars s3aJars")
    b = (SparkSession.builder.appName("dota-parse")
         .config("spark.jars", ",".join(jars))
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    if endpoint := os.environ.get("S3_ENDPOINT"):
        b = (b.config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")  # must match s3_region in garage.toml
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
             .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"]))
    return b.getOrCreate()


def parse(spark: SparkSession, replays: str) -> dict[str, DataFrame]:
    """One DataFrame per spec table, rows from every replay matching the glob / comma list."""
    jmap = spark._jvm.ReplayExtractor.parse(spark._jsparkSession, replays, json.dumps(SPEC))
    return {t["name"]: DataFrame(jmap.get(t["name"]), spark) for t in SPEC["tables"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("replays", help='glob or comma list, e.g. "data/output/*.dem" or "s3a://raw/replays/1.dem,s3a://..."')
    ap.add_argument("out", help="output dir, e.g. data/parsed or s3a://raw/parsed")
    args = ap.parse_args()

    spark = spark_session()
    tables = parse(spark, args.replays)
    for name, df in tables.items():
        df.write.mode("overwrite").partitionBy("match_id").parquet(f"{args.out}/{name}")
    # written last: a match with a _complete partition has every table written (a crash leaves none)
    (tables["match"].select("match_id").withColumn("parsed_at", F.current_timestamp())
     .write.mode("overwrite").partitionBy("match_id").parquet(f"{args.out}/_complete"))
    spark.stop()


if __name__ == "__main__":
    main()
