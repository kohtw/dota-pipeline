"""Plain constants, safe to import at DAG-parse time (no heavy dependencies)."""

BUCKET = "raw"  # Garage raw bucket
S3_ENV_VARS = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")
