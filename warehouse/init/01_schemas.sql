-- Schemas for the analytics warehouse (not Airflow's metadata DB).
-- clean = Spark output loaded as-is. marts = built by dbt. See docs/clean-contract.md.
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS marts;
