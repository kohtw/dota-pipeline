-- Replay download state for dota_daily_download, written by dota.ingest. Portable SQL: runs on Postgres
-- and DuckDB, and is safe to rerun. Timestamps are UTC.
CREATE SCHEMA IF NOT EXISTS ingest;

-- Matches whose replay could not be downloaded yet (OpenDota has no replay_url, or rate-limited).
-- download_pending retries a row once next_try_at has passed; done_at or gave_up_at ends it.
CREATE TABLE IF NOT EXISTS ingest.download_queue (
    match_id    BIGINT    PRIMARY KEY,
    day         DATE      NOT NULL,
    attempts    INTEGER   NOT NULL,
    last_error  TEXT,
    added_at    TIMESTAMP NOT NULL,
    next_try_at TIMESTAMP NOT NULL,
    done_at     TIMESTAMP,
    gave_up_at  TIMESTAMP
);
