-- Ingestion state for dota_daily_download, written by dota.ingest. Portable SQL: runs on Postgres and
-- DuckDB, and is safe to rerun. Kept here rather than in Airflow Variables, which hold only the
-- latest value and live in Airflow's metadata database (CONTRIBUTING rule 10). Timestamps are UTC.
CREATE SCHEMA IF NOT EXISTS ingest;

-- One Steam match_seq_num per day, taken at 00:00 SGT. The download for a day covers the matches
-- that ended between the previous day's bookmark and its own, so a rerun gets the same window.
CREATE TABLE IF NOT EXISTS ingest.bookmarks (
    day           DATE      PRIMARY KEY,
    match_seq_num BIGINT    NOT NULL,
    recorded_at   TIMESTAMP NOT NULL
);
-- download_mode=window only: the next seq to read in the day's window (null = not started). Added
-- with ALTER so a table created before this column gains it when the file reruns.
ALTER TABLE ingest.bookmarks ADD COLUMN IF NOT EXISTS cursor_seq BIGINT;

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
