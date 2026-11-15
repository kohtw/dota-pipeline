-- Reference data history for the refresh_constants DAG, written by dota.constants_load (which runs
-- this file first). Append-only: one row per key each time the feed shows a changed version, stamped
-- with the snapshot's fetch time (observed_at, UTC, no time zone) and its Garage prefix. dbt builds
-- the dims from these rows on top of the seed baseline (dbt/seeds/baseline_*.csv).
-- Portable SQL: runs on Postgres and DuckDB, and is safe to rerun.
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.constant_heroes (
    hero_id         INTEGER   NOT NULL,
    npc_name        TEXT,
    localized_name  TEXT,
    primary_attr    TEXT,
    roles           TEXT,
    observed_at     TIMESTAMP NOT NULL,
    source_snapshot TEXT      NOT NULL,
    PRIMARY KEY (hero_id, observed_at)
);

CREATE TABLE IF NOT EXISTS stg.constant_items (
    item_id         INTEGER   NOT NULL,
    name            TEXT,
    localized_name  TEXT,
    is_recipe       BOOLEAN,
    neutral_tier    INTEGER,
    observed_at     TIMESTAMP NOT NULL,
    source_snapshot TEXT      NOT NULL,
    PRIMARY KEY (item_id, observed_at)
);

CREATE TABLE IF NOT EXISTS stg.constant_patches (
    patch           TEXT      NOT NULL,
    released_at     TIMESTAMP,
    observed_at     TIMESTAMP NOT NULL,
    source_snapshot TEXT      NOT NULL,
    PRIMARY KEY (patch, observed_at)
);

CREATE TABLE IF NOT EXISTS stg.constant_game_modes (
    game_mode_id    INTEGER   NOT NULL,
    name            TEXT,
    label           TEXT,
    observed_at     TIMESTAMP NOT NULL,
    source_snapshot TEXT      NOT NULL,
    PRIMARY KEY (game_mode_id, observed_at)
);
