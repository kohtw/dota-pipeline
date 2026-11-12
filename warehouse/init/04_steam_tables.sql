-- Steam match-summary lane, loaded by dota.steam_summary.load_steam (which runs this file first).
-- Portable SQL: runs on Postgres and DuckDB. Raw flattened Steam rows only; rules live in dbt.
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.steam_matches (
    match_id      BIGINT PRIMARY KEY,
    match_seq_num BIGINT,
    start_time    BIGINT,
    duration      INTEGER,
    game_mode     INTEGER,
    lobby_type    INTEGER,
    radiant_win   BOOLEAN,
    human_players INTEGER
);

CREATE TABLE IF NOT EXISTS stg.steam_players (
    match_id      BIGINT  NOT NULL,
    player_slot   INTEGER NOT NULL,
    hero_id       INTEGER,
    kills         INTEGER,
    deaths        INTEGER,
    assists       INTEGER,
    leaver_status INTEGER,
    PRIMARY KEY (match_id, player_slot)
);
