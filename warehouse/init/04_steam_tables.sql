-- Steam match-summary lane, loaded by dota.steam_summary.load_steam (which runs this file first).
-- Portable SQL: runs on Postgres and DuckDB. Raw flattened Steam rows only; rules live in dbt.
-- Later columns are added with ALTER ... ADD COLUMN IF NOT EXISTS, so a table on an existing volume
-- gains them when this file reruns. DuckDB takes one ADD per ALTER.
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

ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS radiant_score     INTEGER;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS dire_score        INTEGER;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS first_blood_time  INTEGER;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS pre_game_duration INTEGER;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS cluster           INTEGER;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS leagueid          INTEGER;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS source_page       TEXT;
ALTER TABLE stg.steam_matches ADD COLUMN IF NOT EXISTS loaded_at         TIMESTAMP;

ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS team_number  INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS last_hits    INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS denies       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS gold_per_min INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS xp_per_min   INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS level        INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS net_worth    INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS hero_damage  INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS tower_damage INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS hero_healing INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS gold         INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS gold_spent   INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_0       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_1       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_2       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_3       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_4       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_5       INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS backpack_0   INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS backpack_1   INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS backpack_2   INTEGER;
ALTER TABLE stg.steam_players ADD COLUMN IF NOT EXISTS item_neutral INTEGER;

-- One row per draft entry; only matches whose page lists picks_bans have rows.
CREATE TABLE IF NOT EXISTS stg.steam_picks_bans (
    match_id BIGINT  NOT NULL,
    ord      INTEGER NOT NULL,  -- the page's "order" field; ORDER is a reserved word
    is_pick  BOOLEAN,
    hero_id  INTEGER,
    team     INTEGER,
    PRIMARY KEY (match_id, ord)
);
