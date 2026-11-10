-- Clean tables. Column names and grain must match docs/clean-contract.md.
-- Primary key = the grain. No foreign keys: loading deletes and re-inserts one match at a time.
-- Portable SQL: runs on Postgres and DuckDB (used for local dbt tests).

CREATE TABLE IF NOT EXISTS clean.matches (
    match_id        BIGINT PRIMARY KEY,
    game_mode       INTEGER,
    winner_team     INTEGER,
    end_time        BIGINT,
    game_start_tick INTEGER,
    duration_s      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS clean.players (
    match_id    BIGINT  NOT NULL,
    player_slot INTEGER NOT NULL,
    team        INTEGER NOT NULL,
    team_slot   INTEGER NOT NULL,
    hero_name   TEXT    NOT NULL,
    hero_id     INTEGER,
    PRIMARY KEY (match_id, player_slot)
);

CREATE TABLE IF NOT EXISTS clean.player_stats (
    match_id          BIGINT  NOT NULL,
    player_slot       INTEGER NOT NULL,
    tick              INTEGER NOT NULL,
    game_time_s       DOUBLE PRECISION NOT NULL,
    level             INTEGER,
    kills             INTEGER,
    deaths            INTEGER,
    assists           INTEGER,
    net_worth         INTEGER,
    unspent_gold      INTEGER,
    total_earned_gold INTEGER,
    total_earned_xp   INTEGER,
    last_hits         INTEGER,
    denies            INTEGER,
    hero_damage       DOUBLE PRECISION,
    tower_damage      DOUBLE PRECISION,
    healing           DOUBLE PRECISION,
    PRIMARY KEY (match_id, player_slot, tick)
);

CREATE TABLE IF NOT EXISTS clean.positions (
    match_id    BIGINT  NOT NULL,
    player_slot INTEGER NOT NULL,
    tick        INTEGER NOT NULL,
    game_time_s DOUBLE PRECISION NOT NULL,
    x           DOUBLE PRECISION,
    y           DOUBLE PRECISION,
    PRIMARY KEY (match_id, player_slot, tick)
);

CREATE TABLE IF NOT EXISTS clean.damage_events (
    match_id      BIGINT  NOT NULL,
    tick          INTEGER NOT NULL,
    game_time_s   DOUBLE PRECISION NOT NULL,
    attacker_name TEXT    NOT NULL,
    target_name   TEXT    NOT NULL,
    inflictor_name TEXT   NOT NULL,          -- NULL raw index (auto attack) becomes 'dota_unknown'
    attacker_slot INTEGER,                   -- NULL when the attacker is a creep or tower
    target_slot   INTEGER,
    damage        INTEGER,
    hits          INTEGER,
    PRIMARY KEY (match_id, tick, attacker_name, target_name, inflictor_name)
);

CREATE TABLE IF NOT EXISTS clean.deaths (
    match_id    BIGINT  NOT NULL,
    tick        INTEGER NOT NULL,
    game_time_s DOUBLE PRECISION NOT NULL,
    victim_slot INTEGER NOT NULL,
    killer_slot INTEGER,                     -- NULL when a creep or tower got the kill
    killer_name TEXT,
    PRIMARY KEY (match_id, tick, victim_slot)
);

CREATE TABLE IF NOT EXISTS clean.death_assists (
    match_id    BIGINT  NOT NULL,
    tick        INTEGER NOT NULL,
    victim_slot INTEGER NOT NULL,
    assist_slot INTEGER NOT NULL,
    PRIMARY KEY (match_id, tick, victim_slot, assist_slot)
);

CREATE TABLE IF NOT EXISTS clean.purchases (
    match_id    BIGINT  NOT NULL,
    tick        INTEGER NOT NULL,
    game_time_s DOUBLE PRECISION NOT NULL,
    player_slot INTEGER NOT NULL,
    item_name   TEXT    NOT NULL,
    quantity    INTEGER NOT NULL,
    PRIMARY KEY (match_id, tick, player_slot, item_name)
);

CREATE TABLE IF NOT EXISTS clean.final_inventory (
    match_id       BIGINT  NOT NULL,
    player_slot    INTEGER NOT NULL,
    inventory_slot INTEGER NOT NULL,         -- 0-5 inventory, 6-8 backpack, 9-14 stash, 15 teleport, 16 neutral
    item_name      TEXT    NOT NULL,
    PRIMARY KEY (match_id, player_slot, inventory_slot)
);
