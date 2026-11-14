-- Staging: one row per player of every ranked All Pick match worth counting.
-- Rules: lobby 7 (ranked), game mode 22 (All Pick), at least 15 minutes, all 10 slots human, result known.
select
    m.match_id, m.start_time, m.duration, m.radiant_win,
    p.player_slot, p.hero_id, p.kills, p.deaths, p.assists,
    p.gold_per_min, p.xp_per_min,
    p.hero_damage, p.hero_healing, p.tower_damage,
    -- Radiant occupies slots 0-127, Dire 128 and up
    (p.player_slot < 128) as on_radiant
from {{ source('stg', 'steam_matches') }} m
join {{ source('stg', 'steam_players') }} p on p.match_id = m.match_id
where m.lobby_type = 7
  and m.game_mode = 22
  and m.duration >= 900
  and m.human_players = 10
  and m.radiant_win is not null
