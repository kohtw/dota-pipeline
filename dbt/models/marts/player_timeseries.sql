-- Mart: one row per player per second. Feeds the net worth / XP over time charts.
-- Built from clean.player_stats joined to players for hero and team.
select
    cast(s.match_id as varchar) || '-' || cast(s.player_slot as varchar) || '-' || cast(s.tick as varchar) as player_tick_key,
    s.match_id,
    s.player_slot,
    p.hero_name,
    p.team,
    s.tick,
    s.game_time_s,
    s.level,
    s.kills,
    s.deaths,
    s.assists,
    s.net_worth,
    s.unspent_gold,
    s.total_earned_gold,
    s.total_earned_xp,
    s.last_hits,
    s.denies
from {{ source('clean', 'player_stats') }} s
join {{ source('clean', 'players') }} p using (match_id, player_slot)
