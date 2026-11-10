-- Mart: one row per player per match, using the LAST player_stats snapshot (final values).
-- Read by the dashboard's scoreboard page. Built from clean.player_stats, players, matches, final_inventory.
with last_tick as (
    select match_id, player_slot, max(tick) as tick
    from {{ source('clean', 'player_stats') }}
    group by match_id, player_slot
),

final_stats as (
    select s.*
    from {{ source('clean', 'player_stats') }} s
    join last_tick using (match_id, player_slot, tick)
),

-- inventory slots 0-5 only (the six main item slots), in slot order
items as (
    select match_id, player_slot,
           string_agg(item_name, ',' order by inventory_slot) as final_items
    from {{ source('clean', 'final_inventory') }}
    where inventory_slot <= 5
    group by match_id, player_slot
)

select
    cast(s.match_id as varchar) || '-' || cast(s.player_slot as varchar) as player_key,
    s.match_id,
    s.player_slot,
    p.hero_name,
    p.team,
    p.team = m.winner_team as won,
    s.level,
    s.kills,
    s.deaths,
    s.assists,
    s.last_hits,
    s.denies,
    s.net_worth,
    s.unspent_gold,                -- kept apart from net_worth on purpose (see contract)
    s.total_earned_gold,
    s.total_earned_xp,
    s.hero_damage,
    s.tower_damage,
    s.healing,
    s.total_earned_gold / (nullif(m.duration_s, 0) / 60.0) as gpm,   -- NULL, not an error, for a zero duration
    m.duration_s,
    i.final_items
from final_stats s
-- left joins: a missing players/matches row shows up as NULL and fails the not_null tests, instead of dropping the row
left join {{ source('clean', 'players') }} p using (match_id, player_slot)
left join {{ source('clean', 'matches') }} m using (match_id)
left join items i using (match_id, player_slot)
