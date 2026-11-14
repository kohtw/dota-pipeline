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
    select i.match_id, i.player_slot,
           string_agg(i.item_name, ',' order by i.inventory_slot) as final_items,
           to_json(array_agg(coalesce(nullif(trim(d.localized_name), ''), i.item_name)
                             order by i.inventory_slot)) as final_item_labels
    from {{ source('clean', 'final_inventory') }} i
    left join {{ ref('dim_items') }} d on d.name = i.item_name
    where i.inventory_slot between 0 and 5
    group by i.match_id, i.player_slot
)

select
    cast(s.match_id as varchar) || '-' || cast(s.player_slot as varchar) as player_key,
    s.match_id,
    s.player_slot,
    p.hero_name,
    h.localized_name as hero_localized_name,
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
    s.total_earned_gold / (m.duration_s / 60.0) as gpm,
    m.duration_s,
    i.final_items,
    i.final_item_labels
from final_stats s
join {{ source('clean', 'players') }} p using (match_id, player_slot)
left join {{ ref('dim_heroes') }} h on h.npc_name = p.hero_name
join {{ source('clean', 'matches') }} m using (match_id)
left join items i using (match_id, player_slot)
