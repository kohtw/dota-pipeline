-- Mart: one player in one match: the lane they played and where they stood at 10:00.
-- Lane comes from where the hero was while alive between 1:00 and 10:00 of game time. Each
-- sample falls in a zone of the map's world coordinates (Radiant fountain bottom-left, Dire
-- top-right; mid runs along the y = x diagonal):
--   own base   x and y both deep in the team's corner (left out of the count)
--   mid        |y - x| < 2000
--   top        y - x >= 2000 and near the left or top edge (x < -4500 or y > 4500)
--   bot        y - x <= -2000 and near the bottom or right edge (y < -4500 or x > 4500)
--   jungle     anything else
-- lane is the most visited zone when it holds at least 40% of the samples, else 'roaming'.
-- On the 62 replays checked this gives a 2-1-2 split per team in all but one match.
-- lane_role reads lane from the team's side: Radiant's safe lane is bot, Dire's is top.
-- The *_10 columns are the player's last snapshot between 9:50 and 10:00, NULL without one.
with players as (
    select match_id, player_slot, team
    from {{ source('clean', 'players') }}
),

zones as (
    select s.match_id, s.player_slot,
           case
               when (p.team = 2 and s.x < -4300 and s.y < -3900)
                 or (p.team = 3 and s.x > 4300 and s.y > 3900) then 'base'
               when abs(s.y - s.x) < 2000 then 'mid'
               when s.y - s.x >= 2000 and (s.x < -4500 or s.y > 4500) then 'top'
               when s.y - s.x <= -2000 and (s.y < -4500 or s.x > 4500) then 'bot'
               else 'jungle'
           end as zone
    from {{ ref('stg_hero_positions') }} s
    join players p on p.match_id = s.match_id and p.player_slot = s.player_slot
    where s.is_alive and s.game_time_s >= 60 and s.game_time_s < 600
),

zone_counts as (
    select match_id, player_slot, zone, count(*) as samples
    from zones
    where zone <> 'base'
    group by match_id, player_slot, zone
),

ranked as (
    select z.*,
           cast(samples as double precision) / sum(samples) over (partition by match_id, player_slot) as share,
           row_number() over (partition by match_id, player_slot order by samples desc, zone) as rn
    from zone_counts z
),

lanes as (
    select match_id, player_slot, share as lane_share,
           case when share >= 0.4 then zone else 'roaming' end as lane
    from ranked
    where rn = 1
),

at_10 as (
    select s.*,
           row_number() over (partition by s.match_id, s.player_slot order by s.game_time_s desc, s.tick desc) as rn
    from {{ source('clean', 'player_stats') }} s
    where s.game_time_s between 590 and 600
)

select
    cast(p.match_id as varchar) || '-' || cast(p.player_slot as varchar) as player_key,
    p.match_id,
    p.player_slot,
    p.team,
    l.lane,
    l.lane_share,
    case
        when l.lane = 'bot' then case when p.team = 2 then 'safe' else 'off' end
        when l.lane = 'top' then case when p.team = 3 then 'safe' else 'off' end
        else l.lane
    end as lane_role,
    a.net_worth as net_worth_10,
    a.total_earned_xp as xp_10,
    a.level as level_10,
    a.last_hits as last_hits_10,
    a.denies as denies_10,
    a.kills as kills_10,
    a.deaths as deaths_10,
    a.assists as assists_10
from players p
left join lanes l on l.match_id = p.match_id and l.player_slot = p.player_slot
left join at_10 a on a.match_id = p.match_id and a.player_slot = p.player_slot and a.rn = 1
