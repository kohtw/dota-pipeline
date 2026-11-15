-- Mart: where each hero was every five seconds of game time, for the map replay.
-- time_s is the five-second step; x and y are the first sample in that step.
-- is_alive is false while the hero is dead (its position is then the corpse spot).
with ranked as (
    select match_id, player_slot, tick, x, y, is_alive,
           cast(floor(game_time_s / 5) as integer) * 5 as time_s,
           row_number() over (partition by match_id, player_slot, cast(floor(game_time_s / 5) as integer)
                              order by tick) as rn
    from {{ ref('stg_hero_positions') }}
)

select
    cast(match_id as varchar) || '-' || cast(player_slot as varchar) || '-' || cast(time_s as varchar) as position_key,
    match_id,
    player_slot,
    time_s,
    x,
    y,
    is_alive
from ranked
where rn = 1
