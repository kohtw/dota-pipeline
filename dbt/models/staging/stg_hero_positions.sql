-- Staging: one real hero at one position sample, flagged alive or dead.
-- A dead hero's position is its corpse, so map views and lane detection drop dead samples.
-- Dead runs from the death tick until the respawn in stg_hero_deaths; with no respawn found
-- (the replay ended first) it runs for at most 240 seconds.
select s.match_id, s.player_slot, s.tick, s.game_time_s, s.x, s.y,
       count(d.tick) = 0 as is_alive
from {{ source('clean', 'positions') }} s
left join {{ ref('stg_hero_deaths') }} d
  on d.match_id = s.match_id and d.victim_slot = s.player_slot
 and s.tick > d.tick
 and s.game_time_s < coalesce(d.respawn_s, d.game_time_s + 240)
group by s.match_id, s.player_slot, s.tick, s.game_time_s, s.x, s.y
