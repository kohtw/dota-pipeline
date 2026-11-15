-- Staging: one scoreboard death with the victim's corpse position and respawn time.
-- A dead hero's entity stays where it died until it respawns, then jumps to its fountain. So the
-- first position sample after the death tick is the corpse spot, and the first later sample away
-- from that spot is the respawn (or a buyback). Samples are 30 ticks (one second) apart.
with deaths as (
    select match_id, tick, game_time_s, victim_slot
    from {{ source('clean', 'deaths') }}
),

corpse as (
    select d.match_id, d.tick, d.victim_slot, s.tick as corpse_tick, s.x, s.y,
           row_number() over (partition by d.match_id, d.tick, d.victim_slot order by s.tick) as rn
    from deaths d
    join {{ source('clean', 'positions') }} s
      on s.match_id = d.match_id and s.player_slot = d.victim_slot
     and s.tick > d.tick and s.tick <= d.tick + 60
),

-- Fallback when no sample follows the death: the last sample up to two seconds before it.
last_seen as (
    select d.match_id, d.tick, d.victim_slot, s.x, s.y,
           row_number() over (partition by d.match_id, d.tick, d.victim_slot order by s.tick desc) as rn
    from deaths d
    join {{ source('clean', 'positions') }} s
      on s.match_id = d.match_id and s.player_slot = d.victim_slot
     and s.tick between d.tick - 60 and d.tick
),

-- Respawn timers stay well under four minutes, so the search stops there.
respawn as (
    select c.match_id, c.tick, c.victim_slot, min(s.game_time_s) as respawn_s
    from corpse c
    join {{ source('clean', 'positions') }} s
      on s.match_id = c.match_id and s.player_slot = c.victim_slot
     and s.tick > c.corpse_tick and s.tick <= c.tick + 30 * 240
     and (abs(s.x - c.x) > 1 or abs(s.y - c.y) > 1)
    where c.rn = 1
    group by c.match_id, c.tick, c.victim_slot
)

select d.match_id, d.tick, d.game_time_s, d.victim_slot,
       coalesce(c.x, l.x) as x,
       coalesce(c.y, l.y) as y,
       r.respawn_s,
       r.respawn_s - d.game_time_s as dead_s
from deaths d
left join corpse c on c.match_id = d.match_id and c.tick = d.tick and c.victim_slot = d.victim_slot and c.rn = 1
left join last_seen l on l.match_id = d.match_id and l.tick = d.tick and l.victim_slot = d.victim_slot and l.rn = 1
left join respawn r on r.match_id = d.match_id and r.tick = d.tick and r.victim_slot = d.victim_slot
