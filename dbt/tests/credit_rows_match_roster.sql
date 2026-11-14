-- Credit must refer to this match's roster, and assist flags must describe the teams.
select 'death' as record_type, d.match_id, d.tick, d.victim_slot,
       d.credited_killer_slot as credit_slot
from {{ source('clean', 'deaths') }} d
left join {{ source('clean', 'players') }} victim
  on victim.match_id = d.match_id and victim.player_slot = d.victim_slot
left join {{ source('clean', 'players') }} killer
  on killer.match_id = d.match_id and killer.player_slot = d.credited_killer_slot
where victim.player_slot is null
   or (d.credited_killer_slot is not null and killer.player_slot is null)
union all
select 'assist', a.match_id, a.tick, a.victim_slot, a.assist_slot
from {{ source('clean', 'death_assists') }} a
left join {{ source('clean', 'deaths') }} d
  on d.match_id = a.match_id and d.tick = a.tick and d.victim_slot = a.victim_slot
left join {{ source('clean', 'players') }} victim
  on victim.match_id = a.match_id and victim.player_slot = a.victim_slot
left join {{ source('clean', 'players') }} assister
  on assister.match_id = a.match_id and assister.player_slot = a.assist_slot
where d.victim_slot is null or victim.player_slot is null or assister.player_slot is null
   or a.same_side is distinct from (victim.team = assister.team)
   or a.assist_slot = d.credited_killer_slot
