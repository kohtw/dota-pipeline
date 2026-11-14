-- Each roster player retains every requested grid row, even without any usable snapshots.
select m.match_id, p.player_slot
from {{ source('clean', 'matches') }} m
join {{ source('clean', 'players') }} p on p.match_id = m.match_id
left join {{ ref('stg_replay_player_observations') }} o
  on o.match_id = m.match_id and o.player_slot = p.player_slot
group by m.match_id, p.player_slot, m.duration_s, p.team
having count(o.observation_time_s) != floor(m.duration_s / 10) + 1
    or count(*) filter (where o.team is distinct from p.team
        or o.observation_time_s < 0 or o.observation_time_s > m.duration_s
        or o.observation_time_s % 10 != 0) > 0
