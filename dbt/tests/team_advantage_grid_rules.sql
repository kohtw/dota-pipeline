-- Every recorded match needs its complete grid, including rows with missing observations.
with observed as (
    select match_id, count(*) as observations,
           min(observation_time_s) as first_time_s, max(observation_time_s) as last_time_s,
           count(*) filter (where observation_time_s < 0 or observation_time_s % 10 != 0) as invalid_times
    from {{ ref('team_advantage_timeline') }}
    group by match_id
)

select coalesce(m.match_id, o.match_id) as match_id
from {{ source('clean', 'matches') }} m
full outer join observed o on o.match_id = m.match_id
where m.match_id is null or o.match_id is null
   or o.first_time_s != 0 or o.last_time_s != floor(m.duration_s / 10) * 10
   or o.observations != floor(m.duration_s / 10) + 1
   or o.invalid_times != 0
