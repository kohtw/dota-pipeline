select match_id, observation_time_s, count(*) as rows_per_observation
from {{ ref('team_advantage_timeline') }}
group by match_id, observation_time_s
having count(*) > 1
