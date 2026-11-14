select match_id, player_slot, observation_time_s, count(*) as rows_per_observation
from {{ ref('stg_replay_player_observations') }}
group by match_id, player_slot, observation_time_s
having count(*) > 1
