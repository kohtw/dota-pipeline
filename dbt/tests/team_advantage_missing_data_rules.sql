-- Missing coverage remains NULL, including at either endpoint of the exact two-minute comparison.
select a.match_id, a.observation_time_s
from {{ ref('team_advantage_timeline') }} a
left join {{ ref('team_advantage_timeline') }} previous on previous.match_id = a.match_id
  and previous.observation_time_s = a.observation_time_s - 120
where a.radiant_players_observed not between 0 and 5
   or a.dire_players_observed not between 0 and 5
   or (a.radiant_net_worth is null) != (a.radiant_players_observed < 5)
   or (a.dire_net_worth is null) != (a.dire_players_observed < 5)
   or a.radiant_net_worth < 0 or a.dire_net_worth < 0
   or a.net_worth_advantage is distinct from a.radiant_net_worth - a.dire_net_worth
   or a.net_worth_advantage_change_120s is distinct from
       a.net_worth_advantage - previous.net_worth_advantage
   or (a.observation_time_s < 120 and a.net_worth_advantage_change_120s is not null)
   or (a.max_snapshot_age_s is not null and
       not (a.max_snapshot_age_s >= 0 and a.max_snapshot_age_s < 'Infinity'::double precision))
