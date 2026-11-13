-- Mart: one aligned economic observation per replay match and ten-second grid time.
-- Positive advantage favours Radiant. Incomplete teams keep NULL totals, never partial sums.
with rosters as (
    select match_id,
           count(*) = 10 and count(distinct player_slot) = 10
           and count(*) filter (where team = 2) = 5
           and count(*) filter (where team = 3) = 5
           and count(*) filter (
               where team not in (2, 3) or team_slot not between 0 and 4
                  or player_slot != (team - 2) * 5 + team_slot
           ) = 0 as roster_valid
    from {{ source('clean', 'players') }}
    group by match_id
),

team_totals as (
    select match_id, observation_time_s, team,
           count(*) filter (where is_usable) as players_observed,
           case when count(*) = 5 and count(*) filter (where is_usable) = 5
                then sum(net_worth) end as net_worth,
           max(snapshot_age_s) as max_snapshot_age_s
    from {{ ref('stg_replay_player_observations') }}
    group by match_id, observation_time_s, team
),

match_observations as (
    select t.match_id, t.observation_time_s,
           case when r.roster_valid then max(t.net_worth) filter (where t.team = 2) end as radiant_net_worth,
           case when r.roster_valid then max(t.net_worth) filter (where t.team = 3) end as dire_net_worth,
           coalesce(max(t.players_observed) filter (where t.team = 2), 0) as radiant_players_observed,
           coalesce(max(t.players_observed) filter (where t.team = 3), 0) as dire_players_observed,
           max(t.max_snapshot_age_s) as max_snapshot_age_s
    from team_totals t
    join rosters r on r.match_id = t.match_id
    group by t.match_id, t.observation_time_s, r.roster_valid
),

advantages as (
    select match_id, observation_time_s, radiant_net_worth, dire_net_worth,
           radiant_net_worth - dire_net_worth as net_worth_advantage,
           radiant_players_observed, dire_players_observed, max_snapshot_age_s
    from match_observations
)

select a.match_id, a.observation_time_s, a.radiant_net_worth, a.dire_net_worth,
       a.net_worth_advantage, a.radiant_players_observed, a.dire_players_observed,
       a.max_snapshot_age_s,
       a.net_worth_advantage - previous.net_worth_advantage as net_worth_advantage_change_120s
from advantages a
left join advantages previous on previous.match_id = a.match_id
  and previous.observation_time_s = a.observation_time_s - 120
