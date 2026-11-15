-- Mart: one fight, a run of deaths with no gap over 15 seconds (see match_deaths).
-- kind: 'teamfight' at 3+ deaths, 'skirmish' at 2, 'pick-off' at 1.
-- winning_team is the side that lost fewer heroes; NULL on an even trade.
-- Hero damage comes from match_fight_players (window: 10 s before the first death to 3 s after
-- the last). The net-worth swing is the change in Radiant's advantage across that window, read
-- from team_advantage_timeline on its ten-second grid; NULL when either end lacks full coverage.
-- It includes farming in the same window, so it is context, not the fight's own gold value.
with fights as (
    select match_id, fight_number,
           min(game_time_s) as start_s,
           max(game_time_s) as end_s,
           count(*) as deaths,
           count(*) filter (where victim_team = 2) as radiant_deaths,
           count(*) filter (where victim_team = 3) as dire_deaths,
           avg(x) as center_x,
           avg(y) as center_y
    from {{ ref('match_deaths') }}
    group by match_id, fight_number
),

damage as (
    select match_id, fight_number,
           sum(damage_dealt) filter (where team = 2) as radiant_hero_damage,
           sum(damage_dealt) filter (where team = 3) as dire_hero_damage,
           count(*) as participants
    from {{ ref('match_fight_players') }}
    group by match_id, fight_number
),

edges as (
    select f.*,
           cast(floor((f.start_s - 10) / 10) as integer) * 10 as before_time_s,
           cast(ceil((f.end_s + 3) / 10) as integer) * 10 as after_time_s
    from fights f
)

select
    cast(e.match_id as varchar) || '-' || cast(e.fight_number as varchar) as fight_key,
    e.match_id,
    e.fight_number,
    e.start_s,
    e.end_s,
    e.end_s - e.start_s as duration_s,
    e.deaths,
    e.radiant_deaths,
    e.dire_deaths,
    case when e.deaths >= 3 then 'teamfight' when e.deaths = 2 then 'skirmish' else 'pick-off' end as kind,
    case when e.radiant_deaths < e.dire_deaths then 2
         when e.dire_deaths < e.radiant_deaths then 3 end as winning_team,
    e.center_x,
    e.center_y,
    coalesce(d.radiant_hero_damage, 0) as radiant_hero_damage,
    coalesce(d.dire_hero_damage, 0) as dire_hero_damage,
    coalesce(d.participants, 0) as participants,
    b.net_worth_advantage as advantage_before,
    a.net_worth_advantage as advantage_after,
    a.net_worth_advantage - b.net_worth_advantage as advantage_swing
from edges e
left join damage d on d.match_id = e.match_id and d.fight_number = e.fight_number
left join {{ ref('team_advantage_timeline') }} b
  on b.match_id = e.match_id and b.observation_time_s = e.before_time_s
left join {{ ref('team_advantage_timeline') }} a
  on a.match_id = e.match_id and a.observation_time_s = e.after_time_s
