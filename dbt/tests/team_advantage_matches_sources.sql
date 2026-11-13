-- Independent Postgres reference: rank raw snapshot candidates per player/grid time.
-- Does not query the aligned-player staging view or use its lateral snapshot selection.
with grid as (
    select m.match_id, g.time_s
    from {{ source('clean', 'matches') }} m
    cross join lateral generate_series(0, floor(m.duration_s / 10)::integer * 10, 10) g(time_s)
),
candidates as (
    select g.match_id, g.time_s, p.team, p.player_slot, s.net_worth,
           g.time_s - s.game_time_s as age_s,
           row_number() over (
               partition by g.match_id, g.time_s, p.player_slot
               order by s.game_time_s desc nulls last, s.tick desc nulls last
           ) as rank
    from grid g
    join {{ source('clean', 'players') }} p on p.match_id = g.match_id
    left join {{ source('clean', 'player_stats') }} s
      on s.match_id = p.match_id and s.player_slot = p.player_slot and s.game_time_s <= g.time_s
),
selected as (
    select *, coalesce(net_worth >= 0 and age_s between 0 and 5, false) as usable
    from candidates where rank = 1
),
totals as (
    select match_id, time_s,
           count(*) filter (where team = 2 and usable) as radiant_count,
           count(*) filter (where team = 3 and usable) as dire_count,
           sum(net_worth) filter (where team = 2 and usable) as radiant_sum,
           sum(net_worth) filter (where team = 3 and usable) as dire_sum,
           max(age_s) as max_age_s
    from selected group by match_id, time_s
),
expected as (
    select match_id, time_s, radiant_count, dire_count, max_age_s,
           case when radiant_count = 5 then radiant_sum end as radiant_total,
           case when dire_count = 5 then dire_sum end as dire_total
    from totals
)

select coalesce(e.match_id, a.match_id) as match_id,
       coalesce(e.time_s, a.observation_time_s) as observation_time_s
from expected e
full outer join {{ ref('team_advantage_timeline') }} a
  on a.match_id = e.match_id and a.observation_time_s = e.time_s
where e.match_id is null or a.match_id is null
   or a.radiant_net_worth is distinct from e.radiant_total
   or a.dire_net_worth is distinct from e.dire_total
   or a.radiant_players_observed is distinct from e.radiant_count
   or a.dire_players_observed is distinct from e.dire_count
   or a.max_snapshot_age_s is distinct from e.max_age_s
   or a.net_worth_advantage is distinct from e.radiant_total - e.dire_total
