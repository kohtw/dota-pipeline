-- Staging: one roster player at each ten-second observation time of a replay match.
-- Select first, validate second: a newer missing value must not fall back to an older good value.
-- Incremental table, not a view: the grid is expensive (a lateral lookup per player and ten-second
-- step), and as a view it was recomputed by every test and by team_advantage_timeline on every
-- dbt build (most of a 28-minute build at ~300 matches). A match's rows depend only on that
-- match, so each run adds the matches not yet in the table. A match that is reloaded after a
-- re-clean keeps its old rows until `dbt build --full-refresh`.
{{ config(materialized='incremental', unique_key='match_id', incremental_strategy='delete+insert') }}
with grid as (
    select m.match_id, g.observation_time_s
    from {{ source('clean', 'matches') }} m
    cross join lateral generate_series(
        0,
        case when m.duration_s > 0 and m.duration_s < 'Infinity'::double precision
             then floor(m.duration_s / 10)::integer * 10 end,
        10
    ) as g(observation_time_s)
    {% if is_incremental() %}
    where m.match_id not in (select distinct match_id from {{ this }})
    {% endif %}
),

selected as (
    select g.match_id, p.player_slot, p.team, g.observation_time_s,
           s.tick as snapshot_tick, s.game_time_s as snapshot_game_time_s,
           g.observation_time_s - s.game_time_s as snapshot_age_s,
           s.net_worth
    from grid g
    join {{ source('clean', 'players') }} p on p.match_id = g.match_id
    left join lateral (
        select s.tick, s.game_time_s, s.net_worth
        from {{ source('clean', 'player_stats') }} s
        where s.match_id = g.match_id and s.player_slot = p.player_slot
          and s.game_time_s <= g.observation_time_s
        order by s.game_time_s desc, s.tick desc
        limit 1
    ) s on true
)

select match_id, player_slot, team, observation_time_s,
       snapshot_tick, snapshot_game_time_s, snapshot_age_s, net_worth,
       coalesce(net_worth >= 0 and snapshot_age_s between 0 and 5, false) as is_usable
from selected
