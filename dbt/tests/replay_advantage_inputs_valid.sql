-- Fail before building team totals when roster, duration or snapshot identities are invalid.
-- depends_on: {{ ref('stg_replay_player_observations') }}
with rosters as (
    select m.match_id, m.duration_s, count(p.player_slot) as players,
           count(distinct p.player_slot) as distinct_players,
           count(*) filter (where p.team = 2) as radiant_players,
           count(*) filter (where p.team = 3) as dire_players,
           count(*) filter (where p.team not in (2, 3) or p.team_slot not between 0 and 4
               or p.player_slot != (p.team - 2) * 5 + p.team_slot) as invalid_players
    from {{ source('clean', 'matches') }} m
    left join {{ source('clean', 'players') }} p on p.match_id = m.match_id
    group by m.match_id, m.duration_s
),
ordered as (
    select s.*, lag(game_time_s) over (partition by match_id, player_slot order by tick) as previous_time_s
    from {{ source('clean', 'player_stats') }} s
)

select match_id, 'invalid duration or roster' as problem
from rosters
where duration_s is null or not (duration_s > 0 and duration_s < 'Infinity'::double precision)
   or players != 10 or distinct_players != 10 or radiant_players != 5 or dire_players != 5
   or invalid_players != 0
union all
select match_id, 'duplicate snapshot key'
from {{ source('clean', 'player_stats') }}
group by match_id, player_slot, tick
having count(*) > 1
union all
select s.match_id, 'invalid snapshot identity or game time'
from ordered s
left join {{ source('clean', 'players') }} p on p.match_id = s.match_id and p.player_slot = s.player_slot
left join {{ source('clean', 'matches') }} m on m.match_id = s.match_id
where p.player_slot is null or m.match_id is null or s.tick is null or s.game_time_s is null
   or not (s.game_time_s > '-Infinity'::double precision and s.game_time_s < 'Infinity'::double precision)
   or s.game_time_s < s.previous_time_s
