-- Hand-specified expected results cover tower credit, retained same-side assists,
-- assists without a credited player kill, and NULL participation for a zero-kill team.
with expected(match_id, player_slot, kills, deaths, assists, team_assists,
              team_kills, involvements, participation) as (
    values
      (9000000001, 0, 2, 1, 0, 0, 3, 2, 2.0 / 3.0),
      (9000000001, 1, 1, 1, 2, 2, 3, 3, 1.0),
      (9000000001, 5, 1, 2, 1, 0, 2, 1, 0.5),
      (9000000001, 6, 1, 1, 1, 1, 2, 2, 1.0),
      (9000000002, 0, 1, 0, 1, 0, 1, 1, 1.0),
      (9000000002, 5, 0, 2, 0, 0, 0, 0, null)
),
active_expected as (
    -- Production datasets need not contain the hand-made fixture matches.
    select e.* from expected e
    join {{ source('clean', 'matches') }} m on m.match_id = e.match_id
)
select 'credited totals or participation' as problem, e.match_id, e.player_slot
from active_expected e
left join {{ ref('kill_participation') }} m
  on m.match_id = e.match_id and m.player_slot = e.player_slot
where m.player_key is null
   or m.kills_from_events is distinct from e.kills
   or m.deaths_from_events is distinct from e.deaths
   or m.assists_from_events is distinct from e.assists
   or m.assists_on_team_kills is distinct from e.team_assists
   or m.team_kills is distinct from e.team_kills
   or m.kill_involvements is distinct from e.involvements
   or (e.participation is null and m.kill_participation is not null)
   or (e.participation is not null and (m.kill_participation is null
       or abs(m.kill_participation - e.participation) > 0.000000001))
union all
select 'fixture reconciliation', e.match_id, e.player_slot
from active_expected e
left join {{ ref('steam_replay_reconciliation') }} r
  on r.match_id = e.match_id and r.player_slot = e.player_slot
where r.player_slot is null
   or r.scoreboard_matches_steam is distinct from true
   or r.events_match_scoreboard is distinct from true
