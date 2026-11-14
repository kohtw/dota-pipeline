-- Kill participation is a share, so it must be between 0 and 1. Returns players outside that range.
select player_key, kill_participation, kill_involvements, team_kills
from {{ ref('kill_participation') }}
where kill_participation < 0 or kill_participation > 1
   or kill_involvements < 0 or kill_involvements > team_kills
   or (team_kills = 0 and kill_participation is not null)
   or (team_kills > 0 and kill_participation is null)
   or assists_on_team_kills > assists_from_events
   or kill_involvements <> kills_from_events + assists_on_team_kills
