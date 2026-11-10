-- Parser consistency: K/D/A from the player_resource snapshot must equal K/D/A counted from death events.
-- Returns the players that disagree; the test passes when no rows come back.
select s.player_key, s.kills, k.kills_from_events, s.deaths, k.deaths_from_events, s.assists, k.assists_from_events
from {{ ref('match_scoreboard') }} s
join {{ ref('kill_participation') }} k using (player_key)
where s.kills != k.kills_from_events
   or s.deaths != k.deaths_from_events
   or s.assists != k.assists_from_events
