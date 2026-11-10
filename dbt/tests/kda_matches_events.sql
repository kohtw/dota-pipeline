-- Parser consistency: K/D/A from the player_resource snapshot must equal K/D/A counted from death events.
-- Full outer join + "is distinct from" so a missing player or a NULL is reported, not skipped.
-- Returns the players that disagree; passes when empty.
select coalesce(s.player_key, k.player_key) as player_key,
       s.kills, k.kills_from_events, s.deaths, k.deaths_from_events, s.assists, k.assists_from_events
from {{ ref('match_scoreboard') }} s
full outer join {{ ref('kill_participation') }} k on s.player_key = k.player_key
where s.player_key is null
   or k.player_key is null
   or s.kills   is distinct from k.kills_from_events
   or s.deaths  is distinct from k.deaths_from_events
   or s.assists is distinct from k.assists_from_events
