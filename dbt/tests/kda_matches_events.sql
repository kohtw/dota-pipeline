-- Kills/deaths stay exact. Partial assists are accounted for by the cleaner's
-- report; SQL checks their bounds and never treats an unexplained complete gap as valid.
with roster as (
    select cast(match_id as varchar) || '-' || cast(player_slot as varchar) as player_key
    from {{ source('clean', 'players') }}
)
select coalesce(p.player_key, s.player_key, k.player_key) as player_key,
       s.kills, k.kills_from_events, s.deaths, k.deaths_from_events,
       s.assists, k.assists_from_events
from roster p
full outer join {{ ref('match_scoreboard') }} s using (player_key)
full outer join {{ ref('kill_participation') }} k using (player_key)
where p.player_key is null or s.player_key is null or k.player_key is null
   or s.kills is null or s.deaths is null or s.assists is null
   or s.kills is distinct from k.kills_from_events
   or s.deaths is distinct from k.deaths_from_events
   or k.assist_attribution_complete is null
   or k.assist_attribution_assumed is null
   or k.assists_from_events is null
   or k.assists_from_events < 0 or k.assists_from_events > s.assists
   or (k.assist_attribution_complete and s.assists is distinct from k.assists_from_events)
