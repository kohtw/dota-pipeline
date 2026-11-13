-- Two independent sources must agree: hero and final K/D/A from our replay against Valve's match summary.
-- Returns the players that disagree; the test passes when no rows come back.
{{ config(severity='error') }}
select match_id, player_slot, hero_id_replay, hero_id_steam, kills_replay, kills_steam,
       deaths_replay, deaths_steam, assists_replay, assists_steam
from {{ ref('steam_replay_reconciliation') }}
where not scoreboard_matches_steam
