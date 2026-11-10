-- Mart: kill participation = (kills + assists) / team kills, counted from death events.
-- Also keeps the event-based K/D/A so a test can compare it with the player_resource snapshot.
with per_player as (
    select
        p.match_id, p.player_slot, p.team,
        (select count(*) from {{ source('clean', 'deaths') }} d
          where d.match_id = p.match_id and d.killer_slot = p.player_slot)       as kills_from_events,
        (select count(*) from {{ source('clean', 'deaths') }} d
          where d.match_id = p.match_id and d.victim_slot = p.player_slot)       as deaths_from_events,
        (select count(*) from {{ source('clean', 'death_assists') }} a
          where a.match_id = p.match_id and a.assist_slot = p.player_slot)       as assists_from_events
    from {{ source('clean', 'players') }} p
),

with_team as (
    select *, sum(kills_from_events) over (partition by match_id, team) as team_kills
    from per_player
)

select
    cast(match_id as varchar) || '-' || cast(player_slot as varchar) as player_key,
    match_id, player_slot, team,
    kills_from_events, deaths_from_events, assists_from_events, team_kills,
    -- a team with no kills has no meaningful share: NULL, not 0
    cast(kills_from_events + assists_from_events as double precision) / nullif(team_kills, 0) as kill_participation
from with_team
