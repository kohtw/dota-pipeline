-- Mart: kill participation = (kills + assists) / team kills, counted from death events.
-- Only deaths of ENEMY heroes count. A deny (a hero killed by its own team) is nobody's kill and gives the enemy team nothing,
-- so the share stays within 0-1.
-- team_kills = enemy hero deaths not caused by the enemy's own team (creep and tower kills included).
-- Also keeps event-based K/D/A so a test can compare it with the player_resource snapshot.
with deaths_with_team as (
    select d.match_id, d.tick, d.victim_slot, d.killer_slot, v.team as victim_team, k.team as killer_team
    from {{ source('clean', 'deaths') }} d
    join {{ source('clean', 'players') }} v on v.match_id = d.match_id and v.player_slot = d.victim_slot
    left join {{ source('clean', 'players') }} k on k.match_id = d.match_id and k.player_slot = d.killer_slot  -- NULL when a creep or tower killed
),

per_player as (
    select
        p.match_id, p.player_slot, p.team,
        (select count(*) from deaths_with_team d
          where d.match_id = p.match_id and d.killer_slot = p.player_slot and d.victim_team != p.team) as kills_from_events,
        (select count(*) from deaths_with_team d
          where d.match_id = p.match_id and d.victim_slot = p.player_slot)                              as deaths_from_events,
        (select count(*) from {{ source('clean', 'death_assists') }} a
           join deaths_with_team d on d.match_id = a.match_id and d.tick = a.tick and d.victim_slot = a.victim_slot
          where a.match_id = p.match_id and a.assist_slot = p.player_slot and d.victim_team != p.team
            and d.killer_team is distinct from d.victim_team) as assists_from_events,
        (select count(*) from deaths_with_team d
          where d.match_id = p.match_id and d.victim_team != p.team
            and d.killer_team is distinct from d.victim_team)                                           as team_kills
    from {{ source('clean', 'players') }} p
)

select
    cast(match_id as varchar) || '-' || cast(player_slot as varchar) as player_key,
    match_id, player_slot, team,
    kills_from_events, deaths_from_events, assists_from_events, team_kills,
    -- a team with no kills has no meaningful share: NULL, not 0
    cast(kills_from_events + assists_from_events as double precision) / nullif(team_kills, 0) as kill_participation
from per_player
