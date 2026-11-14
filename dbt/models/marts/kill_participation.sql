-- Full credited K/D/A stays comparable to the scoreboard. Participation counts
-- assists only on player-credited kills by the assister's team, excluding same-side credit.
with kill_counts as (
    select match_id, credited_killer_slot as player_slot, count(*) as n
    from {{ source('clean', 'deaths') }}
    where credited_killer_slot is not null
    group by match_id, credited_killer_slot
),

death_counts as (
    select match_id, victim_slot as player_slot, count(*) as n
    from {{ source('clean', 'deaths') }}
    group by match_id, victim_slot
),

assist_counts as (
    -- Preserve every game-recorded assist, including same-side credit.
    select match_id, assist_slot as player_slot, count(*) as n
    from {{ source('clean', 'death_assists') }}
    group by match_id, assist_slot
),

team_assist_counts as (
    select a.match_id, a.assist_slot as player_slot, count(*) as n
    from {{ source('clean', 'death_assists') }} a
    join {{ source('clean', 'deaths') }} d
      on d.match_id = a.match_id and d.tick = a.tick and d.victim_slot = a.victim_slot
    join {{ source('clean', 'players') }} killer
      on killer.match_id = d.match_id and killer.player_slot = d.credited_killer_slot
    join {{ source('clean', 'players') }} assister
      on assister.match_id = a.match_id and assister.player_slot = a.assist_slot
    where assister.team = killer.team
      and a.same_side = false
      and a.assist_slot <> d.credited_killer_slot
    group by a.match_id, a.assist_slot
),

per_player as (
    select p.match_id, p.player_slot, p.team,
           coalesce(k.n, 0) as kills_from_events,
           coalesce(d.n, 0) as deaths_from_events,
           coalesce(a.n, 0) as assists_from_events,
           coalesce(t.n, 0) as assists_on_team_kills
    from {{ source('clean', 'players') }} p
    left join kill_counts k on k.match_id = p.match_id and k.player_slot = p.player_slot
    left join death_counts d on d.match_id = p.match_id and d.player_slot = p.player_slot
    left join assist_counts a on a.match_id = p.match_id and a.player_slot = p.player_slot
    left join team_assist_counts t on t.match_id = p.match_id and t.player_slot = p.player_slot
),

with_team as (
    select *, sum(kills_from_events) over (partition by match_id, team) as team_kills,
           kills_from_events + assists_on_team_kills as kill_involvements
    from per_player
)

select
    cast(match_id as varchar) || '-' || cast(player_slot as varchar) as player_key,
    match_id, player_slot, team,
    kills_from_events, deaths_from_events, assists_from_events, team_kills,
    assists_on_team_kills, kill_involvements,
    -- A team with no credited kills has no meaningful share.
    cast(kill_involvements as double precision) / nullif(team_kills, 0) as kill_participation
from with_team
