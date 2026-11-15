-- Mart: one player's part in one fight (see match_deaths for how deaths group into fights).
-- The fight window runs from 10 seconds before its first death to 3 seconds after its last, so
-- the damage that set up the kills counts. Damage is hero-to-enemy-hero only; self and ally
-- damage are left out. A player appears when they dealt or took such damage, died, got a kill
-- or got an assist in the fight.
with windows as (
    select match_id, fight_number,
           min(game_time_s) - 10 as window_start_s,
           max(game_time_s) + 3 as window_end_s
    from {{ ref('match_deaths') }}
    group by match_id, fight_number
),

players as (
    select match_id, player_slot, team
    from {{ source('clean', 'players') }}
),

enemy_damage as (
    select w.match_id, w.fight_number, e.attacker_slot, e.target_slot, sum(e.damage) as damage
    from windows w
    join {{ source('clean', 'damage_events') }} e
      on e.match_id = w.match_id and e.game_time_s between w.window_start_s and w.window_end_s
    join players pa on pa.match_id = e.match_id and pa.player_slot = e.attacker_slot
    join players pt on pt.match_id = e.match_id and pt.player_slot = e.target_slot
    where pa.team <> pt.team
    group by w.match_id, w.fight_number, e.attacker_slot, e.target_slot
),

dealt as (
    select match_id, fight_number, attacker_slot as player_slot, sum(damage) as damage_dealt
    from enemy_damage group by match_id, fight_number, attacker_slot
),

taken as (
    select match_id, fight_number, target_slot as player_slot, sum(damage) as damage_taken
    from enemy_damage group by match_id, fight_number, target_slot
),

kills as (
    select match_id, fight_number, credited_killer_slot as player_slot, count(*) as kills
    from {{ ref('match_deaths') }}
    where credited_killer_slot is not null
    group by match_id, fight_number, credited_killer_slot
),

deaths as (
    select match_id, fight_number, victim_slot as player_slot, count(*) as deaths
    from {{ ref('match_deaths') }}
    group by match_id, fight_number, victim_slot
),

assists as (
    select d.match_id, d.fight_number, a.assist_slot as player_slot, count(*) as assists
    from {{ source('clean', 'death_assists') }} a
    join {{ ref('match_deaths') }} d
      on d.match_id = a.match_id and d.tick = a.tick and d.victim_slot = a.victim_slot
    group by d.match_id, d.fight_number, a.assist_slot
),

grid as (
    select w.match_id, w.fight_number, p.player_slot, p.team
    from windows w
    join players p on p.match_id = w.match_id
)

select
    cast(g.match_id as varchar) || '-' || cast(g.fight_number as varchar) || '-' || cast(g.player_slot as varchar) as fight_player_key,
    g.match_id,
    g.fight_number,
    g.player_slot,
    g.team,
    coalesce(k.kills, 0) as kills,
    coalesce(d.deaths, 0) as deaths,
    coalesce(a.assists, 0) as assists,
    coalesce(dl.damage_dealt, 0) as damage_dealt,
    coalesce(t.damage_taken, 0) as damage_taken
from grid g
left join kills k on k.match_id = g.match_id and k.fight_number = g.fight_number and k.player_slot = g.player_slot
left join deaths d on d.match_id = g.match_id and d.fight_number = g.fight_number and d.player_slot = g.player_slot
left join assists a on a.match_id = g.match_id and a.fight_number = g.fight_number and a.player_slot = g.player_slot
left join dealt dl on dl.match_id = g.match_id and dl.fight_number = g.fight_number and dl.player_slot = g.player_slot
left join taken t on t.match_id = g.match_id and t.fight_number = g.fight_number and t.player_slot = g.player_slot
where k.kills is not null or d.deaths is not null or a.assists is not null
   or dl.damage_dealt is not null or t.damage_taken is not null
