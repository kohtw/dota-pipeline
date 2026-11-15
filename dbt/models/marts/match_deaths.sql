-- Mart: one scoreboard death with where it happened, who got credit, how long the hero stayed
-- dead and which fight it belongs to. Feeds the kill feed, death map, event timeline and fights.
-- A fight is a run of deaths with no gap over 15 seconds (the OpenDota teamfight rule).
with players as (
    select match_id, player_slot, team
    from {{ source('clean', 'players') }}
),

assists as (
    select match_id, tick, victim_slot, count(*) as assist_count,
           string_agg(cast(assist_slot as varchar), ',' order by assist_slot) as assist_slots
    from {{ source('clean', 'death_assists') }}
    group by match_id, tick, victim_slot
),

ordered as (
    select d.*,
           lag(d.game_time_s) over (partition by d.match_id order by d.tick, d.victim_slot) as previous_death_s
    from {{ source('clean', 'deaths') }} d
),

fights as (
    select o.*,
           sum(case when previous_death_s is null or game_time_s - previous_death_s > 15 then 1 else 0 end)
               over (partition by match_id order by tick, victim_slot
                     rows between unbounded preceding and current row) as fight_number
    from ordered o
)

select
    cast(f.match_id as varchar) || '-' || cast(f.tick as varchar) || '-' || cast(f.victim_slot as varchar) as death_key,
    f.match_id,
    f.tick,
    f.game_time_s,
    f.victim_slot,
    v.team as victim_team,
    f.credited_killer_slot,
    k.team as killer_team,
    f.killer_slot,
    f.killer_name,
    case
        when f.killer_slot is not null or f.killer_name like 'npc_dota_hero_%' then 'hero'
        when f.killer_name is null then 'unknown'
        when f.killer_name = 'dota_fountain' then 'fountain'
        when f.killer_name like '%tower%' or f.killer_name like '%fort%' or f.killer_name like '%rax%'
          or f.killer_name like '%fillers%' or f.killer_name like '%healers%' then 'building'
        when f.killer_name like 'npc_dota_creep_%' or f.killer_name like '%siege%' then 'lane creep'
        when f.killer_name like 'npc_dota_neutral_%' then 'neutral creep'
        when f.killer_name = 'npc_dota_roshan' then 'roshan'
        else 'summon'
    end as killer_kind,
    f.link_status,
    coalesce(a.assist_count, 0) as assist_count,
    a.assist_slots,
    sd.x,
    sd.y,
    sd.respawn_s,
    sd.dead_s,
    cast(f.fight_number as integer) as fight_number,
    row_number() over (partition by f.match_id order by f.tick, f.victim_slot) = 1 as is_first_blood
from fights f
join players v on v.match_id = f.match_id and v.player_slot = f.victim_slot
left join players k on k.match_id = f.match_id and k.player_slot = f.credited_killer_slot
left join assists a on a.match_id = f.match_id and a.tick = f.tick and a.victim_slot = f.victim_slot
left join {{ ref('stg_hero_deaths') }} sd
  on sd.match_id = f.match_id and sd.tick = f.tick and sd.victim_slot = f.victim_slot
