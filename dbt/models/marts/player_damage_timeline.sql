-- Mart: one player in one game minute: damage dealt by target type and damage taken by source.
-- minute = floor(game_time_s / 60), so minute -1 is the last pre-game minute.
-- Every roster player gets every minute the replay recorded (player_stats or damage events).
-- A zero means the combat log recorded no such damage that minute; the log covers the whole
-- replay, so it is a measured zero, not missing data.
-- Hero damage is to enemy heroes only; self-damage and ally damage are left out.
-- Buildings: towers, barracks, the Ancient (fort) and filler buildings. Units: lane, siege and
-- neutral creeps, summons and wards. Roshan is counted on his own.
with players as (
    select match_id, player_slot, team
    from {{ source('clean', 'players') }}
),

events as (
    select e.match_id,
           cast(floor(e.game_time_s / 60) as integer) as minute,
           e.attacker_slot, e.target_slot, e.target_name, e.damage,
           pa.team as attacker_team, pt.team as target_team,
           e.target_slot is null and (e.target_name like '%tower%' or e.target_name like '%fort%'
               or e.target_name like '%rax%' or e.target_name like '%fillers%'
               or e.target_name like '%healers%') as is_building
    from {{ source('clean', 'damage_events') }} e
    left join players pa on pa.match_id = e.match_id and pa.player_slot = e.attacker_slot
    left join players pt on pt.match_id = e.match_id and pt.player_slot = e.target_slot
),

dealt as (
    select match_id, attacker_slot as player_slot, minute,
           sum(case when target_slot is not null and target_team <> attacker_team then damage else 0 end) as damage_to_heroes,
           sum(case when is_building then damage else 0 end) as damage_to_buildings,
           sum(case when target_name = 'npc_dota_roshan' then damage else 0 end) as damage_to_roshan,
           sum(case when target_slot is null and not is_building and target_name <> 'npc_dota_roshan'
                    then damage else 0 end) as damage_to_units
    from events
    where attacker_slot is not null
    group by match_id, attacker_slot, minute
),

taken as (
    select match_id, target_slot as player_slot, minute,
           sum(case when attacker_slot is not null and attacker_team <> target_team then damage else 0 end) as damage_from_heroes,
           sum(case when attacker_slot is null then damage else 0 end) as damage_from_units
    from events
    where target_slot is not null
    group by match_id, target_slot, minute
),

minutes as (
    select distinct match_id, cast(floor(game_time_s / 60) as integer) as minute
    from {{ source('clean', 'player_stats') }}
    union
    select distinct match_id, minute from events
)

select
    cast(p.match_id as varchar) || '-' || cast(p.player_slot as varchar) || '-' || cast(m.minute as varchar) as player_minute_key,
    p.match_id,
    p.player_slot,
    p.team,
    m.minute,
    coalesce(d.damage_to_heroes, 0) as damage_to_heroes,
    coalesce(d.damage_to_buildings, 0) as damage_to_buildings,
    coalesce(d.damage_to_roshan, 0) as damage_to_roshan,
    coalesce(d.damage_to_units, 0) as damage_to_units,
    coalesce(t.damage_from_heroes, 0) as damage_from_heroes,
    coalesce(t.damage_from_units, 0) as damage_from_units
from players p
join minutes m on m.match_id = p.match_id
left join dealt d on d.match_id = p.match_id and d.player_slot = p.player_slot and d.minute = m.minute
left join taken t on t.match_id = p.match_id and t.player_slot = p.player_slot and t.minute = m.minute
