-- Mart: hero-to-hero damage by attacker, target and damage source.
-- Creeps and towers are left out: they have no attacker_slot / target_slot.
-- share_of_team_damage = this row's damage / all hero damage dealt by the attacker's team.
with hero_damage as (
    select d.match_id, d.attacker_slot, d.target_slot, d.inflictor_name,
           pa.team as attacker_team, pt.team as target_team,
           d.damage, d.hits
    from {{ source('clean', 'damage_events') }} d
    join {{ source('clean', 'players') }} pa on pa.match_id = d.match_id and pa.player_slot = d.attacker_slot
    join {{ source('clean', 'players') }} pt on pt.match_id = d.match_id and pt.player_slot = d.target_slot
),

grouped as (
    select match_id, attacker_slot, target_slot, inflictor_name, attacker_team, target_team,
           sum(damage) as total_damage,
           sum(hits)   as hits
    from hero_damage
    group by match_id, attacker_slot, target_slot, inflictor_name, attacker_team, target_team
)

select
    cast(match_id as varchar) || '-' || cast(attacker_slot as varchar) || '-' || cast(target_slot as varchar) || '-' || inflictor_name as pair_key,
    match_id, attacker_slot, target_slot, inflictor_name, attacker_team, target_team,
    total_damage,
    hits,
    cast(total_damage as double precision)
        / sum(total_damage) over (partition by match_id, attacker_team) as share_of_team_damage
from grouped
