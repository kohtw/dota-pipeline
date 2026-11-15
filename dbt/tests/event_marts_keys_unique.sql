-- One corpse row per death in stg_hero_deaths, which the event marts join on.
-- (stg_hero_positions groups by its own key, so it is unique by construction.)
select match_id, tick, victim_slot, count(*) as n
from {{ ref('stg_hero_deaths') }}
group by match_id, tick, victim_slot
having count(*) > 1
