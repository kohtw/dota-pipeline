-- The current version of every hero; same columns and types as the seed it replaced.
select hero_id, npc_name, localized_name, primary_attr, roles
from {{ ref('dim_hero_versions') }}
where is_current
order by hero_id
