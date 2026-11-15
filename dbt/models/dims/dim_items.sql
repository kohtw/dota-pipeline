-- The current version of every item; same columns and types as the seed it replaced.
select item_id, name, localized_name, is_recipe, neutral_tier
from {{ ref('dim_item_versions') }}
where is_current
order by item_id
