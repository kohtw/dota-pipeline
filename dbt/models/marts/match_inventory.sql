-- Mart: one occupied item slot per player at the final recorded hero snapshot.
-- Keep slot identities and unknown items; empty slots have no source row.
select
    cast(i.match_id as varchar) || '-' || cast(i.player_slot as varchar)
        || '-' || cast(i.inventory_slot as varchar) as inventory_key,
    i.match_id,
    i.player_slot,
    i.inventory_slot,
    case
        when i.inventory_slot between 0 and 5 then 'main'
        when i.inventory_slot between 6 and 8 then 'backpack'
        when i.inventory_slot between 9 and 14 then 'stash'
        when i.inventory_slot = 15 then 'teleport'
        when i.inventory_slot = 16 then 'neutral'
    end as location,
    i.item_name,
    coalesce(nullif(trim(d.localized_name), ''),
             replace(case when left(i.item_name, 5) = 'item_' then substring(i.item_name from 6)
                          else i.item_name end, '_', ' ')) as item_display_name
from {{ source('clean', 'final_inventory') }} i
left join {{ ref('dim_items') }} d on d.name = i.item_name
