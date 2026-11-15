-- Mart: the items one player bought at one tick, in buying order, with display names.
-- A recipe purchase usually completes its item, so completed_item_name names the item it builds.
-- is_consumable marks items bought to be used up (wards, regeneration, scrolls, smoke, dust,
-- tomes), so build-order views can hide them.
with purchases as (
    select p.*,
           case when left(p.item_name, 12) = 'item_recipe_'
                then 'item_' || substring(p.item_name from 13) end as recipe_target
    from {{ source('clean', 'purchases') }} p
)

select
    cast(p.match_id as varchar) || '-' || cast(p.tick as varchar) || '-' || cast(p.player_slot as varchar)
        || '-' || p.item_name as purchase_key,
    p.match_id,
    p.player_slot,
    pl.team,
    p.tick,
    p.game_time_s,
    p.item_name,
    coalesce(nullif(trim(d.localized_name), ''),
             replace(case when left(p.item_name, 5) = 'item_' then substring(p.item_name from 6)
                          else p.item_name end, '_', ' ')) as item_display_name,
    p.quantity,
    coalesce(d.is_recipe, p.recipe_target is not null) as is_recipe,
    p.recipe_target as completed_item_name,
    case when p.recipe_target is not null
         then coalesce(nullif(trim(t.localized_name), ''),
                       replace(substring(p.recipe_target from 6), '_', ' ')) end as completed_item_display_name,
    p.item_name in (
        'item_tango', 'item_tango_single', 'item_flask', 'item_clarity', 'item_enchanted_mango',
        'item_faerie_fire', 'item_ward_observer', 'item_ward_sentry', 'item_ward_dispenser',
        'item_tpscroll', 'item_smoke_of_deceit', 'item_dust', 'item_tome_of_knowledge',
        'item_blood_grenade', 'item_famango', 'item_great_famango', 'item_greater_famango',
        'item_recipe_ward_dispenser'
    ) as is_consumable,
    row_number() over (partition by p.match_id, p.player_slot order by p.tick, p.item_name) as purchase_order
from purchases p
join {{ source('clean', 'players') }} pl on pl.match_id = p.match_id and pl.player_slot = p.player_slot
left join {{ ref('dim_items') }} d on d.name = p.item_name
left join {{ ref('dim_items') }} t on t.name = p.recipe_target
