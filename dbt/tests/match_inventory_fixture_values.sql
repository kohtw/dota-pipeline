-- Hand-checked final slots, including duplicates, unknown items and unnamed recipes.
-- Only runs against the synthetic fixture when that match exists in the source.
with expected(player_slot, inventory_slot, location, item_name, item_display_name) as (
    values
        (0, 0, 'main', 'item_blink', 'Blink Dagger'),
        (0, 1, 'main', 'item_desolator', 'Desolator'),
        (0, 6, 'backpack', 'item_branches', 'Iron Branch'),
        (0, 7, 'backpack', 'item_branches', 'Iron Branch'),
        (0, 8, 'backpack', 'item_fixture_unknown', 'fixture unknown'),
        (0, 9, 'stash', 'item_blink', 'Blink Dagger'),
        (0, 14, 'stash', 'item_recipe_phase_boots', 'recipe phase boots'),
        (0, 15, 'teleport', 'item_tpscroll', 'Town Portal Scroll'),
        (0, 16, 'neutral', 'item_faded_broach', 'Faded Broach'),
        (1, 0, 'main', 'item_boots', 'Boots of Speed')
),

actual as (
    select * from {{ ref('match_inventory') }} where match_id = 9000000001
)

select coalesce(a.player_slot, e.player_slot) as player_slot,
       coalesce(a.inventory_slot, e.inventory_slot) as inventory_slot
from expected e
full outer join actual a on a.player_slot = e.player_slot and a.inventory_slot = e.inventory_slot
where exists (select 1 from {{ source('clean', 'matches') }} where match_id = 9000000001)
  and (a.inventory_key is null or e.inventory_slot is null
       or a.location is distinct from e.location
       or a.item_name is distinct from e.item_name
       or a.item_display_name is distinct from e.item_display_name)
