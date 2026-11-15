-- Mart: one hero and one item across the parsed replays: how often the hero bought it and when.
-- Counts each match once, at the hero's first purchase of the item. Consumables are left out;
-- a recipe counts as its completed item, which is when that item was finished.
-- A small replay sample: purchase_rate and timings describe these matches only.
with heroes as (
    select match_id, player_slot, hero_name
    from {{ source('clean', 'players') }}
),

items as (
    select match_id, player_slot, game_time_s,
           case when is_recipe and completed_item_name is not null then completed_item_name else item_name end as item_name,
           case when is_recipe and completed_item_name is not null then completed_item_display_name
                else item_display_name end as item_display_name
    from {{ ref('player_purchases') }}
    where not is_consumable
),

first_buys as (
    select h.hero_name, i.match_id, i.item_name,
           min(i.item_display_name) as item_display_name,
           min(i.game_time_s) as first_purchase_s
    from items i
    join heroes h on h.match_id = i.match_id and h.player_slot = i.player_slot
    group by h.hero_name, i.match_id, i.item_name
),

hero_matches as (
    select hero_name, count(*) as hero_matches
    from heroes
    group by hero_name
)

select
    f.hero_name || '-' || f.item_name as hero_item_key,
    f.hero_name,
    dh.hero_id,
    dh.localized_name as hero_localized_name,
    f.item_name,
    min(f.item_display_name) as item_display_name,
    hm.hero_matches,
    count(*) as matches_bought,
    cast(count(*) as double precision) / hm.hero_matches as purchase_rate,
    percentile_cont(0.5) within group (order by f.first_purchase_s) as median_first_purchase_s,
    min(f.first_purchase_s) as fastest_first_purchase_s
from first_buys f
join hero_matches hm on hm.hero_name = f.hero_name
left join {{ ref('dim_heroes') }} dh on dh.npc_name = f.hero_name
group by f.hero_name, dh.hero_id, dh.localized_name, f.item_name, hm.hero_matches
