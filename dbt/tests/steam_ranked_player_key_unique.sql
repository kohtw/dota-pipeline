-- One qualifying appearance per match and Steam player slot; duplicates would inflate hero aggregates.
select match_id, player_slot, count(*) as appearances
from {{ ref('stg_steam_ranked') }}
group by match_id, player_slot
having count(*) > 1
