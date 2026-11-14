-- One known hero per qualifying match, across both teams. Duplicates inflate matchup counts.
-- NULL hero IDs remain in staging but will be excluded from both sides of opponent pairing.
select match_id, hero_id, count(*) as appearances
from {{ ref('stg_steam_ranked') }}
where hero_id is not null
group by match_id, hero_id
having count(*) > 1
