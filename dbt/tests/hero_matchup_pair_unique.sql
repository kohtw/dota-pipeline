-- One row per ordered pair. A reverse pairing is a separate, valid row.
select hero_id, opponent_hero_id, count(*) as rows_per_pair
from {{ ref('hero_matchup_summary') }}
group by hero_id, opponent_hero_id
having count(*) > 1
