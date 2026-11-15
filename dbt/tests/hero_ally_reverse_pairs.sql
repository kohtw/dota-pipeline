-- Allies share a result: every ordered pair needs its reverse with the same matches and wins.
select a.hero_id, a.ally_hero_id, a.matches, a.wins
from {{ ref('hero_ally_synergy') }} a
left join {{ ref('hero_ally_synergy') }} b
  on b.hero_id = a.ally_hero_id and b.ally_hero_id = a.hero_id
where b.hero_id is null
   or a.matches is distinct from b.matches
   or a.wins is distinct from b.wins
