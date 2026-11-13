-- Every ordered pair needs the reverse observation, with matching counts and opposite outcomes.
select a.hero_id, a.opponent_hero_id, a.matches, a.wins, a.losses
from {{ ref('hero_matchup_summary') }} a
left join {{ ref('hero_matchup_summary') }} b
  on b.hero_id = a.opponent_hero_id and b.opponent_hero_id = a.hero_id
where b.hero_id is null
   or a.matches is distinct from b.matches
   or a.wins is distinct from b.losses
   or a.losses is distinct from b.wins
