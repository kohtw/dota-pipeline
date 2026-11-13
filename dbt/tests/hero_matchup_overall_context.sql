-- Each pair's context must agree with the hero mart and cannot exceed either hero's picks.
select m.hero_id, m.opponent_hero_id, m.matches, m.overall_matches, m.overall_win_rate
from {{ ref('hero_matchup_summary') }} m
left join {{ ref('hero_pick_win_rate') }} h on h.hero_id = m.hero_id
left join {{ ref('hero_pick_win_rate') }} o on o.hero_id = m.opponent_hero_id
where h.hero_id is null or o.hero_id is null
   or m.overall_matches is distinct from h.picks
   or m.overall_win_rate is distinct from h.win_rate
   or m.matches > h.picks or m.matches > o.picks
