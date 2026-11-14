-- Both hero marts must describe the same sample. A missing row on either side is a failure.
select coalesce(h.hero_id, p.hero_id) as hero_id,
       h.appearances, p.picks, h.wins, p.wins as pick_mart_wins,
       h.win_rate, p.win_rate as pick_mart_win_rate
from {{ ref('hero_performance_summary') }} h
full outer join {{ ref('hero_pick_win_rate') }} p on p.hero_id = h.hero_id
where h.hero_id is null or p.hero_id is null
   or h.appearances is distinct from p.picks
   or h.wins is distinct from p.wins
   or h.win_rate is null or p.win_rate is null
   or not (h.win_rate between 0 and 1 and p.win_rate between 0 and 1)
   or abs(h.win_rate - p.win_rate) > 0.000001
