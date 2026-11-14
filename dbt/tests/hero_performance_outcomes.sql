-- Returns invalid counts or rates, including NULLs. Empty output means pass.
select hero_id, appearances, wins, losses, win_rate, avg_match_duration_s
from {{ ref('hero_performance_summary') }}
where appearances is null or appearances <= 0
   or wins is null or wins < 0 or wins > appearances
   or losses is null or losses < 0 or wins + losses != appearances
   or win_rate is null or not (win_rate between 0 and 1)
   or abs(win_rate - cast(wins as double precision) / nullif(appearances, 0)) > 0.000001
   or avg_match_duration_s is null
   or not (avg_match_duration_s >= 900 and avg_match_duration_s < 'Infinity'::double precision)
