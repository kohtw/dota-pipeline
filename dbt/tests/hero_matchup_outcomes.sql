-- Invalid counts, non-finite rates or a wrong percentage-point calculation fail the test.
select hero_id, opponent_hero_id, matches, wins, losses, win_rate,
       overall_win_rate, win_rate_difference_pp
from {{ ref('hero_matchup_summary') }}
where hero_id is null or opponent_hero_id is null or hero_id = opponent_hero_id
   or matches is null or matches <= 0
   or wins is null or wins < 0 or wins > matches
   or losses is null or losses < 0 or wins + losses != matches
   or win_rate is null or not (win_rate between 0 and 1)
   or abs(win_rate - cast(wins as double precision) / nullif(matches, 0)) > 0.000001
   or overall_win_rate is null or not (overall_win_rate between 0 and 1)
   or win_rate_difference_pp is null or not (win_rate_difference_pp between -100 and 100)
   or abs(win_rate_difference_pp - 100 * (win_rate - overall_win_rate)) > 0.000001
