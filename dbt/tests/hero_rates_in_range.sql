-- Pick rate and win rate are shares, so they must be between 0 and 1. Returns heroes outside that range.
select hero_id, pick_rate, win_rate
from {{ ref('hero_pick_win_rate') }}
where pick_rate < 0 or pick_rate > 1 or win_rate < 0 or win_rate > 1
