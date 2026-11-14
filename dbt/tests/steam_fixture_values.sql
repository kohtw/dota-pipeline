-- Hand-checked rates for the Steam dev fixture (warehouse/fixtures/sample_steam.sql).
-- Returns a row for every expected value that is wrong. Passes when empty.
-- Target-safe: the checks only apply when the fixture's matches are present and are the only ranked matches
-- (4 of them). On a database without fixture rows (prod), the gate is false and nothing is returned.
with gate as (
    select 1 as ok
    where (select count(*) from {{ source('stg', 'steam_matches') }} where match_id between 9100000001 and 9100000009) = 9
      and (select count(distinct match_id) from {{ ref('stg_steam_ranked') }}) = 4
)

select 'hero 1 should have 3 picks, 1 win, pick_rate 0.75, win_rate 1/3' as problem
from gate
where not exists (
    select 1 from {{ ref('hero_pick_win_rate') }}
    where hero_id = 1 and picks = 3 and wins = 1
      and abs(pick_rate - 0.75) < 0.000001 and abs(win_rate - 1.0/3) < 0.000001
)
union all
select 'hero 3 should have 4 picks, 2 wins, pick_rate 1, win_rate 0.5'
from gate
where not exists (
    select 1 from {{ ref('hero_pick_win_rate') }}
    where hero_id = 3 and picks = 4 and wins = 2
      and abs(pick_rate - 1.0) < 0.000001 and abs(win_rate - 0.5) < 0.000001
)
union all
select 'hero 13 appears only in excluded matches and must not be in the mart'
from gate
where exists (select 1 from {{ ref('hero_pick_win_rate') }} where hero_id = 13)
