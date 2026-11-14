-- Exact expectations for sample_steam.sql. Opt-in only; never gate on fixture presence.
-- Missing required rows or source inputs must fail instead of producing an empty pass.
{{ config(enabled=var('hero_matchup_fixture_tests', false)) }}

with expected(hero_id, opponent_hero_id, matches, wins, losses, overall_matches, overall_win_rate) as (
    values
        (1, 6, 2, 1, 1, 3, 1.0/3),
        (6, 1, 2, 1, 1, 4, 0.5),
        (1, 2, 1, 0, 1, 3, 1.0/3),
        (2, 1, 1, 1, 0, 3, 2.0/3),
        (1, 3, 1, 0, 1, 3, 1.0/3),
        (3, 1, 1, 1, 0, 4, 0.5),
        (1, 999999, 1, 1, 0, 3, 1.0/3),
        (999999, 1, 1, 0, 1, 1, 0.0)
)

select 'wrong or missing pair: ' || e.hero_id || ' vs ' || e.opponent_hero_id as problem
from expected e
where not exists (
    select 1 from {{ ref('hero_matchup_summary') }} m
    where m.hero_id = e.hero_id and m.opponent_hero_id = e.opponent_hero_id
      and m.matches = e.matches and m.wins = e.wins and m.losses = e.losses
      and m.overall_matches = e.overall_matches
      and abs(m.overall_win_rate - e.overall_win_rate) < 0.000001
      and abs(m.win_rate - cast(e.wins as double precision) / e.matches) < 0.000001
      and abs(m.win_rate_difference_pp
              - 100 * (cast(e.wins as double precision) / e.matches - e.overall_win_rate)) < 0.000001
)
union all
select 'fixture must contain nine source matches and four qualifying matches'
where (select count(*) from {{ source('stg', 'steam_matches') }}) != 9
   or (select count(distinct match_id) from {{ ref('stg_steam_ranked') }}) != 4
union all
select 'ally-only heroes 3 and 4 must not form opponent pairs'
where exists (
    select 1 from {{ ref('hero_matchup_summary') }}
    where (hero_id = 3 and opponent_hero_id = 4) or (hero_id = 4 and opponent_hero_id = 3)
)
union all
select 'unknown opponent must retain a NULL name'
where not exists (
    select 1 from {{ ref('hero_matchup_summary') }}
    where hero_id = 1 and hero_name = 'Anti-Mage' and opponent_hero_id = 999999
      and opponent_name is null
)
union all
select 'unknown selected hero must retain a NULL name'
where not exists (
    select 1 from {{ ref('hero_matchup_summary') }}
    where hero_id = 999999 and hero_name is null and opponent_hero_id = 1
      and opponent_name = 'Anti-Mage'
)
union all
select 'NULL and excluded-only heroes must not form opponent pairs'
where exists (
    select 1 from {{ ref('hero_matchup_summary') }}
    where hero_id is null or opponent_hero_id is null
       or hero_id in (13, 14, 15) or opponent_hero_id in (13, 14, 15)
)
union all
select 'unexpected or missing matchup rows'
where (select count(*) from {{ ref('hero_matchup_summary') }}) != 78
union all
select 'a missing hero ID must not create an opponent or remove other known pairs'
where (select coalesce(sum(matches), 0) from {{ ref('hero_matchup_summary') }}) != 190
