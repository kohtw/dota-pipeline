-- Exact expectations for sample_steam.sql: match-length buckets and ally pairs.
-- Opt-in with the matchup fixture flag, since prod has no fixture matches.
{{ config(enabled=var('hero_matchup_fixture_tests', false)) }}

with expected_duration(hero_id, bucket_order, matches, wins) as (
    values
        (1, 1, 1, 1),   -- 9100000001, 20 minutes, Radiant won
        (1, 3, 2, 0),   -- 9100000002 and 9100000003, 40 minutes, both lost
        (2, 1, 1, 1),
        (2, 3, 2, 1)    -- 9100000003 won, 9100000004 lost
),

expected_allies(hero_id, ally_hero_id, matches, wins) as (
    values
        (3, 4, 4, 2),   -- together on Radiant in all four matches
        (1, 2, 1, 1),   -- only 9100000001
        (1, 6, 1, 0),   -- 9100000003 on Dire, lost
        (6, 999999, 1, 0)
)

select 'wrong or missing duration bucket: hero ' || e.hero_id || ' bucket ' || e.bucket_order as problem
from expected_duration e
where not exists (
    select 1 from {{ ref('hero_duration_win_rate') }} d
    where d.hero_id = e.hero_id and d.bucket_order = e.bucket_order
      and d.matches = e.matches and d.wins = e.wins
      and abs(d.win_rate - cast(e.wins as double precision) / e.matches) < 0.000001
)
union all
select 'wrong or missing ally pair: ' || e.hero_id || ' with ' || e.ally_hero_id
from expected_allies e
where not exists (
    select 1 from {{ ref('hero_ally_synergy') }} s
    where s.hero_id = e.hero_id and s.ally_hero_id = e.ally_hero_id
      and s.matches = e.matches and s.wins = e.wins and s.losses = e.matches - e.wins
)
union all
select 'opponents and missing hero IDs must not form ally pairs'
where exists (
    select 1 from {{ ref('hero_ally_synergy') }}
    where (hero_id = 1 and ally_hero_id = 999999) or hero_id = ally_hero_id
)
