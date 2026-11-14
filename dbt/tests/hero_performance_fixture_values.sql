-- Opt-in exact checks for sample_steam.sql, run only by the isolated Postgres fixture runner.
-- No source-data gate: missing input or missing expected output must fail these checks.
{{ config(enabled=var('hero_performance_fixture_tests', false)) }}

with expected_metrics(hero_id, metric, average, observations) as (
    values
        (1, 'avg_kills', 5::double precision, 2::bigint),
        (2, 'avg_kills', NULL::double precision, 0::bigint),
        (1, 'avg_deaths', 3::double precision, 2::bigint),
        (2, 'avg_deaths', NULL::double precision, 0::bigint),
        (1, 'avg_assists', 4::double precision, 2::bigint),
        (2, 'avg_assists', NULL::double precision, 0::bigint),
        (1, 'avg_gpm', 500::double precision, 2::bigint),
        (2, 'avg_gpm', NULL::double precision, 0::bigint),
        (1, 'avg_xpm', 600::double precision, 2::bigint),
        (2, 'avg_xpm', NULL::double precision, 0::bigint),
        (1, 'avg_hero_damage_per_min', 450::double precision, 2::bigint),
        (2, 'avg_hero_damage_per_min', NULL::double precision, 0::bigint),
        (1, 'avg_hero_healing_per_min', 50::double precision, 2::bigint),
        (2, 'avg_hero_healing_per_min', NULL::double precision, 0::bigint),
        (1, 'avg_tower_damage_per_min', 50::double precision, 2::bigint),
        (2, 'avg_tower_damage_per_min', NULL::double precision, 0::bigint)
),

actual_metrics as (
    select h.hero_id, m.metric, m.average, m.observations
    from {{ ref('hero_performance_summary') }} h
    cross join lateral (values
        ('avg_kills', h.avg_kills, h.kills_observations),
        ('avg_deaths', h.avg_deaths, h.deaths_observations),
        ('avg_assists', h.avg_assists, h.assists_observations),
        ('avg_gpm', h.avg_gpm, h.gpm_observations),
        ('avg_xpm', h.avg_xpm, h.xpm_observations),
        ('avg_hero_damage_per_min', h.avg_hero_damage_per_min, h.hero_damage_observations),
        ('avg_hero_healing_per_min', h.avg_hero_healing_per_min, h.hero_healing_observations),
        ('avg_tower_damage_per_min', h.avg_tower_damage_per_min, h.tower_damage_observations)
    ) as m(metric, average, observations)
)

select 'metric mismatch: hero ' || e.hero_id || ' ' || e.metric as problem
from expected_metrics e
left join actual_metrics a on a.hero_id = e.hero_id and a.metric = e.metric
where a.hero_id is null
   or a.observations is distinct from e.observations
   or (e.average is null and a.average is not null)
   or (e.average is not null and
       (a.average is null or not (abs(a.average - e.average) < 0.000001)))
union all
select 'fixture must contain nine source matches and four qualifying matches'
where (select count(*) from {{ source('stg', 'steam_matches') }}) != 9
   or (select count(distinct match_id) from {{ ref('stg_steam_ranked') }} ) != 4
union all
select 'hero 1 outcomes, duration or name incorrect'
where not exists (
    select 1 from {{ ref('hero_performance_summary') }}
    where hero_id = 1 and appearances = 3 and wins = 1 and losses = 2
      and abs(win_rate - 1.0/3) < 0.000001
      and abs(avg_match_duration_s - 2000) < 0.000001
      and localized_name = 'Anti-Mage'
)
union all
select 'hero 2 outcomes or duration incorrect'
where not exists (
    select 1 from {{ ref('hero_performance_summary') }}
    where hero_id = 2 and appearances = 3 and wins = 2 and losses = 1
      and abs(win_rate - 2.0/3) < 0.000001
      and abs(avg_match_duration_s - 2000) < 0.000001
)
union all
select 'hero 3 existing pick and win expectations changed'
where not exists (
    select 1 from {{ ref('hero_performance_summary') }}
    where hero_id = 3 and appearances = 4 and wins = 2 and losses = 2
      and abs(win_rate - 0.5) < 0.000001
      and kills_observations = 4 and abs(avg_kills - 5) < 0.000001
)
union all
select 'unknown hero must be retained with a NULL name'
where not exists (
    select 1 from {{ ref('hero_performance_summary') }}
    where hero_id = 999999 and localized_name is null
      and appearances = 1 and wins = 0 and losses = 1
)
union all
select 'excluded-only or NULL heroes must be absent'
where exists (
    select 1 from {{ ref('hero_performance_summary') }}
    where hero_id is null or hero_id in (13, 14, 15)
)
union all
select 'unexpected or missing hero rows'
where (select count(*) from {{ ref('hero_performance_summary') }}) != 12

