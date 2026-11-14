-- Check every metric's coverage, missing-value semantics and nonnegative finite averages.
with metrics as (
    select h.hero_id, h.appearances, m.metric, m.average, m.observations
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

select *
from metrics
where observations is null or observations < 0 or observations > appearances
   or (observations = 0 and average is not null)
   or (observations > 0 and average is null)
   or (average is not null and not (average >= 0 and average < 'Infinity'::double precision))
