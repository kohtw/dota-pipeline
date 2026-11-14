-- Mart: one row per hero in the same ranked All Pick sample as hero_pick_win_rate.
-- Each appearance has equal weight. Missing metrics stay NULL; recorded zeros count.
with player_metrics as (
    select
        hero_id,
        radiant_win = on_radiant as won,
        duration,
        kills, deaths, assists,
        gold_per_min, xp_per_min,
        -- Calculate rates before averaging so longer matches do not get more weight.
        cast(hero_damage as double precision) / nullif(duration / 60.0, 0) as hero_damage_per_min,
        cast(hero_healing as double precision) / nullif(duration / 60.0, 0) as hero_healing_per_min,
        cast(tower_damage as double precision) / nullif(duration / 60.0, 0) as tower_damage_per_min
    from {{ ref('stg_steam_ranked') }}
    where hero_id is not null
),

per_hero as (
    select
        hero_id,
        count(*) as appearances,
        cast(sum(case when won then 1 else 0 end) as bigint) as wins,
        avg(cast(kills as double precision)) as avg_kills,
        avg(cast(deaths as double precision)) as avg_deaths,
        avg(cast(assists as double precision)) as avg_assists,
        avg(cast(gold_per_min as double precision)) as avg_gpm,
        avg(cast(xp_per_min as double precision)) as avg_xpm,
        avg(hero_damage_per_min) as avg_hero_damage_per_min,
        avg(hero_healing_per_min) as avg_hero_healing_per_min,
        avg(tower_damage_per_min) as avg_tower_damage_per_min,
        avg(cast(duration as double precision)) as avg_match_duration_s,
        count(kills) as kills_observations,
        count(deaths) as deaths_observations,
        count(assists) as assists_observations,
        count(gold_per_min) as gpm_observations,
        count(xp_per_min) as xpm_observations,
        count(hero_damage_per_min) as hero_damage_observations,
        count(hero_healing_per_min) as hero_healing_observations,
        count(tower_damage_per_min) as tower_damage_observations
    from player_metrics
    group by hero_id
)

select
    h.hero_id,
    d.localized_name,
    h.appearances,
    h.wins,
    h.appearances - h.wins as losses,
    cast(h.wins as double precision) / h.appearances as win_rate,
    h.avg_kills,
    h.avg_deaths,
    h.avg_assists,
    h.avg_gpm,
    h.avg_xpm,
    h.avg_hero_damage_per_min,
    h.avg_hero_healing_per_min,
    h.avg_tower_damage_per_min,
    h.avg_match_duration_s,
    h.kills_observations,
    h.deaths_observations,
    h.assists_observations,
    h.gpm_observations,
    h.xpm_observations,
    h.hero_damage_observations,
    h.hero_healing_observations,
    h.tower_damage_observations
from per_hero h
left join {{ ref('dim_heroes') }} d on d.hero_id = h.hero_id
