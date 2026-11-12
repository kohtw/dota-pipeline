-- Mart: one row per hero in ranked All Pick matches (stg_steam_ranked).
-- pick_rate = picks / ranked matches; win_rate = wins / picks. A pick is a win when radiant_win matches the side.
with total as (
    select count(distinct match_id) as ranked_matches from {{ ref('stg_steam_ranked') }}
),

per_hero as (
    select
        hero_id,
        count(*) as picks,
        sum(case when radiant_win = on_radiant then 1 else 0 end) as wins
    from {{ ref('stg_steam_ranked') }}
    where hero_id is not null
    group by hero_id
)

select
    h.hero_id,
    d.localized_name,
    cast(h.picks as bigint) as picks,
    cast(h.wins as bigint) as wins,
    t.ranked_matches,
    cast(h.picks as double precision) / t.ranked_matches as pick_rate,
    cast(h.wins as double precision) / h.picks as win_rate
from per_hero h
cross join total t
left join {{ ref('dim_heroes') }} d on d.hero_id = h.hero_id
