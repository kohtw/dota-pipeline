-- Mart: one ordered hero/ally pair on the same team across qualifying ranked All Pick matches.
-- Whole-match team results, the same-team counterpart of hero_matchup_summary. Each match counts
-- once per pair. Descriptive only: the other eight players, skill and roles also decide outcomes.
with appearances as (
    select match_id, hero_id, on_radiant,
           radiant_win = on_radiant as won
    from {{ ref('stg_steam_ranked') }}
    where hero_id is not null
),

ally_pairs as (
    select distinct a.match_id, a.hero_id, b.hero_id as ally_hero_id, a.won
    from appearances a
    join appearances b on b.match_id = a.match_id and b.on_radiant = a.on_radiant and b.hero_id != a.hero_id
),

per_pair as (
    select hero_id, ally_hero_id,
           count(*) as matches,
           cast(sum(case when won then 1 else 0 end) as bigint) as wins
    from ally_pairs
    group by hero_id, ally_hero_id
)

select
    cast(r.hero_id as varchar) || '-' || cast(r.ally_hero_id as varchar) as hero_ally_key,
    r.hero_id,
    r.ally_hero_id,
    a.localized_name as ally_name,
    cast(r.matches as bigint) as matches,
    r.wins,
    r.matches - r.wins as losses,
    cast(r.wins as double precision) / r.matches as win_rate,
    p.win_rate as overall_win_rate,
    100.0 * (cast(r.wins as double precision) / r.matches - p.win_rate) as win_rate_difference_pp
from per_pair r
left join {{ ref('dim_heroes') }} a on a.hero_id = r.ally_hero_id
left join {{ ref('hero_pick_win_rate') }} p on p.hero_id = r.hero_id
