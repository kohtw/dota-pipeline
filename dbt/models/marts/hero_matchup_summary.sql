-- Mart: one ordered hero/opponent pair across qualifying ranked All Pick matches.
-- Whole-match team results, not lane performance or direct hero-to-hero combat.
with appearances as (
    select match_id, hero_id, on_radiant,
           radiant_win = on_radiant as won
    from {{ ref('stg_steam_ranked') }}
    where hero_id is not null
),

opponent_pairs as (
    -- Each known enemy contributes a separate pairing, counted once per match.
    select distinct a.match_id, a.hero_id, b.hero_id as opponent_hero_id, a.won
    from appearances a
    join appearances b on b.match_id = a.match_id and b.on_radiant != a.on_radiant
),

per_pair as (
    select hero_id, opponent_hero_id,
           count(*) as matches,
           cast(sum(case when won then 1 else 0 end) as bigint) as wins
    from opponent_pairs
    group by hero_id, opponent_hero_id
),

rates as (
    select hero_id, opponent_hero_id, matches, wins,
           matches - wins as losses,
           cast(wins as double precision) / matches as win_rate
    from per_pair
)

select
    r.hero_id,
    h.localized_name as hero_name,
    r.opponent_hero_id,
    o.localized_name as opponent_name,
    r.matches,
    r.wins,
    r.losses,
    r.win_rate,
    p.picks as overall_matches,
    p.win_rate as overall_win_rate,
    100.0 * (r.win_rate - p.win_rate) as win_rate_difference_pp
from rates r
left join {{ ref('dim_heroes') }} h on h.hero_id = r.hero_id
left join {{ ref('dim_heroes') }} o on o.hero_id = r.opponent_hero_id
-- Keep missing context visible to not-null tests rather than dropping observed pairs.
left join {{ ref('hero_pick_win_rate') }} p on p.hero_id = r.hero_id
