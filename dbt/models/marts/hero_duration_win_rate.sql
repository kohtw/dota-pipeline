-- Mart: one hero and one match-length bucket in the ranked All Pick sample (stg_steam_ranked).
-- Shows whether a hero wins more in short or long games. Buckets by full match duration:
-- under 30, 30-40, 40-50 and 50+ minutes (the sample starts at 15 minutes).
-- A pick is a win when radiant_win matches the side, as in hero_pick_win_rate.
with appearances as (
    select hero_id,
           radiant_win = on_radiant as won,
           case when duration < 1800 then 1
                when duration < 2400 then 2
                when duration < 3000 then 3
                else 4 end as bucket_order
    from {{ ref('stg_steam_ranked') }}
    where hero_id is not null
)

select
    cast(hero_id as varchar) || '-' || cast(bucket_order as varchar) as hero_duration_key,
    hero_id,
    bucket_order,
    case bucket_order when 1 then 'Under 30 min' when 2 then '30–40 min'
                      when 3 then '40–50 min' else '50+ min' end as duration_bucket,
    cast(count(*) as bigint) as matches,
    cast(sum(case when won then 1 else 0 end) as bigint) as wins,
    cast(sum(case when won then 1 else 0 end) as double precision) / count(*) as win_rate
from appearances
group by hero_id, bucket_order
