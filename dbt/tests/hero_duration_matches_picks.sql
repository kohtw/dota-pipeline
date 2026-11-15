-- A hero's match-length buckets cover every pick exactly once, with the same wins.
select p.hero_id, p.picks, p.wins, d.matches, d.wins as bucket_wins
from {{ ref('hero_pick_win_rate') }} p
full join (
    select hero_id, sum(matches) as matches, sum(wins) as wins
    from {{ ref('hero_duration_win_rate') }}
    group by hero_id
) d on d.hero_id = p.hero_id
where p.hero_id is null or d.hero_id is null
   or p.picks != d.matches or p.wins != d.wins
