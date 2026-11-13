-- Independent Postgres reference: build team rosters directly from Steam source tables,
-- expand Radiant/Dire combinations, then compare every ordered pair with the mart.
-- This bypasses stg_steam_ranked and the model's player self-join. Missing or extra rows fail.
with rosters as (
    select m.match_id, m.radiant_win,
           array_agg(p.hero_id) filter (
               where p.player_slot between 0 and 4 and p.hero_id is not null
           ) as radiant_heroes,
           array_agg(p.hero_id) filter (
               where p.player_slot between 128 and 132 and p.hero_id is not null
           ) as dire_heroes
    from {{ source('stg', 'steam_matches') }} m
    join {{ source('stg', 'steam_players') }} p on p.match_id = m.match_id
    where m.lobby_type = 7 and m.game_mode = 22 and m.duration >= 900
      and m.human_players = 10 and m.radiant_win is not null
    group by m.match_id, m.radiant_win
),

observations as (
    select pair.hero_id, pair.opponent_hero_id, pair.won
    from rosters r
    cross join lateral unnest(r.radiant_heroes) as radiant(hero_id)
    cross join lateral unnest(r.dire_heroes) as dire(hero_id)
    cross join lateral (values
        (radiant.hero_id, dire.hero_id, r.radiant_win),
        (dire.hero_id, radiant.hero_id, not r.radiant_win)
    ) as pair(hero_id, opponent_hero_id, won)
),

expected as (
    select hero_id, opponent_hero_id, count(*) as matches,
           count(*) filter (where won) as wins,
           count(*) filter (where not won) as losses
    from observations
    group by hero_id, opponent_hero_id
)

select coalesce(e.hero_id, m.hero_id) as hero_id,
       coalesce(e.opponent_hero_id, m.opponent_hero_id) as opponent_hero_id,
       e.matches as source_matches, m.matches as mart_matches,
       e.wins as source_wins, m.wins as mart_wins,
       e.losses as source_losses, m.losses as mart_losses
from expected e
full outer join {{ ref('hero_matchup_summary') }} m
  on m.hero_id = e.hero_id and m.opponent_hero_id = e.opponent_hero_id
where e.hero_id is null or m.hero_id is null
   or e.matches is distinct from m.matches
   or e.wins is distinct from m.wins
   or e.losses is distinct from m.losses
