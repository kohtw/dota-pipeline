-- No future samples, silent fallback, invented values or inconsistent freshness flags.
select o.match_id, o.player_slot, o.observation_time_s
from {{ ref('stg_replay_player_observations') }} o
left join {{ source('clean', 'player_stats') }} s
  on s.match_id = o.match_id and s.player_slot = o.player_slot and s.tick = o.snapshot_tick
where (o.snapshot_tick is null and
       (o.snapshot_game_time_s is not null or o.snapshot_age_s is not null or o.net_worth is not null))
   or (o.snapshot_tick is not null and
       (s.tick is null or o.snapshot_game_time_s is distinct from s.game_time_s
        or o.net_worth is distinct from s.net_worth
        or o.snapshot_game_time_s > o.observation_time_s
        or o.snapshot_age_s is distinct from o.observation_time_s - o.snapshot_game_time_s))
   or o.is_usable is distinct from
       coalesce(o.net_worth >= 0 and o.snapshot_age_s between 0 and 5, false)
   or exists (
       select 1 from {{ source('clean', 'player_stats') }} newer
       where newer.match_id = o.match_id and newer.player_slot = o.player_slot
         and newer.game_time_s <= o.observation_time_s
         and (o.snapshot_tick is null
              or (newer.game_time_s, newer.tick) > (o.snapshot_game_time_s, o.snapshot_tick))
   )
