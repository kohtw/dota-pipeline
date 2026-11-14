-- Read-only Postgres audit for the planned team_advantage_timeline.
-- Run against the replay warehouse, not the Steam fixture database.
begin read only;

select m.match_id, m.duration_s, count(p.player_slot) as roster_players,
       count(*) filter (where p.team = 2) as radiant_players,
       count(*) filter (where p.team = 3) as dire_players,
       count(*) filter (where p.team not in (2, 3)
           or p.player_slot != (p.team - 2) * 5 + p.team_slot
           or p.team_slot not between 0 and 4) as invalid_roster_rows
from clean.matches m
left join clean.players p on p.match_id = m.match_id
group by m.match_id, m.duration_s
order by m.match_id;

with ordered as (
    select s.*, lag(game_time_s) over (partition by match_id, player_slot order by tick) as previous_time_s
    from clean.player_stats s
)
select match_id, count(*) as snapshots, count(distinct player_slot) as players,
       min(game_time_s) as first_time_s, max(game_time_s) as last_time_s,
       count(*) filter (where net_worth is null) as missing_net_worth,
       count(*) filter (where net_worth < 0) as negative_net_worth,
       count(*) filter (where not (game_time_s > '-Infinity'::double precision
                                  and game_time_s < 'Infinity'::double precision)) as nonfinite_times,
       count(*) filter (where game_time_s <= previous_time_s) as nonincreasing_times,
       percentile_cont(0.5) within group (order by game_time_s - previous_time_s) as median_gap_s,
       percentile_cont(0.99) within group (order by game_time_s - previous_time_s) as p99_gap_s,
       max(game_time_s - previous_time_s) as largest_gap_s
from ordered
group by match_id
order by match_id;

select match_id, player_slot, count(*) as snapshots,
       min(game_time_s) as first_time_s, max(game_time_s) as last_time_s
from clean.player_stats
group by match_id, player_slot
order by match_id, player_slot;

select count(*) as duplicate_snapshot_keys
from (
    select match_id, player_slot, tick
    from clean.player_stats
    group by match_id, player_slot, tick
    having count(*) > 1
) duplicates;

select count(*) as orphan_snapshots
from clean.player_stats s
left join clean.players p on p.match_id = s.match_id and p.player_slot = s.player_slot
where p.player_slot is null;

select s.match_id,
       count(*) filter (where s.game_time_s < 0) as pregame_snapshots,
       count(*) filter (where s.game_time_s > m.duration_s) as post_duration_snapshots,
       max(abs(s.game_time_s - (s.tick - m.game_start_tick) / 30.0)) as max_clock_difference_s
from clean.player_stats s
join clean.matches m on m.match_id = s.match_id
group by s.match_id
order by s.match_id;

-- The same fixed grid and latest-at-or-before selection proposed for the staging model.
with grid as (
    select m.match_id, g.observation_time_s
    from clean.matches m
    cross join lateral generate_series(0, floor(m.duration_s / 10)::integer * 10, 10) g(observation_time_s)
    where m.duration_s > 0 and m.duration_s < 'Infinity'::double precision
),
coverage as (
    select g.match_id, g.observation_time_s,
           count(*) filter (where p.team = 2 and s.net_worth >= 0
               and g.observation_time_s - s.game_time_s <= 5) as radiant_usable,
           count(*) filter (where p.team = 3 and s.net_worth >= 0
               and g.observation_time_s - s.game_time_s <= 5) as dire_usable,
           max(g.observation_time_s - s.game_time_s) as oldest_snapshot_age_s
    from grid g
    join clean.players p on p.match_id = g.match_id
    left join lateral (
        select game_time_s, net_worth
        from clean.player_stats s
        where s.match_id = g.match_id and s.player_slot = p.player_slot
          and s.game_time_s <= g.observation_time_s
        order by s.game_time_s desc, s.tick desc
        limit 1
    ) s on true
    group by g.match_id, g.observation_time_s
)
select match_id, count(*) as grid_observations,
       count(*) filter (where radiant_usable = 5 and dire_usable = 5) as complete_observations,
       min(observation_time_s) as first_observation_s,
       max(observation_time_s) as last_observation_s,
       max(oldest_snapshot_age_s) as max_snapshot_age_s,
       max(oldest_snapshot_age_s) filter (where observation_time_s = 0) as initial_snapshot_age_s
from coverage
group by match_id
order by match_id;

commit;
