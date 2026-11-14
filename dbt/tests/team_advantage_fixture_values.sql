-- Hand-calculated Postgres fixture answers. Missing expected rows must fail, even for an empty mart.
{{ config(enabled=var('team_advantage_fixture_tests', false)) }}

with expected(match_id, time_s, radiant, dire, advantage, radiant_count, dire_count, max_age, change_120s) as (
    values
        (9200000001::bigint, 0, 20000, 20000, 0, 5, 5, 0.0, NULL::bigint),
        (9200000001, 120, 22000, 25000, -3000, 5, 5, 0.0, -3000),
        (9200000001, 130, 26000, 25000, 1000, 5, 5, 0.0, 1000),
        (9200000002, 0, 0, 0, 0, 5, 5, 0.0, NULL),
        (9200000002, 10, NULL, NULL, NULL, 4, 4, 0.0, NULL),
        (9200000002, 20, 3000, 3000, 0, 5, 5, 0.0, NULL),
        (9200000003, 0, NULL, 3000, NULL, 4, 5, 0.0, NULL),
        (9200000003, 120, 3000, 3000, 0, 5, 5, 0.0, NULL),
        (9200000003, 130, NULL, 3000, NULL, 4, 5, 0.0, NULL),
        (9200000004, 0, 500, 500, 0, 5, 5, 5.0, NULL),
        (9200000004, 10, NULL, 500, NULL, 4, 5, 5.001, NULL),
        (9200000005, 0, NULL, NULL, NULL, 0, 0, NULL, NULL),
        (9200000005, 10, NULL, NULL, NULL, 0, 0, NULL, NULL),
        (9200000006, 10, 600, 500, 100, 5, 5, 1.0, NULL)
)

select 'wrong or missing observation: ' || e.match_id || ' at ' || e.time_s as problem
from expected e
left join {{ ref('team_advantage_timeline') }} a
  on a.match_id = e.match_id and a.observation_time_s = e.time_s
where a.match_id is null
   or a.radiant_net_worth is distinct from e.radiant
   or a.dire_net_worth is distinct from e.dire
   or a.net_worth_advantage is distinct from e.advantage
   or a.radiant_players_observed is distinct from e.radiant_count
   or a.dire_players_observed is distinct from e.dire_count
   or a.net_worth_advantage_change_120s is distinct from e.change_120s
   or (e.max_age is null and a.max_snapshot_age_s is not null)
   or (e.max_age is not null and (a.max_snapshot_age_s is null
       or not (abs(a.max_snapshot_age_s - e.max_age) < 0.000001)))
union all
select 'fixture inputs or complete grid rows are missing'
where (select count(*) from {{ source('clean', 'matches') }}) != 6
   or (select count(*) from {{ source('clean', 'players') }}) != 60
   or (select count(*) from {{ ref('stg_replay_player_observations') }}) != 370
   or (select count(*) from {{ ref('team_advantage_timeline') }}) != 37
   or (select count(net_worth_advantage_change_120s) from {{ ref('team_advantage_timeline') }}) != 2
union all
select 'newer NULL must not fall back to an older valid value'
where not exists (
    select 1 from {{ ref('stg_replay_player_observations') }}
    where match_id = 9200000002 and player_slot = 0 and observation_time_s = 10
      and snapshot_tick = 600 and net_worth is null and not is_usable
)
union all
select 'negative net worth must remain unusable'
where not exists (
    select 1 from {{ ref('stg_replay_player_observations') }}
    where match_id = 9200000002 and player_slot = 5 and observation_time_s = 10
      and snapshot_tick = 600 and net_worth = -1 and not is_usable
)
union all
select 'age exactly five must be usable'
where not exists (
    select 1 from {{ ref('stg_replay_player_observations') }}
    where match_id = 9200000004 and player_slot = 0 and observation_time_s = 10
      and snapshot_tick = 450 and snapshot_age_s = 5 and is_usable
)
union all
select 'stale preceding sample must not be replaced by a closer future sample'
where not exists (
    select 1 from {{ ref('stg_replay_player_observations') }}
    where match_id = 9200000004 and player_slot = 1 and observation_time_s = 10
      and snapshot_tick = 449 and snapshot_game_time_s < 5 and not is_usable
)
union all
select 'equal game times must select the higher tick'
where not exists (
    select 1 from {{ ref('stg_replay_player_observations') }}
    where match_id = 9200000006 and player_slot = 0 and observation_time_s = 10
      and snapshot_tick = 571 and net_worth = 200 and is_usable
)
