-- Synthetic replay records for Postgres timeline checks. Never load into the dashboard database.
-- 1: both teams gain while Radiant falls behind, then the lead changes sides; duration 135 -> grid ends 130.
-- 2: recorded zeros, newer NULL/negative values (no fallback), then complete observations again.
-- 3: missing initial endpoint and incomplete current endpoint for two-minute changes.
-- 4: pregame data, age exactly 5 versus 5.001, and a closer future snapshot that must be ignored.
-- 5: valid roster with no snapshots; all requested grid rows must remain present.
-- 6: equal game times at different ticks; latest tick wins deterministically.
insert into clean.matches (match_id, game_mode, winner_team, game_start_tick, duration_s)
values (9200000001, 22, 2, 300, 135), (9200000002, 22, 2, 300, 20),
       (9200000003, 22, 3, 300, 130), (9200000004, 22, 3, 300, 10),
       (9200000005, 22, 2, 300, 10), (9200000006, 22, 2, 300, 10);

insert into clean.players (match_id, player_slot, team, team_slot, hero_name, hero_id)
select m.match_id, s.slot, case when s.slot < 5 then 2 else 3 end,
       s.slot % 5, 'fixture_hero_' || s.slot, s.slot + 1
from clean.matches m cross join generate_series(0, 9) s(slot);

insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
select 9200000001, s.slot, (t.time_s + 10) * 30, t.time_s,
       case when t.time_s > 135 then 999999
            when t.time_s >= 130 and s.slot < 5 then 5200
            when t.time_s >= 120 and s.slot < 5 then 4400
            when t.time_s >= 120 then 5000 else 4000 end
from generate_series(0, 9) s(slot) cross join generate_series(0, 140, 10) t(time_s);

insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
select 9200000002, s.slot, (t.time_s + 10) * 30, t.time_s,
       case when t.time_s = 0 then 0
            when t.time_s = 10 and s.slot = 0 then null
            when t.time_s = 10 and s.slot = 5 then -1 else 600 end
from generate_series(0, 9) s(slot) cross join generate_series(0, 20, 10) t(time_s);
insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
values (9200000002, 0, 570, 9, 9999);

insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
select 9200000003, s.slot, (t.time_s + 10) * 30, t.time_s,
       case when t.time_s = 130 and s.slot = 0 then null else 600 end
from generate_series(0, 9) s(slot) cross join generate_series(0, 130, 10) t(time_s)
where not (t.time_s = 0 and s.slot = 0);

insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
select 9200000004, s.slot, 150, -5, 100 from generate_series(0, 9) s(slot);
insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
select 9200000004, s.slot, 600, 10, 100 from generate_series(2, 9) s(slot);
insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
values (9200000004, 0, 450, 5, 100),
       (9200000004, 1, 449, 4.999, 100),
       (9200000004, 1, 601, 10.001, 99999);

insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
select 9200000006, s.slot, (t.time_s + 10) * 30, t.time_s, 100
from generate_series(0, 9) s(slot) cross join generate_series(0, 10, 10) t(time_s)
where not (t.time_s = 10 and s.slot = 0);
insert into clean.player_stats (match_id, player_slot, tick, game_time_s, net_worth)
values (9200000006, 0, 570, 9, 100), (9200000006, 0, 571, 9, 200);
