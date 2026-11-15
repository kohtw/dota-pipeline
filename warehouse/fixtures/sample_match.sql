-- Hand-made fixture: one 10-minute match, 10 players (slots 0-4 Radiant; 5-9 Dire).
-- Four players have full statistics; the other six have final K/D/A only, exercising missing economy coverage.
-- Numbers are chosen so expected dbt results can be checked by hand. Not real data.
-- Kills: slot0 x2 (deaths d1,d2), slot1 x1 (d4), slot5 x1 (d3), slot6 x1 (d5).
-- d1's last hitter is a tower, but slot0 receives kill credit.
-- Assists: slot1 on d1,d2; slot6 on d3; slot5 also receives same-side credit on d1.

INSERT INTO clean.matches VALUES (9000000001, 22, 2, 1700000000, 1800, 600.0, TRUE, FALSE);

INSERT INTO clean.players VALUES
 (9000000001, 0, 2, 0, 'npc_dota_hero_antimage',     1),
 (9000000001, 1, 2, 1, 'npc_dota_hero_crystal_maiden', 5),
 (9000000001, 2, 2, 2, 'npc_dota_hero_juggernaut',   8),
 (9000000001, 3, 2, 3, 'npc_dota_hero_pudge',       14),
 (9000000001, 4, 2, 4, 'npc_dota_hero_lion',        26),
 (9000000001, 5, 3, 0, 'npc_dota_hero_axe',          2),
 (9000000001, 6, 3, 1, 'npc_dota_hero_lina',         25),
 (9000000001, 7, 3, 2, 'npc_dota_hero_sven',        18),
 (9000000001, 8, 3, 3, 'npc_dota_hero_windrunner',  21),
 (9000000001, 9, 3, 4, 'npc_dota_hero_lich',        31);

-- tick 1800 = 0:00, tick 19800 = 10:00
INSERT INTO clean.player_stats VALUES
 (9000000001, 0, 1800,  0.0, 1, 0, 0, 0,  600,  600,    0,    0,   0,  0,    0,    0, 0),
 (9000000001, 0, 19800, 600.0, 12, 2, 1, 0, 6000, 400, 4500, 5000, 80, 10, 1800, 300, 0),
 (9000000001, 1, 1800,  0.0, 1, 0, 0, 0,  600,  600,    0,    0,   0,  0,    0,    0, 0),
 (9000000001, 1, 19800, 600.0, 9, 1, 1, 2, 3000, 200, 2500, 3500, 20,  2,  900,   0, 400),
 (9000000001, 5, 1800,  0.0, 1, 0, 0, 0,  600,  600,    0,    0,   0,  0,    0,    0, 0),
 (9000000001, 5, 19800, 600.0, 11, 1, 2, 1, 5000, 300, 4000, 4800, 60,  5, 1400, 500, 0),
 (9000000001, 6, 1800,  0.0, 1, 0, 0, 0,  600,  600,    0,    0,   0,  0,    0,    0, 0),
 (9000000001, 6, 19800, 600.0, 10, 1, 1, 1, 3500, 250, 3000, 4200, 30,  4, 1100, 100, 0);

INSERT INTO clean.positions VALUES
 (9000000001, 0, 1800, 0.0, -6700.0, -6200.0),
 (9000000001, 5, 1800, 0.0,  6500.0,  6000.0);

-- Map fixture (event marts). Slot 0 plays bot, slot 5 top.
-- Slot 5 dies at tick 4800 (d1): the corpse stays at (-5050, 5000) until the respawn at 2:00
-- (tick 5400, back in the Dire base), so d1 has dead_s 20 and the two corpse samples are dead.
-- Slot 0 dies at tick 16800 (d5) and the replay has no sample after the corpse: no respawn.
INSERT INTO clean.positions VALUES
 (9000000001, 0,  3600,  60.0,  5000.0, -6000.0),
 (9000000001, 0, 16770, 499.0,  5000.0, -6000.0),
 (9000000001, 0, 16830, 501.0,  5100.0, -6000.0),
 (9000000001, 5,  4770,  99.0, -5000.0,  5000.0),
 (9000000001, 5,  4830, 101.0, -5050.0,  5000.0),
 (9000000001, 5,  4860, 102.0, -5050.0,  5000.0),
 (9000000001, 5,  5400, 120.0,  5600.0,  5000.0),
 (9000000001, 5,  6000, 140.0, -5000.0,  4900.0);

INSERT INTO clean.damage_events VALUES
 (9000000001, 3000, 40.0, 'npc_dota_hero_antimage',      'npc_dota_hero_axe',   'dota_unknown',             0, 5, 300, 5),
 (9000000001, 3000, 40.0, 'npc_dota_hero_antimage',      'npc_dota_hero_lina',  'dota_unknown',             0, 6, 100, 2),
 (9000000001, 3300, 50.0, 'npc_dota_hero_crystal_maiden','npc_dota_hero_axe',   'crystal_maiden_frostbite', 1, 5, 150, 1),
 (9000000001, 3600, 60.0, 'npc_dota_hero_axe',           'npc_dota_hero_antimage','dota_unknown',           5, 0, 250, 4),
 (9000000001, 3900, 70.0, 'npc_dota_hero_lina',          'npc_dota_hero_crystal_maiden','lina_laguna_blade',6, 1, 450, 1);
-- creep damage to a hero: attacker_slot is NULL, so it must NOT appear in damage_by_pair
INSERT INTO clean.damage_events VALUES
 (9000000001, 3000, 40.0, 'npc_dota_creep_badguys_melee','npc_dota_hero_antimage','dota_unknown', NULL, 0, 60, 3);

INSERT INTO clean.deaths
 (match_id, tick, game_time_s, victim_slot, killer_slot, killer_name, credited_killer_slot, link_status) VALUES
 (9000000001,  4800,  100.0, 5, NULL, 'npc_dota_goodguys_tower1_mid', 0, 'exact'),
 (9000000001,  7800,  200.0, 6, 0, 'npc_dota_hero_antimage', 0, 'exact'),
 (9000000001, 10800,  300.0, 1, 5, 'npc_dota_hero_axe', 5, 'exact'),
 (9000000001, 13800,  400.0, 5, 1, 'npc_dota_hero_crystal_maiden', 1, 'exact'),
 (9000000001, 16800,  500.0, 0, 6, 'npc_dota_hero_lina', 6, 'exact');

INSERT INTO clean.death_assists
 (match_id, tick, victim_slot, assist_slot, same_side) VALUES
 (9000000001,  4800, 5, 1, FALSE),
 (9000000001,  7800, 6, 1, FALSE),
 (9000000001, 10800, 1, 6, FALSE),
 (9000000001,  4800, 5, 5, TRUE);

INSERT INTO clean.purchases VALUES
 (9000000001, 1800, 0.0, 0, 'item_tango', 2),
 (9000000001, 6000, 140.0, 0, 'item_blink', 1);

INSERT INTO clean.final_inventory VALUES
 (9000000001, 0, 0, 'item_blink'),
 (9000000001, 0, 1, 'item_desolator'),
 (9000000001, 1, 0, 'item_boots'),
 -- Off-main slots exercise location, duplicate items and display-name fallbacks.
 (9000000001, 0, 6, 'item_branches'),
 (9000000001, 0, 7, 'item_branches'),
 (9000000001, 0, 8, 'item_fixture_unknown'),
 (9000000001, 0, 9, 'item_blink'),
 (9000000001, 0, 14, 'item_recipe_phase_boots'),
 (9000000001, 0, 15, 'item_tpscroll'),
 (9000000001, 0, 16, 'item_faded_broach');


-- A second small fixture covers a zero-kill team and an assist on a death with
-- no credited player kill. That assist belongs in K/D/A but not participation.
INSERT INTO clean.matches VALUES (9000000002, 22, 2, 1700000600, 1800, 600.0, TRUE, FALSE);
INSERT INTO clean.players VALUES
 (9000000002, 0, 2, 0, 'npc_dota_hero_antimage', 1),
 (9000000002, 5, 3, 0, 'npc_dota_hero_axe', 2),
 -- Complete roster; no extra K/D/A credit.
 (9000000002, 1, 2, 1, 'npc_dota_hero_crystal_maiden', 5),
 (9000000002, 2, 2, 2, 'npc_dota_hero_juggernaut', 8),
 (9000000002, 3, 2, 3, 'npc_dota_hero_pudge', 14),
 (9000000002, 4, 2, 4, 'npc_dota_hero_lion', 26),
 (9000000002, 6, 3, 1, 'npc_dota_hero_lina', 25),
 (9000000002, 7, 3, 2, 'npc_dota_hero_sven', 18),
 (9000000002, 8, 3, 3, 'npc_dota_hero_windrunner', 21),
 (9000000002, 9, 3, 4, 'npc_dota_hero_lich', 31);
INSERT INTO clean.player_stats VALUES
 (9000000002, 0, 19800, 600.0, 10, 1, 0, 1, 2000, 200, 1500, 2000, 20, 2, 100, 0, 0),
 (9000000002, 5, 19800, 600.0,  9, 0, 2, 0, 1500, 100, 1000, 1500, 10, 1,  50, 0, 0);
INSERT INTO clean.deaths
 (match_id, tick, game_time_s, victim_slot, killer_slot, killer_name, credited_killer_slot, link_status) VALUES
 (9000000002, 4800, 100.0, 5, 0, 'npc_dota_hero_antimage', 0, 'participant_assumed'),
 (9000000002, 7800, 200.0, 5, NULL, 'npc_dota_neutral_centaur_khan', NULL, 'exact');
INSERT INTO clean.death_assists
 (match_id, tick, victim_slot, assist_slot, same_side) VALUES
 (9000000002, 7800, 5, 0, FALSE);
INSERT INTO clean.positions VALUES
 (9000000002, 0, 19800, 600.0, -100.0, -100.0),
 (9000000002, 5, 19800, 600.0, 100.0, 100.0);
INSERT INTO clean.damage_events VALUES
 (9000000002, 4800, 100.0, 'npc_dota_hero_antimage', 'npc_dota_hero_axe', 'dota_unknown', 0, 5, 100, 1);
INSERT INTO clean.final_inventory VALUES
 (9000000002, 0, 0, 'item_tango'),
 (9000000002, 5, 0, 'item_branches');

-- Every roster player needs final K/D/A for credit validation. Added players have
-- zero combat counters and no economic measurements, so missing coverage remains.
INSERT INTO clean.player_stats (match_id, player_slot, tick, game_time_s, kills, deaths, assists)
SELECT p.match_id, p.player_slot, 19800, 600.0, 0, 0, 0
FROM clean.players p
WHERE p.match_id IN (9000000001, 9000000002)
  AND NOT EXISTS (SELECT 1 FROM clean.player_stats s
                  WHERE s.match_id = p.match_id AND s.player_slot = p.player_slot);
