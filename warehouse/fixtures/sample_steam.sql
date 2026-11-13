-- Hand-made Steam fixture for the hero marts. Not real data; load only into a test database.
-- Matches 9100000001-4 are ranked All Pick, 10 humans (slots 0-4 Radiant, 128-132 Dire).
-- Match 1 lasts 20 minutes; matches 2-4 last 40 minutes:
--   1 Radiant win : R 1,2,3,4,5      D 6,7,8,9,10
--   2 Dire win    : R 1,3,4,5,11     D 6,7,8,9,12
--   3 Radiant win : R 2,3,4,5,11     D 1,6,7,8,9
--   4 Dire win    : R 2,3,4,5,11     D 6,7,8,9,12
-- Matches 9100000005-9 must be dropped by stg_steam_ranked: non-ranked lobby, 600 s, 8 humans,
-- NULL radiant_win, game mode 1. They all field heroes 1 and 13, so a leaking filter shows up.
-- Expected mart (4 ranked matches):
--   hero 1: picks 3 (M1 win, M2 loss, M3 loss as Dire), wins 1, pick_rate 3/4 = 0.75, win_rate 1/3
--   hero 3: picks 4, wins 2 (M1, M3), pick_rate 1.0, win_rate 0.5
--   hero 13: absent (only in excluded matches)

INSERT INTO stg.steam_matches (match_id, match_seq_num, start_time, duration, game_mode, lobby_type,
                               radiant_win, human_players) VALUES
 (9100000001, 9100000001, 1700000000, 2400, 22, 7, TRUE, 10),
 (9100000002, 9100000002, 1700000000, 2400, 22, 7, FALSE, 10),
 (9100000003, 9100000003, 1700000000, 2400, 22, 7, TRUE, 10),
 (9100000004, 9100000004, 1700000000, 2400, 22, 7, FALSE, 10),
 (9100000005, 9100000005, 1700000000, 2400, 22, 0, TRUE, 10),
 (9100000006, 9100000006, 1700000000, 600, 22, 7, TRUE, 10),
 (9100000007, 9100000007, 1700000000, 2400, 22, 7, TRUE, 8),
 (9100000008, 9100000008, 1700000000, 2400, 22, 7, NULL, 10),
 (9100000009, 9100000009, 1700000000, 2400, 1, 7, TRUE, 10);

INSERT INTO stg.steam_players (match_id, player_slot, hero_id, kills, deaths, assists, leaver_status) VALUES
 (9100000001, 0, 1, 5, 5, 5, 0),
 (9100000001, 1, 2, 5, 5, 5, 0),
 (9100000001, 2, 3, 5, 5, 5, 0),
 (9100000001, 3, 4, 5, 5, 5, 0),
 (9100000001, 4, 5, 5, 5, 5, 0),
 (9100000001, 128, 6, 5, 5, 5, 0),
 (9100000001, 129, 7, 5, 5, 5, 0),
 (9100000001, 130, 8, 5, 5, 5, 0),
 (9100000001, 131, 9, 5, 5, 5, 0),
 (9100000001, 132, 10, 5, 5, 5, 0),
 (9100000002, 0, 1, 5, 5, 5, 0),
 (9100000002, 1, 3, 5, 5, 5, 0),
 (9100000002, 2, 4, 5, 5, 5, 0),
 (9100000002, 3, 5, 5, 5, 5, 0),
 (9100000002, 4, 11, 5, 5, 5, 0),
 (9100000002, 128, 6, 5, 5, 5, 0),
 (9100000002, 129, 7, 5, 5, 5, 0),
 (9100000002, 130, 8, 5, 5, 5, 0),
 (9100000002, 131, 9, 5, 5, 5, 0),
 (9100000002, 132, 12, 5, 5, 5, 0),
 (9100000003, 0, 2, 5, 5, 5, 0),
 (9100000003, 1, 3, 5, 5, 5, 0),
 (9100000003, 2, 4, 5, 5, 5, 0),
 (9100000003, 3, 5, 5, 5, 5, 0),
 (9100000003, 4, 11, 5, 5, 5, 0),
 (9100000003, 128, 1, 5, 5, 5, 0),
 (9100000003, 129, 6, 5, 5, 5, 0),
 (9100000003, 130, 7, 5, 5, 5, 0),
 (9100000003, 131, 8, 5, 5, 5, 0),
 (9100000003, 132, 9, 5, 5, 5, 0),
 (9100000004, 0, 2, 5, 5, 5, 0),
 (9100000004, 1, 3, 5, 5, 5, 0),
 (9100000004, 2, 4, 5, 5, 5, 0),
 (9100000004, 3, 5, 5, 5, 5, 0),
 (9100000004, 4, 11, 5, 5, 5, 0),
 (9100000004, 128, 6, 5, 5, 5, 0),
 (9100000004, 129, 7, 5, 5, 5, 0),
 (9100000004, 130, 8, 5, 5, 5, 0),
 (9100000004, 131, 9, 5, 5, 5, 0),
 (9100000004, 132, 12, 5, 5, 5, 0),
 (9100000005, 0, 1, 5, 5, 5, 0),
 (9100000005, 1, 13, 5, 5, 5, 0),
 (9100000005, 2, 14, 5, 5, 5, 0),
 (9100000005, 3, 15, 5, 5, 5, 0),
 (9100000005, 4, 2, 5, 5, 5, 0),
 (9100000005, 128, 6, 5, 5, 5, 0),
 (9100000005, 129, 7, 5, 5, 5, 0),
 (9100000005, 130, 8, 5, 5, 5, 0),
 (9100000005, 131, 9, 5, 5, 5, 0),
 (9100000005, 132, 10, 5, 5, 5, 0),
 (9100000006, 0, 1, 5, 5, 5, 0),
 (9100000006, 1, 13, 5, 5, 5, 0),
 (9100000006, 2, 14, 5, 5, 5, 0),
 (9100000006, 3, 15, 5, 5, 5, 0),
 (9100000006, 4, 2, 5, 5, 5, 0),
 (9100000006, 128, 6, 5, 5, 5, 0),
 (9100000006, 129, 7, 5, 5, 5, 0),
 (9100000006, 130, 8, 5, 5, 5, 0),
 (9100000006, 131, 9, 5, 5, 5, 0),
 (9100000006, 132, 10, 5, 5, 5, 0),
 (9100000007, 0, 1, 5, 5, 5, 0),
 (9100000007, 1, 13, 5, 5, 5, 0),
 (9100000007, 2, 14, 5, 5, 5, 0),
 (9100000007, 3, 15, 5, 5, 5, 0),
 (9100000007, 4, 2, 5, 5, 5, 0),
 (9100000007, 128, 6, 5, 5, 5, 0),
 (9100000007, 129, 7, 5, 5, 5, 0),
 (9100000007, 130, 8, 5, 5, 5, 0),
 (9100000007, 131, 9, 5, 5, 5, 0),
 (9100000007, 132, 10, 5, 5, 5, 0),
 (9100000008, 0, 1, 5, 5, 5, 0),
 (9100000008, 1, 13, 5, 5, 5, 0),
 (9100000008, 2, 14, 5, 5, 5, 0),
 (9100000008, 3, 15, 5, 5, 5, 0),
 (9100000008, 4, 2, 5, 5, 5, 0),
 (9100000008, 128, 6, 5, 5, 5, 0),
 (9100000008, 129, 7, 5, 5, 5, 0),
 (9100000008, 130, 8, 5, 5, 5, 0),
 (9100000008, 131, 9, 5, 5, 5, 0),
 (9100000008, 132, 10, 5, 5, 5, 0),
 (9100000009, 0, 1, 5, 5, 5, 0),
 (9100000009, 1, 13, 5, 5, 5, 0),
 (9100000009, 2, 14, 5, 5, 5, 0),
 (9100000009, 3, 15, 5, 5, 5, 0),
 (9100000009, 4, 2, 5, 5, 5, 0),
 (9100000009, 128, 6, 5, 5, 5, 0),
 (9100000009, 129, 7, 5, 5, 5, 0),
 (9100000009, 130, 8, 5, 5, 5, 0),
 (9100000009, 131, 9, 5, 5, 5, 0),
 (9100000009, 132, 10, 5, 5, 5, 0);

-- Preserve the existing hero 1 and 3 pick/win expectations while varying performance inputs.
UPDATE stg.steam_matches SET duration = 1200 WHERE match_id = 9100000001;

-- Hero 1: GPM 400,600,NULL -> 500 (2 observations); XPM -> 600 (2).
-- Damage/minute: 12000/20,12000/40,NULL -> 450, not 400 (ratio of totals).
-- Healing and tower damage/minute: 0,100,NULL -> 50 (zero must count).
UPDATE stg.steam_players
SET kills = 0, deaths = 2, assists = 0, gold_per_min = 400, xp_per_min = 500,
    hero_damage = 12000, hero_healing = 0, tower_damage = 0
WHERE match_id = 9100000001 AND player_slot = 0;
UPDATE stg.steam_players
SET kills = 10, deaths = 4, assists = 8, gold_per_min = 600, xp_per_min = 700,
    hero_damage = 12000, hero_healing = 4000, tower_damage = 4000
WHERE match_id = 9100000002 AND player_slot = 0;
UPDATE stg.steam_players SET kills = NULL, deaths = NULL, assists = NULL
WHERE match_id = 9100000003 AND player_slot = 128;

-- Hero 2 has no metric observations, including K/D/A; averages must stay NULL.
UPDATE stg.steam_players SET kills = NULL, deaths = NULL, assists = NULL
WHERE hero_id = 2;

-- Unknown hero names must not remove observations; NULL hero IDs must not make a mart row.
UPDATE stg.steam_players SET hero_id = 999999
WHERE match_id = 9100000001 AND player_slot = 132;
UPDATE stg.steam_players SET hero_id = NULL
WHERE match_id = 9100000004 AND player_slot = 132;

-- Leavers remain in the qualifying sample, matching the existing staging rules.
UPDATE stg.steam_players SET leaver_status = 1
WHERE match_id = 9100000002 AND player_slot = 0;

-- Matchup expectations (reuse these matches; do not change existing performance answers):
--   1 vs 6: M1 win, M2 loss, M3 allies -> 2 matches, 1 win, 1 loss.
--   1 vs 2 and 1 vs 3: allies where present in M1/M2, enemies in M3 -> 1 loss each.
--   3 and 4 are allies in every qualifying match -> no opponent pairing.
--   1 vs 999999: 1 win in M1; names remain NULL for the unknown hero, on either side.
--   Heroes 13-15 appear only in excluded matches -> no pairs.
--   M4 has one NULL hero ID -> 40 ordered match-level pairs instead of 50.
-- Total: 190 ordered match-level pairs, aggregated into 78 ordered hero/opponent rows.
