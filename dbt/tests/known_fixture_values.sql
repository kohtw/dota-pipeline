-- Hand-checked answers for the fixture match 9000000001 (warehouse/fixtures/sample_match.sql).
-- Each check returns a row when the expected value is WRONG OR MISSING (not exists), so a lost row or a
-- NULL cannot pass silently. Passes when empty.
select 'scoreboard should have 4 players' as problem
where (select count(*) from {{ ref('match_scoreboard') }} where match_id = 9000000001) != 4
union all
select 'gpm slot0 should be 450'
where not exists (select 1 from {{ ref('match_scoreboard') }}
                  where match_id = 9000000001 and player_slot = 0 and abs(gpm - 450) <= 0.001)
union all
select 'slot0 final items should be item_blink,item_desolator'
where not exists (select 1 from {{ ref('match_scoreboard') }}
                  where match_id = 9000000001 and player_slot = 0 and final_items = 'item_blink,item_desolator')
union all
select 'kp slot0 should be 2/3 (the deny is not a kill, enemy deaths = 3)'
where not exists (select 1 from {{ ref('kill_participation') }}
                  where match_id = 9000000001 and player_slot = 0 and abs(kill_participation - 2.0/3) <= 0.0001)
union all
select 'kp slot5 should be 0.5'
where not exists (select 1 from {{ ref('kill_participation') }}
                  where match_id = 9000000001 and player_slot = 5 and abs(kill_participation - 0.5) <= 0.0001)
union all
select 'damage 0->5 attack share should be 300/550'
where not exists (select 1 from {{ ref('damage_by_pair') }}
                  where match_id = 9000000001 and attacker_slot = 0 and target_slot = 5
                    and abs(share_of_team_damage - 300.0/550) <= 0.0001)
union all
-- 1250 = the five enemy hero hits only; excludes the 60 creep damage and the 80 friendly fire
select 'damage_by_pair total should be 1250'
where (select coalesce(sum(total_damage), 0) from {{ ref('damage_by_pair') }} where match_id = 9000000001) != 1250
