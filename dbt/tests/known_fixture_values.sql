-- Hand-checked answers for the fixture match 9000000001 (warehouse/fixtures/sample_match.sql).
-- Returns a row for every expected value that is wrong. Passes when empty.
select 'gpm slot0 should be 450' as problem
from {{ ref('match_scoreboard') }}
where match_id = 9000000001 and player_slot = 0 and abs(gpm - 450) > 0.001
union all
select 'kp slot0 should be 2/3'
from {{ ref('kill_participation') }}
where match_id = 9000000001 and player_slot = 0 and abs(kill_participation - 2.0/3) > 0.0001
union all
select 'kp slot5 should be 0.5'
from {{ ref('kill_participation') }}
where match_id = 9000000001 and player_slot = 5 and abs(kill_participation - 0.5) > 0.0001
union all
select 'radiant damage 0->5 attack share should be 300/550'
from {{ ref('damage_by_pair') }}
where match_id = 9000000001 and attacker_slot = 0 and target_slot = 5 and abs(share_of_team_damage - 300.0/550) > 0.0001
union all
select 'slot0 final items should be item_blink,item_desolator'
from {{ ref('match_scoreboard') }}
where match_id = 9000000001 and player_slot = 0 and final_items != 'item_blink,item_desolator'
union all
select 'creep damage must not reach damage_by_pair'
from {{ ref('damage_by_pair') }}
where attacker_slot is null
