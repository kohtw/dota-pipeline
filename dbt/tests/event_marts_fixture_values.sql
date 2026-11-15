-- Hand-checked answers for the event marts on the fixture matches (warehouse/fixtures/sample_match.sql).
-- Returns a row for every expected value that is wrong. Passes when empty.
select 'd1: corpse (-5050, 5000), respawn 2:00, dead 20 s, tower last hit, first blood, assists 1,5' as problem
from {{ ref('match_deaths') }}
where death_key = '9000000001-4800-5'
  and not (x = -5050 and y = 5000 and respawn_s = 120 and dead_s = 20 and killer_kind = 'building'
           and is_first_blood and assist_count = 2 and assist_slots = '1,5' and fight_number = 1
           and credited_killer_slot = 0 and killer_team = 2 and victim_team = 3)
union all
select 'd5: corpse (5100, -6000) with no respawn, fight 5'
from {{ ref('match_deaths') }}
where death_key = '9000000001-16800-0'
  and not (x = 5100 and y = -6000 and respawn_s is null and fight_number = 5 and not is_first_blood)
union all
select 'match 2: neutral last hit with no credit'
from {{ ref('match_deaths') }}
where death_key = '9000000002-7800-5'
  and not (killer_kind = 'neutral creep' and credited_killer_slot is null and killer_team is null)
union all
-- Counts group by match, so a warehouse without the fixture returns no rows.
select 'match 1 should have 5 pick-offs'
from {{ ref('match_fights') }}
where match_id = 9000000001
group by match_id
having count(*) filter (where kind = 'pick-off') != 5
union all
select 'match 2 fight 1: Radiant 100 hero damage, Radiant wins'
from {{ ref('match_fights') }}
where fight_key = '9000000002-1'
  and not (radiant_hero_damage = 100 and dire_hero_damage = 0 and winning_team = 2 and participants = 2)
union all
select 'match 1 fight 1: killer, assisters and victim take part'
from {{ ref('match_fight_players') }}
where match_id = 9000000001 and fight_number = 1
group by match_id
having count(*) != 3
union all
select 'slot0 minute 0: 400 to heroes, 60 from units'
from {{ ref('player_damage_timeline') }}
where player_minute_key = '9000000001-0-0'
  and not (damage_to_heroes = 400 and damage_from_units = 60 and damage_from_heroes = 0)
union all
select 'slot0 minute 1: 250 from heroes'
from {{ ref('player_damage_timeline') }}
where player_minute_key = '9000000001-0-1' and damage_from_heroes != 250
union all
select 'purchases: tango consumable first, blink second'
from {{ ref('player_purchases') }}
where match_id = 9000000001 and player_slot = 0
  and not ((item_name = 'item_tango' and is_consumable and purchase_order = 1)
        or (item_name = 'item_blink' and not is_consumable and purchase_order = 2
            and item_display_name = 'Blink Dagger'))
union all
select 'lanes: slot0 bot safe, slot5 top safe'
from {{ ref('player_lanes') }}
where match_id = 9000000001 and player_slot in (0, 5)
  and not ((player_slot = 0 and lane = 'bot' and lane_role = 'safe' and net_worth_10 = 6000)
        or (player_slot = 5 and lane = 'top' and lane_role = 'safe'))
union all
select 'slot5 at 1:40 is dead'
from {{ ref('match_positions') }}
where position_key = '9000000001-5-100' and is_alive
union all
select 'antimage bought blink in 1 of 2 matches at 2:20'
from {{ ref('hero_item_timings') }}
where hero_item_key = 'npc_dota_hero_antimage-item_blink'
  and not (matches_bought = 1 and hero_matches = 2 and median_first_purchase_s = 140)
