-- damage_by_pair counts damage to ENEMY heroes only: friendly fire and self damage must not appear.
-- Returns every row where attacker and target are on the same team. Passes when empty.
select pair_key, match_id, attacker_slot, target_slot
from {{ ref('damage_by_pair') }}
where attacker_team = target_team
