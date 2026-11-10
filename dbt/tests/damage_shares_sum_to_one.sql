-- Each team's damage shares must add up to 1. Returns teams where they do not.
select match_id, attacker_team, sum(share_of_team_damage) as total_share
from {{ ref('damage_by_pair') }}
group by match_id, attacker_team
having abs(sum(share_of_team_damage) - 1) > 0.000001
