-- Kill participation is a share, so it must be between 0 and 1. Returns players outside that range.
select player_key, kill_participation
from {{ ref('kill_participation') }}
where kill_participation < 0 or kill_participation > 1
