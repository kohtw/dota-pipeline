-- Every game mode with its latest name and label; same columns and types as the seed it replaced.
with versions as (
    {{ constant_versions('baseline_game_modes', 'constant_game_modes', [
        ['game_mode_id', 'integer'],
        ['name', 'text'],
        ['label', 'text'],
    ]) }}
)

select game_mode_id, name, label
from versions
where is_current
order by game_mode_id
