-- Every version of every hero (renames, attribute and role changes), with its validity window.
{{ constant_versions('baseline_heroes', 'constant_heroes', [
    ['hero_id', 'integer'],
    ['npc_name', 'text'],
    ['localized_name', 'text'],
    ['primary_attr', 'text'],
    ['roles', 'text'],
]) }}
