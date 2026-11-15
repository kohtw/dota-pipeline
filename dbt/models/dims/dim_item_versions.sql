-- Every version of every item (renames, recipe and neutral tier changes), with its validity window.
{{ constant_versions('baseline_items', 'constant_items', [
    ['item_id', 'integer'],
    ['name', 'text'],
    ['localized_name', 'text'],
    ['is_recipe', 'boolean'],
    ['neutral_tier', 'integer'],
]) }}
