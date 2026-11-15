-- Every patch with its latest known release time; same columns and types as the seed it replaced.
with versions as (
    {{ constant_versions('baseline_patches', 'constant_patches', [
        ['patch', 'text'],
        ['released_at', 'timestamp'],
    ]) }}
)

select patch, released_at
from versions
where is_current
order by released_at
