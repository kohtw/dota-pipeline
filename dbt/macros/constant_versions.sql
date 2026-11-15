{#
  Validity windows (slowly changing dimension, type 2) for one kind of reference data: the seed
  baseline, valid from 1970-01-01, followed by the rows refresh_constants appended to stg.constant_*.
  A row opens a new version only when a column differs from the key's previous row, so a history
  row that repeats the baseline or the previous version adds no window. valid_to is the next
  version's valid_from, NULL for the current one.
  columns: [[name, type], ...], key first; the types are the ones the seed had.
#}
{% macro constant_versions(baseline, history, columns) %}
{%- set key = columns[0][0] -%}
with observed as (
    select
        {%- for name, type in columns %}
        cast({{ name }} as {{ type }}) as {{ name }},
        {%- endfor %}
        cast('1970-01-01 00:00:00' as timestamp) as observed_at
    from {{ ref(baseline) }}
    union all
    select
        {%- for name, type in columns %}
        cast({{ name }} as {{ type }}) as {{ name }},
        {%- endfor %}
        observed_at
    from {{ source('stg', history) }}
),

compared as (
    select
        *,
        (row_number() over w = 1
        {%- for name, type in columns[1:] %}
            or {{ name }} is distinct from lag({{ name }}) over w
        {%- endfor %}) as opens_version
    from observed
    window w as (partition by {{ key }} order by observed_at)
),

versions as (
    select
        {%- for name, type in columns %}
        {{ name }},
        {%- endfor %}
        observed_at as valid_from
    from compared
    where opens_version
)

select
    *,
    lead(valid_from) over (partition by {{ key }} order by valid_from) as valid_to,
    lead(valid_from) over (partition by {{ key }} order by valid_from) is null as is_current
from versions
{% endmacro %}
