{#
  For a versions model (valid_from, valid_to, is_current per key): fails on a repeated
  (key, valid_from), more than one current row for a key, a window that ends before it starts, or
  two windows of one key that overlap.
#}
{% test valid_windows(model, key) %}
with windows as (
    select {{ key }} as key_value, valid_from, valid_to, is_current
    from {{ model }}
)

select key_value, 'repeated valid_from' as problem
from windows
group by key_value, valid_from
having count(*) > 1

union all

select key_value, 'more than one current row'
from windows
where is_current
group by key_value
having count(*) > 1

union all

select key_value, 'window ends before it starts'
from windows
where valid_to is not null and valid_to <= valid_from

union all

select a.key_value, 'overlapping windows'
from windows a
join windows b
    on a.key_value = b.key_value
    and a.valid_from < b.valid_from
    and (a.valid_to is null or a.valid_to > b.valid_from)
{% endtest %}
