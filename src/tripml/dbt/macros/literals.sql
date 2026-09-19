{% macro sql_string(value) -%}
  '{{ value | replace("'", "''") }}'
{%- endmacro %}

{% macro sql_string_list(values) -%}
  [
  {%- for value in values -%}
    {{ sql_string(value) }}{% if not loop.last %}, {% endif %}
  {%- endfor -%}
  ]
{%- endmacro %}
