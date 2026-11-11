{#
    Divide two SQL expressions, returning null instead of an error
    when the denominator is 0.
    Usage: {{ safe_divide('tip_amount', 'fare_amount') }}
#}

{% macro safe_divide(numerator, denominator) -%}
    ({{ numerator }}) / nullif(({{ denominator }}), 0)
{%- endmacro %}