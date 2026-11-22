{#
    Warehouse-specific SQL behind one macro name.
    adapter.dispatch picks snowflake__<name> on Snowflake, else default__<name> (DuckDB).
#}

-- 'YYYY-MM' from a TLC file name like yellow_tripdata_2024-01.parquet
{% macro file_month_from_filename(column) -%}
    {{ return(adapter.dispatch('file_month_from_filename')(column)) }}
{%- endmacro %}

{% macro default__file_month_from_filename(column) -%}
    regexp_extract({{ column }}, '([0-9]{4}-[0-9]{2})[.]parquet', 1)
{%- endmacro %}

{% macro snowflake__file_month_from_filename(column) -%}
    regexp_substr({{ column }}, '([0-9]{4}-[0-9]{2})[.]parquet', 1, 1, 'e', 1)
{%- endmacro %}


-- ISO day of week: 1 = Monday ... 7 = Sunday
{% macro iso_day_of_week(column) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(column)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(column) -%}
    isodow({{ column }})
{%- endmacro %}

{% macro snowflake__iso_day_of_week(column) -%}
    dayofweekiso({{ column }})
{%- endmacro %}