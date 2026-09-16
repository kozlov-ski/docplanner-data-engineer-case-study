{% macro incremental_partition_filter(source_alias, partition_column, key_columns, lookback_days=2, as_of_date=none) %}
    {% if lookback_days is not integer or lookback_days < 1 %}
        {{ exceptions.raise_compiler_error('lookback_days must be a positive integer') }}
    {% endif %}
    {% set effective_date = as_of_date or run_started_at.strftime('%Y-%m-%d') %}
    {% set as_of = modules.datetime.datetime.strptime(effective_date, '%Y-%m-%d') %}
    {% set window_start = (as_of - modules.datetime.timedelta(days=lookback_days - 1)).strftime('%Y-%m-%d') %}
    {% set window_end = (as_of + modules.datetime.timedelta(days=1)).strftime('%Y-%m-%d') %}

    {% if is_incremental() %}
    where {{ source_alias }}.{{ partition_column }} >= timestamp '{{ window_start }} 00:00:00 UTC'
      and {{ source_alias }}.{{ partition_column }} < timestamp '{{ window_end }} 00:00:00 UTC'
      and not exists (
        select 1 from {{ this }} as captured
        where cast(captured.{{ partition_column }} as date) = cast({{ source_alias }}.{{ partition_column }} as date)
        {% for key_column in key_columns %}
          and captured.{{ key_column }} = {{ source_alias }}.{{ key_column }}
        {% endfor %}
          and captured.{{ partition_column }} >= timestamp '{{ window_start }} 00:00:00 UTC'
          and captured.{{ partition_column }} < timestamp '{{ window_end }} 00:00:00 UTC'
    )
    {% endif %}
{% endmacro %}
