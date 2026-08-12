{#
    ==================================================================
    CDC helpers
    ==================================================================
    The raw layer is ReplacingMergeTree, which means:
      * a key can be present N times until a merge collapses it
      * merges are asynchronous and give no completion guarantee
      * a deleted row is present with _is_deleted = 1 until CLEANUP runs

    So every read of the raw layer must collapse explicitly. These macros
    are the single place that decision is made.
#}

{#
    current_rows(source_relation)

    FINAL-based collapse. Correct at any merge state, one line at each
    call site. Used for dimension-sized tables where the cost of FINAL
    (a merge-on-read across parts) is trivially small.

    Deliberately NOT used for the transaction fact - see
    latest_by_key below.
#}
{% macro current_rows(relation) -%}
    SELECT *
    FROM {{ relation }} FINAL
    WHERE _is_deleted = 0
{%- endmacro %}


{#
    latest_by_key(relation, key_columns, value_columns, filter)

    argMax-based collapse: one row per key, taking the value from the
    row with the highest _version (= source commit time).

    Why not FINAL here: FINAL forces a merge-on-read over ALL columns of
    ALL parts touched by the query, and it disables some read
    optimisations. For a wide, partitioned, high-volume fact table an
    explicit argMax over just the projected columns is materially
    cheaper, and it composes with an incremental filter - FINAL applied
    after an incremental WHERE clause can produce a partially-collapsed
    result, which is a genuinely nasty class of bug.
#}
{% macro latest_by_key(relation, key_columns, value_columns, filter=none) -%}
    SELECT
        {% for key in key_columns -%}
        {{ key }},
        {% endfor -%}
        {% for column in value_columns -%}
        argMax({{ column }}, _version) AS {{ column }},
        {% endfor -%}
        argMax(_is_deleted, _version) AS _is_deleted,
        argMax(_source_commit_at, _version) AS _source_commit_at,
        max(_version) AS _version
    FROM {{ relation }}
    {% if filter %}WHERE {{ filter }}{% endif %}
    GROUP BY {{ key_columns | join(', ') }}
{%- endmacro %}


{#
    incremental_cdc_filter(timestamp_column)

    Standard incremental predicate: reprocess everything committed since
    the newest record already in this model, minus a lookback window.

    The lookback exists because CDC is not strictly ordered end to end -
    a connector restart or a backfill can deliver an event with an older
    commit timestamp after the watermark has moved past it. Re-processing
    a window is free (unique_key makes it idempotent); missing an event
    is not.
#}
{% macro incremental_cdc_filter(timestamp_column='_source_commit_at') -%}
    {% if is_incremental() %}
    {{ timestamp_column }} >= (
        SELECT ifNull(max({{ timestamp_column }}), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
             - INTERVAL {{ var('cdc_lookback_hours', 48) }} HOUR
        FROM {{ this }}
    )
    {% else %}
    1 = 1
    {% endif %}
{%- endmacro %}


{#
    par_bucket(days_expression)

    Portfolio-at-Risk bucketing. Defined once so the mart, the ML feature
    table and the Grafana panels cannot drift apart - a PAR30 that means
    something different in two places is worse than no PAR30 at all.
#}
{% macro par_bucket(days_column) -%}
    multiIf(
        {{ days_column }} IS NULL OR {{ days_column }} <= 0, 'Current',
        {{ days_column }} <= 30,  'PAR 1-30',
        {{ days_column }} <= 60,  'PAR 31-60',
        {{ days_column }} <= 90,  'PAR 61-90',
        {{ days_column }} <= 180, 'PAR 91-180',
        'PAR 180+'
    )
{%- endmacro %}


{#
    Money in this warehouse is Decimal(19,6) end to end. Never Float64:
    a portfolio total that disagrees with the core banking system by
    0.0000001 per row is a reconciliation meeting nobody enjoys.
#}
{% macro money(expression) -%}
    toDecimal64(ifNull({{ expression }}, 0), 6)
{%- endmacro %}


{#
    Safe division that returns NULL (not an exception, not Inf) on a zero
    denominator - ratios over empty cohorts are normal in this domain.
#}
{% macro safe_divide(numerator, denominator) -%}
    if({{ denominator }} = 0 OR {{ denominator }} IS NULL,
       NULL,
       toFloat64({{ numerator }}) / toFloat64({{ denominator }}))
{%- endmacro %}


{#
    surrogate_key(columns)

    cityHash64 over the pipe-joined natural key. Deterministic across
    runs and across environments, which is what makes an incremental
    rebuild produce the same keys as a full refresh.
#}
{% macro surrogate_key(columns) -%}
    cityHash64(concat(
        {%- for column in columns %}
        ifNull(toString({{ column }}), '<null>')
        {%- if not loop.last %}, '|', {% endif %}
        {%- endfor %}
    ))
{%- endmacro %}
