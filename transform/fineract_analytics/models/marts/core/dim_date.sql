{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(date_day)'
    )
}}

/*
    Conformed date dimension.

    Generated from a numbers() spine rather than seeded from a CSV: the
    calendar is a function, not data, and a function does not drift when
    somebody edits a spreadsheet. Bounds come from project vars so the
    reporting window is configuration, not a code change.

    Fiscal calendar assumes a January start; change `fiscal_offset_months`
    if the institution's year differs.
*/

with spine as (

    select
        toDate('{{ var("date_spine_start") }}') + number             as date_day
    from numbers(
        dateDiff('day',
                 toDate('{{ var("date_spine_start") }}'),
                 toDate('{{ var("date_spine_end") }}')) + 1
    )

)

select
    date_day,
    toYYYYMMDD(date_day)                                            as date_key,

    toYear(date_day)                                                as year_number,
    toQuarter(date_day)                                             as quarter_number,
    toMonth(date_day)                                               as month_number,
    toDayOfMonth(date_day)                                          as day_of_month,
    toDayOfWeek(date_day)                                           as day_of_week,
    toDayOfYear(date_day)                                           as day_of_year,
    toISOWeek(date_day)                                             as iso_week,

    -- ClickHouse format specifiers: %b = abbreviated month, %M = full
    -- month name, %a = abbreviated weekday, %W = full weekday. (%B is a
    -- strftime-ism that ClickHouse rejects outright.)
    formatDateTime(date_day, '%b')                                  as month_short_name,
    formatDateTime(date_day, '%M')                                  as month_name,
    formatDateTime(date_day, '%a')                                  as day_short_name,
    formatDateTime(date_day, '%W')                                  as day_name,
    concat(toString(toYear(date_day)), '-Q', toString(toQuarter(date_day))) as year_quarter,
    formatDateTime(date_day, '%Y-%m')                               as year_month,

    toStartOfWeek(date_day)                                         as week_start_date,
    toStartOfMonth(date_day)                                        as month_start_date,
    toLastDayOfMonth(date_day)                                      as month_end_date,
    toStartOfQuarter(date_day)                                      as quarter_start_date,
    toStartOfYear(date_day)                                         as year_start_date,

    if(toDayOfWeek(date_day) >= 6, 1, 0)                            as is_weekend,
    if(toDayOfWeek(date_day) < 6, 1, 0)                             as is_weekday,
    if(date_day = toLastDayOfMonth(date_day), 1, 0)                 as is_month_end,
    if(date_day = toStartOfMonth(date_day), 1, 0)                   as is_month_start,
    if(date_day = toStartOfQuarter(date_day), 1, 0)                 as is_quarter_start,

    if(date_day <= today(), 1, 0)                                   as is_past,
    if(date_day = today(), 1, 0)                                    as is_today,
    dateDiff('day', date_day, today())                              as days_ago

from spine
