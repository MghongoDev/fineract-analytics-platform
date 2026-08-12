{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='snapshot_key',
        engine='MergeTree()',
        partition_by='toYYYYMM(snapshot_date)',
        order_by='(snapshot_date, office_id, product_id)',
        settings={'index_granularity': 8192, 'allow_nullable_key': 1}
    )
}}

/*
    Daily portfolio snapshot by office x product.

    WHY A SNAPSHOT MODEL AT ALL
    ---------------------------
    The Fineract loan resource reports *current* balances. There is no
    "balance as of last Tuesday" endpoint, and the CDC stream only tells
    us when a row changed, not what the aggregate looked like on a day
    nobody touched it. So portfolio history has to be accumulated: each
    run appends today's aggregate, keyed by snapshot_date.

    `delete+insert` on `snapshot_key` makes a same-day re-run replace
    rather than duplicate - important because the pipeline runs several
    times a day and the last run of the day should win.

    Once accumulated, this table answers the questions the loan fact
    cannot: PAR trend, book growth, month-on-month portfolio movement.
    It is also the source for the executive Grafana dashboard.
*/

with loans as (

    select * from {{ ref('stg_fineract__loans') }}

),

snapshot as (

    /*
        NOTE ON THE `l.` PREFIXES BELOW
        --------------------------------
        In ClickHouse, an alias defined in the SELECT list is visible to
        the other expressions in that same SELECT list. So

            sumIf(total_outstanding, is_active = 1) AS total_outstanding,
            sumIf(total_outstanding, days_past_due > 30) AS par30_amount

        resolves the second `total_outstanding` to the *alias* - i.e. an
        aggregate inside an aggregate - and the query fails (or worse, on
        some versions, silently means something else). Qualifying every
        source column with the relation alias removes the ambiguity.
    */
    select
        today()                                                     as snapshot_date,
        l.office_id                                                 as office_id,
        l.product_id                                                as product_id,

        -- counts
        count()                                                     as loans_total,
        countIf(l.is_active = 1)                                    as loans_active,
        countIf(l.is_closed = 1)                                    as loans_closed,
        countIf(l.is_written_off = 1)                               as loans_written_off,
        countIf(l.is_active = 1 and l.days_past_due > 0)            as loans_delinquent,
        countIf(l.is_active = 1 and l.days_past_due > 30)           as loans_par30,
        countIf(l.is_active = 1 and l.days_past_due > 90)           as loans_par90,
        countDistinct(l.client_id)                                  as distinct_borrowers,

        -- balances
        sumIf(l.principal_outstanding, l.is_active = 1)             as principal_outstanding,
        sumIf(l.interest_outstanding, l.is_active = 1)              as interest_outstanding,
        sumIf(l.total_outstanding, l.is_active = 1)                 as total_outstanding,
        sumIf(l.total_overdue, l.is_active = 1)                     as total_overdue,
        sumIf(l.total_outstanding, l.is_active = 1 and l.days_past_due > 30) as par30_amount,
        sumIf(l.total_outstanding, l.is_active = 1 and l.days_past_due > 90) as par90_amount,
        sum(l.principal_disbursed)                                  as cumulative_disbursed,
        sum(l.principal_written_off)                                as cumulative_written_off,

        -- shape of the book
        avgIf(toFloat64(l.principal), l.is_active = 1)              as avg_active_principal,
        maxIf(l.days_past_due, l.is_active = 1)                     as max_days_past_due,
        avgIf(toFloat64(l.days_past_due), l.is_active = 1)          as avg_days_past_due

    from loans l
    group by l.office_id, l.product_id

)

select
    {{ surrogate_key(['snapshot_date', 'office_id', 'product_id']) }} as snapshot_key,
    snapshot_date,
    toYYYYMMDD(snapshot_date)                                       as snapshot_date_key,
    office_id,
    product_id,

    loans_total,
    loans_active,
    loans_closed,
    loans_written_off,
    loans_delinquent,
    loans_par30,
    loans_par90,
    distinct_borrowers,

    principal_outstanding,
    interest_outstanding,
    total_outstanding,
    total_overdue,
    par30_amount,
    par90_amount,
    cumulative_disbursed,
    cumulative_written_off,

    avg_active_principal,
    max_days_past_due,
    avg_days_past_due,

    -- The three ratios every microfinance board pack opens with.
    {{ safe_divide('par30_amount', 'nullIf(total_outstanding, 0)') }} as par30_ratio,
    {{ safe_divide('par90_amount', 'nullIf(total_outstanding, 0)') }} as par90_ratio,
    {{ safe_divide('cumulative_written_off', 'nullIf(cumulative_disbursed, 0)') }}
                                                                     as write_off_ratio,

    now64(3)                                                        as dbt_updated_at

from snapshot
