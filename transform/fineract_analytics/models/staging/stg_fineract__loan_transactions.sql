{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        engine='MergeTree()',
        partition_by='toYYYYMM(transaction_date)',
        order_by='(transaction_date, loan_id, transaction_id)',
        settings={'index_granularity': 8192}
    )
}}

/*
    Staging: loan transaction ledger. The largest table in the warehouse
    and the only staging model that is incremental.

    WHY argMax INSTEAD OF FINAL HERE
    --------------------------------
    Everywhere else in staging, FINAL is the readable choice. Not here:

      1. FINAL applied *after* an incremental WHERE clause collapses only
         the rows inside the window. If two versions of a transaction
         straddle the window boundary, the result is a partially
         collapsed row - silently wrong, and very hard to spot.
      2. FINAL is a merge-on-read across every column of every touched
         part. argMax only pays for the columns projected.

    So the collapse is explicit, and it happens *inside* the incremental
    window with a lookback wide enough to swallow late CDC arrivals.

    WHY delete+insert
    -----------------
    ClickHouse has no UPDATE in the OLTP sense. `delete+insert` on
    `transaction_id` is dbt-clickhouse's atomic-ish replace: it removes
    the affected keys and re-inserts them in one operation, which is what
    makes a re-processed lookback window idempotent rather than
    duplicating.
*/

with source as (

    select *
    from {{ source('fineract_raw', 'loan_transactions') }}
    where {{ incremental_cdc_filter('_source_commit_at') }}

),

deduplicated as (

    select
        transaction_id,
        argMax(loan_id, _version)                   as loan_id,
        argMax(office_id, _version)                 as office_id,
        argMax(office_name, _version)               as office_name,
        argMax(type_id, _version)                   as type_id,
        argMax(type_code, _version)                 as type_code,
        argMax(type_value, _version)                as type_value,
        argMax(is_reversed, _version)               as is_reversed,
        argMax(transaction_date, _version)          as transaction_date,
        argMax(submitted_on_date, _version)         as submitted_on_date,
        argMax(currency_code, _version)             as currency_code,
        argMax(amount, _version)                    as amount,
        argMax(net_disbursal_amount, _version)      as net_disbursal_amount,
        argMax(principal_portion, _version)         as principal_portion,
        argMax(interest_portion, _version)          as interest_portion,
        argMax(fee_charges_portion, _version)       as fee_charges_portion,
        argMax(penalty_charges_portion, _version)   as penalty_charges_portion,
        argMax(overpayment_portion, _version)       as overpayment_portion,
        argMax(outstanding_loan_balance, _version)  as outstanding_loan_balance,
        argMax(source_ingested_at, _version)        as source_ingested_at,
        argMax(_source_commit_at, _version)         as cdc_commit_at,
        argMax(_is_deleted, _version)               as is_deleted,
        max(_version)                               as cdc_version
    from source
    group by transaction_id

),

final as (

    select
        transaction_id,
        loan_id,
        office_id,
        office_name,
        type_id,
        type_code,
        type_value,
        coalesce(is_reversed, 0)                    as is_reversed,
        transaction_date,
        submitted_on_date,
        currency_code,

        {{ money('amount') }}                       as amount,
        {{ money('net_disbursal_amount') }}         as net_disbursal_amount,
        {{ money('principal_portion') }}            as principal_portion,
        {{ money('interest_portion') }}             as interest_portion,
        {{ money('fee_charges_portion') }}          as fee_charges_portion,
        {{ money('penalty_charges_portion') }}      as penalty_charges_portion,
        {{ money('overpayment_portion') }}          as overpayment_portion,
        {{ money('outstanding_loan_balance') }}     as outstanding_loan_balance,

        -- Transaction classification. Fineract type ids are stable
        -- across versions; the names are not, so classify on the id.
        multiIf(
            type_id = 1,  'disbursement',
            type_id = 2,  'repayment',
            type_id = 4,  'accrual',
            type_id = 5,  'waive_interest',
            type_id = 6,  'write_off',
            type_id = 7,  'recovery_repayment',
            type_id = 8,  'waive_charges',
            type_id = 9,  'charge_payment',
            type_id = 10, 'refund',
            'other'
        )                                           as transaction_category,

        -- A reversed transaction must never contribute to a total. This
        -- flag is what every downstream sum() filters on - reversals are
        -- kept (audit) but excluded from measures.
        if(coalesce(is_reversed, 0) = 1, 0, 1)      as is_countable,

        toYear(transaction_date)                    as transaction_year,
        toMonth(transaction_date)                   as transaction_month,
        toStartOfMonth(transaction_date)            as transaction_month_start,
        toDayOfWeek(transaction_date)               as transaction_day_of_week,

        source_ingested_at,
        cdc_commit_at,
        cdc_version

    from deduplicated
    where is_deleted = 0

)

select * from final
