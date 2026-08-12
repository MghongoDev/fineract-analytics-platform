{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        engine='MergeTree()',
        partition_by='toYYYYMM(transaction_date)',
        order_by='(transaction_date, office_id, loan_id, transaction_id)',
        settings={'index_granularity': 8192}
    )
}}

/*
    Transaction fact - the ledger, enriched with the dimensional keys the
    warehouse cares about.

    Incremental on the same window as its staging model. The unique key
    makes reprocessing idempotent, so the lookback can be generous.

    Dimension attributes are joined in rather than looked up at query
    time: this table is the one that gets scanned by every finance
    dashboard, and a 4-way join over millions of rows on each page load
    is exactly the cost ClickHouse denormalisation exists to avoid.
*/

with transactions as (

    select * from {{ ref('stg_fineract__loan_transactions') }}
    {% if is_incremental() %}
    where cdc_commit_at >= (
        select ifNull(max(source_updated_at), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'))
             - INTERVAL {{ var('cdc_lookback_hours', 48) }} HOUR
        from {{ this }}
    )
    {% endif %}

),

loans as (

    select loan_id, client_id, product_id, office_id, loan_officer_id,
           status as loan_status, principal as loan_principal,
           disbursed_on_date, par_bucket, days_past_due
    from {{ ref('stg_fineract__loans') }}

)

select
    -- keys
    t.transaction_id                                            as transaction_id,
    t.loan_id                                                   as loan_id,
    l.client_id                                                 as client_id,
    l.product_id                                                as product_id,
    coalesce(t.office_id, l.office_id)                  as office_id,
    l.loan_officer_id                                           as loan_officer_id,
    toYYYYMMDD(t.transaction_date)                      as transaction_date_key,

    -- descriptive
    t.transaction_date                                          as transaction_date,
    t.submitted_on_date                                         as submitted_on_date,
    t.transaction_month_start                                   as transaction_month_start,
    t.transaction_year                                          as transaction_year,
    t.transaction_month                                         as transaction_month,
    t.transaction_day_of_week                                   as transaction_day_of_week,
    t.type_id                                                   as type_id,
    t.type_value                                                as type_value,
    t.transaction_category                                      as transaction_category,
    t.is_reversed                                               as is_reversed,
    t.is_countable                                              as is_countable,
    t.currency_code                                             as currency_code,
    l.loan_status                                               as loan_status,
    l.par_bucket                                        as loan_par_bucket_now,

    -- measures. Every one is zeroed for a reversed transaction so a
    -- naive SUM() over this table is still correct - the most common
    -- reporting mistake in a ledger is forgetting the reversal filter.
    if(t.is_countable = 1, t.amount, toDecimal64(0, 6))                     as amount,
    if(t.is_countable = 1, t.principal_portion, toDecimal64(0, 6))          as principal_portion,
    if(t.is_countable = 1, t.interest_portion, toDecimal64(0, 6))           as interest_portion,
    if(t.is_countable = 1, t.fee_charges_portion, toDecimal64(0, 6))        as fee_charges_portion,
    if(t.is_countable = 1, t.penalty_charges_portion, toDecimal64(0, 6))    as penalty_charges_portion,
    if(t.is_countable = 1, t.overpayment_portion, toDecimal64(0, 6))        as overpayment_portion,
    t.outstanding_loan_balance                                  as outstanding_loan_balance,

    -- the unfiltered value, kept for audit and reconciliation
    t.amount                                            as amount_including_reversals,

    -- split measures by category, so a single scan produces the whole
    -- cash-flow picture without repeated CASE expressions downstream
    if(t.transaction_category = 'repayment' and t.is_countable = 1,
       t.amount, toDecimal64(0, 6))                     as repayment_amount,
    if(t.transaction_category = 'disbursement' and t.is_countable = 1,
       t.amount, toDecimal64(0, 6))                     as disbursement_amount,
    if(t.transaction_category = 'write_off' and t.is_countable = 1,
       t.amount, toDecimal64(0, 6))                     as write_off_amount,

    -- timing relative to the loan's own life
    if(l.disbursed_on_date is null, null,
       dateDiff('day', l.disbursed_on_date, t.transaction_date)) as days_since_disbursement,

    -- lineage
    t.cdc_commit_at                                     as source_updated_at,
    now64(3)                                            as dbt_updated_at

from transactions t
left join loans l on t.loan_id = l.loan_id
