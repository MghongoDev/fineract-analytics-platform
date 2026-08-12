{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(loan_id)'
    )
}}

/*
    Repayment behaviour per loan, derived from the transaction ledger.

    This is the only place the ledger is collapsed to loan grain, and it
    is reused by fct_loan, the ML features and the risk marts - which is
    exactly why it is a materialised intermediate model rather than three
    near-identical CTEs that drift apart over time.

    Reversed transactions are excluded from every measure via
    `is_countable`, but the reversal count is kept: a loan with many
    reversals is an operational signal (data entry problems, disputes)
    and turns out to be a useful risk feature.
*/

with transactions as (

    select *
    from {{ ref('stg_fineract__loan_transactions') }}

),

repayments as (

    select
        loan_id,
        count()                                                     as repayment_count,
        sum(amount)                                                 as total_repaid,
        sum(principal_portion)                                      as principal_repaid,
        sum(interest_portion)                                       as interest_repaid,
        sum(fee_charges_portion)                                    as fees_repaid,
        sum(penalty_charges_portion)                                as penalties_repaid,
        min(transaction_date)                                       as first_repayment_date,
        max(transaction_date)                                       as last_repayment_date,
        avg(amount)                                                 as avg_repayment_amount,
        -- Population stddev, not sample: this is the whole ledger for
        -- the loan, not a sample of it.
        stddevPop(toFloat64(amount))                                as stddev_repayment_amount,
        median(toFloat64(amount))                                   as median_repayment_amount,
        max(amount)                                                 as max_repayment_amount,
        countDistinct(transaction_month_start)                      as active_repayment_months
    from transactions
    where transaction_category = 'repayment' and is_countable = 1
    group by loan_id

),

disbursements as (

    select
        loan_id,
        count()                                                     as disbursement_count,
        sum(amount)                                                 as total_disbursed,
        min(transaction_date)                                       as first_disbursement_date,
        max(transaction_date)                                       as last_disbursement_date
    from transactions
    where transaction_category = 'disbursement' and is_countable = 1
    group by loan_id

),

anomalies as (

    select
        loan_id,
        countIf(is_reversed = 1)                                    as reversed_transaction_count,
        countIf(transaction_category = 'write_off')                 as write_off_transaction_count,
        countIf(transaction_category = 'waive_interest')            as interest_waiver_count,
        countIf(transaction_category = 'recovery_repayment')        as recovery_count,
        count()                                                     as total_transaction_count,
        max(transaction_date)                                       as last_activity_date
    from transactions
    group by loan_id

),

/*
    Repayment regularity.

    The gap between consecutive repayments is the single most predictive
    behavioural feature in microfinance default models - a borrower whose
    intervals are lengthening is in trouble well before the delinquency
    counter moves. Computed with a window function over the ledger.
*/
intervals as (

    select
        loan_id,
        avg(gap_days)                                               as avg_days_between_repayments,
        stddevPop(gap_days)                                         as stddev_days_between_repayments,
        max(gap_days)                                               as max_days_between_repayments
    from (
        select
            loan_id,
            dateDiff('day',
                     any(transaction_date) over (
                         partition by loan_id order by transaction_date
                         rows between 1 preceding and 1 preceding),
                     transaction_date)                              as gap_days
        from transactions
        where transaction_category = 'repayment' and is_countable = 1
    )
    where gap_days is not null and gap_days >= 0
    group by loan_id

)

select
    coalesce(r.loan_id, d.loan_id, a.loan_id, i.loan_id)            as loan_id,

    -- repayments
    coalesce(r.repayment_count, 0)                                  as repayment_count,
    coalesce(r.total_repaid, 0)                                     as total_repaid,
    coalesce(r.principal_repaid, 0)                                 as principal_repaid,
    coalesce(r.interest_repaid, 0)                                  as interest_repaid,
    coalesce(r.fees_repaid, 0)                                      as fees_repaid,
    coalesce(r.penalties_repaid, 0)                                 as penalties_repaid,
    r.first_repayment_date                                      as first_repayment_date,
    r.last_repayment_date                                       as last_repayment_date,
    r.avg_repayment_amount                                      as avg_repayment_amount,
    r.stddev_repayment_amount                                   as stddev_repayment_amount,
    r.median_repayment_amount                                   as median_repayment_amount,
    r.max_repayment_amount                                      as max_repayment_amount,
    coalesce(r.active_repayment_months, 0)                          as active_repayment_months,

    -- disbursements
    coalesce(d.disbursement_count, 0)                               as disbursement_count,
    coalesce(d.total_disbursed, 0)                                  as total_disbursed,
    d.first_disbursement_date                                   as first_disbursement_date,
    d.last_disbursement_date                                    as last_disbursement_date,

    -- anomalies
    coalesce(a.reversed_transaction_count, 0)                       as reversed_transaction_count,
    coalesce(a.write_off_transaction_count, 0)                      as write_off_transaction_count,
    coalesce(a.interest_waiver_count, 0)                            as interest_waiver_count,
    coalesce(a.recovery_count, 0)                                   as recovery_count,
    coalesce(a.total_transaction_count, 0)                          as total_transaction_count,
    a.last_activity_date                                        as last_activity_date,
    if(a.last_activity_date is null, null,
       dateDiff('day', a.last_activity_date, today()))              as days_since_last_activity,

    -- regularity
    i.avg_days_between_repayments                               as avg_days_between_repayments,
    i.stddev_days_between_repayments                            as stddev_days_between_repayments,
    i.max_days_between_repayments                               as max_days_between_repayments,
    -- Coefficient of variation: scale-free irregularity. High = erratic.
    {{ safe_divide('i.stddev_days_between_repayments', 'nullIf(i.avg_days_between_repayments, 0)') }}
                                                                    as repayment_regularity_cv

from repayments r
left join disbursements d on r.loan_id = d.loan_id
left join anomalies a on r.loan_id = a.loan_id
left join intervals i on r.loan_id = i.loan_id

union all

select
    coalesce(d.loan_id, a.loan_id, i.loan_id)                       as loan_id,

    -- repayments
    0                                                               as repayment_count,
    0                                                               as total_repaid,
    0                                                               as principal_repaid,
    0                                                               as interest_repaid,
    0                                                               as fees_repaid,
    0                                                               as penalties_repaid,
    null                                                            as first_repayment_date,
    null                                                            as last_repayment_date,
    null                                                            as avg_repayment_amount,
    null                                                            as stddev_repayment_amount,
    null                                                            as median_repayment_amount,
    null                                                            as max_repayment_amount,
    0                                                               as active_repayment_months,

    -- disbursements
    coalesce(d.disbursement_count, 0)                               as disbursement_count,
    coalesce(d.total_disbursed, 0)                                  as total_disbursed,
    d.first_disbursement_date                                   as first_disbursement_date,
    d.last_disbursement_date                                    as last_disbursement_date,

    -- anomalies
    coalesce(a.reversed_transaction_count, 0)                       as reversed_transaction_count,
    coalesce(a.write_off_transaction_count, 0)                      as write_off_transaction_count,
    coalesce(a.interest_waiver_count, 0)                            as interest_waiver_count,
    coalesce(a.recovery_count, 0)                                   as recovery_count,
    coalesce(a.total_transaction_count, 0)                          as total_transaction_count,
    a.last_activity_date                                        as last_activity_date,
    if(a.last_activity_date is null, null,
       dateDiff('day', a.last_activity_date, today()))              as days_since_last_activity,

    -- regularity
    i.avg_days_between_repayments                               as avg_days_between_repayments,
    i.stddev_days_between_repayments                            as stddev_days_between_repayments,
    i.max_days_between_repayments                               as max_days_between_repayments,
    {{ safe_divide('i.stddev_days_between_repayments', 'nullIf(i.avg_days_between_repayments, 0)') }}
                                                                    as repayment_regularity_cv

from disbursements d
left join anomalies a on d.loan_id = a.loan_id
left join intervals i on d.loan_id = i.loan_id
where d.loan_id not in (select loan_id from repayments)
