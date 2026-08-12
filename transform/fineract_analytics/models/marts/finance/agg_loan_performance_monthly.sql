{{
    config(
        materialized='table',
        engine='MergeTree()',
        partition_by='toYYYYMM(month_start)',
        order_by='(month_start, office_id, product_id)',
        settings={'allow_nullable_key': 1}
    )
}}

/*
    Monthly cash-flow and origination summary by office x product.

    Derived entirely from the transaction ledger and the loan fact, so
    unlike the daily snapshot it is fully reproducible from history - a
    rebuild produces identical numbers, which is what makes it safe to
    use for reporting that has to tie out month after month.
*/

with transactions as (

    select * from {{ ref('fct_loan_transaction') }}

),

cash_flows as (

    select
        transaction_month_start                                     as month_start,
        office_id,
        product_id,
        countIf(transaction_category = 'repayment')                 as repayment_count,
        sum(repayment_amount)                                       as repayment_amount,
        sum(principal_portion)                                      as principal_collected,
        sum(interest_portion)                                       as interest_collected,
        sum(fee_charges_portion)                                    as fees_collected,
        sum(penalty_charges_portion)                                as penalties_collected,
        countIf(transaction_category = 'disbursement')              as disbursement_count,
        sum(disbursement_amount)                                    as disbursement_amount,
        sum(write_off_amount)                                       as write_off_amount,
        countIf(is_reversed = 1)                                    as reversal_count,
        countDistinct(loan_id)                                      as active_loan_count
    from transactions
    group by transaction_month_start, office_id, product_id

),

originations as (

    select
        toStartOfMonth(disbursed_on_date)                           as month_start,
        office_id,
        product_id,
        count()                                                     as loans_originated,
        sum(principal_disbursed)                                    as principal_originated,
        countDistinct(client_id)                                    as borrowers_served,
        avg(toFloat64(principal))                                   as avg_origination_principal,
        avg(toFloat64(days_to_approval))                            as avg_days_to_approval,
        avg(toFloat64(days_to_disbursement))                        as avg_days_to_disbursement,
        -- Vintage quality: of the loans written in this month, how many
        -- have since gone bad? The single most useful number for
        -- credit-policy review.
        countIf(is_in_default = 1)                                  as loans_now_in_default,
        countIf(is_written_off = 1)                                 as loans_now_written_off
    from {{ ref('fct_loan') }}
    where disbursed_on_date is not null
    group by toStartOfMonth(disbursed_on_date), office_id, product_id

)

select
    coalesce(c.month_start, o.month_start)                          as month_start,
    toYYYYMM(coalesce(c.month_start, o.month_start))                as month_key,
    coalesce(c.office_id, o.office_id)                              as office_id,
    coalesce(c.product_id, o.product_id)                            as product_id,

    -- collections
    coalesce(c.repayment_count, 0)                                  as repayment_count,
    coalesce(c.repayment_amount, 0)                                 as repayment_amount,
    coalesce(c.principal_collected, 0)                              as principal_collected,
    coalesce(c.interest_collected, 0)                               as interest_collected,
    coalesce(c.fees_collected, 0)                                   as fees_collected,
    coalesce(c.penalties_collected, 0)                              as penalties_collected,
    coalesce(c.reversal_count, 0)                                   as reversal_count,
    coalesce(c.active_loan_count, 0)                                as loans_with_activity,

    -- disbursements
    coalesce(c.disbursement_count, 0)                               as disbursement_count,
    coalesce(c.disbursement_amount, 0)                              as disbursement_amount,
    coalesce(c.write_off_amount, 0)                                 as write_off_amount,

    -- originations / vintage
    coalesce(o.loans_originated, 0)                                 as loans_originated,
    coalesce(o.principal_originated, 0)                             as principal_originated,
    coalesce(o.borrowers_served, 0)                                 as borrowers_served,
    o.avg_origination_principal                                 as avg_origination_principal,
    o.avg_days_to_approval                                      as avg_days_to_approval,
    o.avg_days_to_disbursement                                  as avg_days_to_disbursement,
    coalesce(o.loans_now_in_default, 0)                             as vintage_loans_in_default,
    coalesce(o.loans_now_written_off, 0)                            as vintage_loans_written_off,
    {{ safe_divide('o.loans_now_in_default', 'nullIf(o.loans_originated, 0)') }}
                                                                    as vintage_default_rate,

    -- net cash movement for the month
    coalesce(c.repayment_amount, 0) - coalesce(c.disbursement_amount, 0) as net_cash_flow,

    now64(3)                                                        as dbt_updated_at

from cash_flows c
full outer join originations o
  on c.month_start = o.month_start
 and c.office_id = o.office_id
 and c.product_id = o.product_id
