{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(client_id)'
    )
}}

/*
    One row per client: their whole relationship with the institution,
    across loans and savings.

    Grain is the client, so every loan-level measure is aggregated here
    exactly once. dim_client and the ML feature tables both read this
    rather than re-aggregating - which is what stops "number of active
    loans" from meaning two different things on two dashboards.
*/

with loans as (

    select *
    from {{ ref('stg_fineract__loans') }}

),

savings as (

    select *
    from {{ ref('stg_fineract__savings_accounts') }}

),

loan_rollup as (

    select
        client_id,
        count()                                                     as total_loans,
        countIf(is_active = 1)                                      as active_loans,
        countIf(is_closed = 1)                                      as closed_loans,
        countIf(is_written_off = 1)                                 as written_off_loans,
        countIf(days_past_due > 0)                                  as delinquent_loans,
        countIf(is_in_default = 1)                                  as defaulted_loans,

        sum(principal_disbursed)                                    as lifetime_disbursed,
        sum(total_repayment)                                        as lifetime_repaid,
        sumIf(principal_outstanding, is_active = 1)                 as current_principal_outstanding,
        sumIf(total_outstanding, is_active = 1)                     as current_total_outstanding,
        sumIf(total_overdue, is_active = 1)                         as current_total_overdue,
        sum(principal_written_off)                                  as lifetime_written_off,

        max(days_past_due)                                          as worst_days_past_due,
        maxIf(days_past_due, is_active = 1)                         as current_worst_days_past_due,

        min(disbursed_on_date)                                      as first_disbursement_date,
        max(disbursed_on_date)                                      as latest_disbursement_date,
        max(closed_on_date)                                         as latest_closure_date,
        avg(toFloat64(principal))                                   as avg_loan_principal,
        max(principal)                                              as max_loan_principal,
        countDistinct(product_id)                                   as distinct_products_used
    from loans
    group by client_id

),

savings_rollup as (

    select
        client_id,
        count()                                                     as total_savings_accounts,
        countIf(is_active = 1)                                      as active_savings_accounts,
        sum(account_balance)                                        as total_savings_balance,
        sum(total_deposits)                                         as lifetime_deposits,
        sum(total_withdrawals)                                      as lifetime_withdrawals,
        min(activated_on_date)                                      as first_savings_date
    from savings
    group by client_id

)

select
    coalesce(l.client_id, s.client_id)                              as client_id,

    -- loans
    coalesce(l.total_loans, 0)                                      as total_loans,
    coalesce(l.active_loans, 0)                                     as active_loans,
    coalesce(l.closed_loans, 0)                                     as closed_loans,
    coalesce(l.written_off_loans, 0)                                as written_off_loans,
    coalesce(l.delinquent_loans, 0)                                 as delinquent_loans,
    coalesce(l.defaulted_loans, 0)                                  as defaulted_loans,
    coalesce(l.lifetime_disbursed, 0)                               as lifetime_disbursed,
    coalesce(l.lifetime_repaid, 0)                                  as lifetime_repaid,
    coalesce(l.current_principal_outstanding, 0)                    as current_principal_outstanding,
    coalesce(l.current_total_outstanding, 0)                        as current_total_outstanding,
    coalesce(l.current_total_overdue, 0)                            as current_total_overdue,
    coalesce(l.lifetime_written_off, 0)                             as lifetime_written_off,
    coalesce(l.worst_days_past_due, 0)                              as worst_days_past_due,
    coalesce(l.current_worst_days_past_due, 0)                      as current_worst_days_past_due,
    l.first_disbursement_date                                   as first_disbursement_date,
    l.latest_disbursement_date                                  as latest_disbursement_date,
    l.latest_closure_date                                       as latest_closure_date,
    l.avg_loan_principal                                        as avg_loan_principal,
    l.max_loan_principal                                        as max_loan_principal,
    coalesce(l.distinct_products_used, 0)                           as distinct_products_used,

    -- savings
    coalesce(s.total_savings_accounts, 0)                           as total_savings_accounts,
    coalesce(s.active_savings_accounts, 0)                          as active_savings_accounts,
    coalesce(s.total_savings_balance, 0)                            as total_savings_balance,
    coalesce(s.lifetime_deposits, 0)                                as lifetime_deposits,
    coalesce(s.lifetime_withdrawals, 0)                             as lifetime_withdrawals,
    s.first_savings_date                                        as first_savings_date,

    -- relationship shape
    if(coalesce(l.total_loans, 0) > 0 and coalesce(s.total_savings_accounts, 0) > 0,
       1, 0)                                                        as is_multi_product_client,
    if(coalesce(l.total_loans, 0) > 1, 1, 0)                        as is_repeat_borrower,
    greatest(coalesce(l.total_loans, 0) - 1, 0)                     as prior_loan_count,

    -- Historical write-off rate. The strongest single predictor of the
    -- next default, and cheap to compute here once.
    {{ safe_divide('l.written_off_loans', 'nullIf(l.total_loans, 0)') }}
                                                                    as historical_write_off_rate,
    {{ safe_divide('l.lifetime_repaid', 'nullIf(l.lifetime_disbursed, 0)') }}
                                                                    as lifetime_repayment_ratio,
    {{ safe_divide('s.total_savings_balance', 'nullIf(l.current_total_outstanding, 0)') }}
                                                                    as savings_to_debt_ratio,

    if(l.first_disbursement_date is null, null,
       dateDiff('day', l.first_disbursement_date, today()))         as days_since_first_loan

from loan_rollup l
full outer join savings_rollup s on l.client_id = s.client_id
